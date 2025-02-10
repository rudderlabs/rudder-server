package transformer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/response"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	violationErrors     = "violationErrors"
	redshiftStringLimit = 512
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func New(conf *config.Config, logger logger.Logger, statsFactory stats.Stats) *Transformer {
	t := &Transformer{
		logger:         logger.Child("warehouse-transformer"),
		statsFactory:   statsFactory,
		now:            timeutil.Now,
		loggedFileName: generateLogFileName(),
	}

	t.stats.mismatchedEvents = t.statsFactory.NewStat("warehouse_dest_transform_mismatched_events", stats.HistogramType)
	t.stats.comparisionTime = t.statsFactory.NewStat("warehouse_dest_transform_comparison_time", stats.TimerType)

	t.config.enableIDResolution = conf.GetReloadableBoolVar(false, "Warehouse.enableIDResolution")
	t.config.populateSrcDestInfoInContext = conf.GetReloadableBoolVar(true, "WH_POPULATE_SRC_DEST_INFO_IN_CONTEXT")
	t.config.maxColumnsInEvent = conf.GetReloadableIntVar(200, 1, "WH_MAX_COLUMNS_IN_EVENT")
	t.config.maxLoggedEvents = conf.GetReloadableIntVar(10000, 1, "Warehouse.maxLoggedEvents")
	return t
}

func (t *Transformer) Transform(_ context.Context, clientEvents []ptrans.TransformerEvent, _ int) (res ptrans.Response) {
	if len(clientEvents) == 0 {
		return
	}

	startTime := t.now()
	metadata := clientEvents[0].Metadata
	c := &cache{}

	defer func() {
		tags := stats.Tags{
			"workspaceId":     metadata.WorkspaceID,
			"sourceId":        metadata.SourceID,
			"sourceType":      metadata.SourceType,
			"destinationId":   metadata.DestinationID,
			"destinationType": metadata.DestinationType,
		}

		t.statsFactory.NewTaggedStat("warehouse_dest_transform_request_latency", stats.TimerType, tags).Since(startTime)
		t.statsFactory.NewTaggedStat("warehouse_dest_transform_requests", stats.CountType, tags).Increment()
		t.statsFactory.NewTaggedStat("warehouse_dest_transform_input_events", stats.HistogramType, tags).Observe(float64(len(clientEvents)))
		t.statsFactory.NewTaggedStat("warehouse_dest_transform_output_events", stats.HistogramType, tags).Observe(float64(len(res.Events)))
		t.statsFactory.NewTaggedStat("warehouse_dest_transform_output_failed_events", stats.HistogramType, tags).Observe(float64(len(res.FailedEvents)))
	}()

	for _, clientEvent := range clientEvents {
		r, err := t.processWarehouseMessage(c, &clientEvent)
		if err != nil {
			res.FailedEvents = append(res.FailedEvents, transformerResponseFromErr(&clientEvent, err))
			continue
		}

		res.Events = append(res.Events, lo.Map(r, func(item map[string]any, index int) ptrans.TransformerResponse {
			return ptrans.TransformerResponse{
				Output:     item,
				Metadata:   clientEvent.Metadata,
				StatusCode: http.StatusOK,
			}
		})...)
	}
	return
}

func (t *Transformer) processWarehouseMessage(cache *cache, event *ptrans.TransformerEvent) ([]map[string]any, error) {
	if err := t.enhanceContextWithSourceDestInfo(event); err != nil {
		return nil, fmt.Errorf("enhancing context with source and destination info: %w", err)
	}
	return t.handleEvent(event, cache)
}

func (t *Transformer) enhanceContextWithSourceDestInfo(event *ptrans.TransformerEvent) error {
	if !t.config.populateSrcDestInfoInContext.Load() {
		return nil
	}

	messageContext, ok := event.Message["context"]
	if !ok || messageContext == nil {
		messageContext = map[string]any{}
	}
	messageContextMap, ok := messageContext.(map[string]any)
	if !ok {
		return response.ErrContextNotMap
	}
	messageContextMap["sourceId"] = event.Metadata.SourceID
	messageContextMap["sourceType"] = event.Metadata.SourceType
	messageContextMap["destinationId"] = event.Metadata.DestinationID
	messageContextMap["destinationType"] = event.Metadata.DestinationType

	event.Message["context"] = messageContextMap
	return nil
}

func (t *Transformer) handleEvent(event *ptrans.TransformerEvent, cache *cache) ([]map[string]any, error) {
	intrOpts := extractIntrOpts(event.Metadata.DestinationType, event.Message)
	destOpts := extractDestOpts(event.Metadata.DestinationType, event.Destination.Config)
	jsonPathsInfo := extractJSONPathInfo(append(intrOpts.jsonPaths, destOpts.jsonPaths...))

	eventType := strings.ToLower(event.Metadata.EventType)

	tec := &transformEventContext{
		event:         event,
		intrOpts:      &intrOpts,
		destOpts:      &destOpts,
		jsonPathsInfo: &jsonPathsInfo,
		cache:         cache,
	}

	switch eventType {
	case "track":
		return t.trackEvents(tec)
	case "identify":
		return t.identifyEvents(tec)
	case "page":
		return t.pageEvents(tec)
	case "screen":
		return t.screenEvents(tec)
	case "alias":
		return t.aliasEvents(tec)
	case "group":
		return t.groupEvents(tec)
	case "extract":
		return t.extractEvents(tec)
	case "merge":
		return t.mergeEvents(tec)
	default:
		return nil, response.NewTransformerError(fmt.Sprintf("Unknown event type: %q", eventType), http.StatusBadRequest)
	}
}

func transformerResponseFromErr(event *ptrans.TransformerEvent, err error) ptrans.TransformerResponse {
	var te *response.TransformerError
	if ok := errors.As(err, &te); ok {
		return ptrans.TransformerResponse{
			Output:     nil,
			Metadata:   event.Metadata,
			Error:      te.Error(),
			StatusCode: te.StatusCode(),
		}
	}
	return ptrans.TransformerResponse{
		Output:     nil,
		Metadata:   event.Metadata,
		Error:      response.ErrInternalServer.Error(),
		StatusCode: response.ErrInternalServer.StatusCode(),
	}
}

func (t *Transformer) getColumns(
	destType string,
	data map[string]any, metadata map[string]string,
) (map[string]any, error) {
	columns := make(map[string]any, len(data))

	// uuid_ts and loaded_at datatypes are passed from here to create appropriate columns.
	// Corresponding values are inserted when loading into the warehouse
	uuidTS := whutils.ToProviderCase(destType, "uuid_ts")
	columns[uuidTS] = model.DateTimeDataType

	if destType == whutils.BQ {
		columns["loaded_at"] = model.DateTimeDataType
	}

	for key, value := range data {
		if dataType, ok := metadata[key]; ok {
			columns[key] = dataType
		} else {
			columns[key] = dataTypeFor(destType, key, value, false)
		}
	}
	if len(columns) > t.config.maxColumnsInEvent.Load() && !utils.IsRudderSources(data) && !utils.IsDataLake(destType) {
		return nil, response.NewTransformerError(fmt.Sprintf("%s transformer: Too many columns outputted from the event", strings.ToLower(destType)), http.StatusBadRequest)
	}
	return columns, nil
}
