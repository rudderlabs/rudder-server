package transformer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/response"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
)

type (
	transformer struct {
		now func() time.Time

		conf         *config.Config
		logger       logger.Logger
		statsFactory stats.Stats

		config struct {
			enableIDResolution           config.ValueLoader[bool]
			enableArraySupport           config.ValueLoader[bool]
			populateSrcDestInfoInContext config.ValueLoader[bool]
			maxColumnsInEvent            config.ValueLoader[int]
		}
	}

	processingInfo struct {
		event         ptrans.TransformerEvent
		itrOpts       integrationsOptions
		dstOpts       destConfigOptions
		jsonPathsInfo jsonPathInfo
	}
)

func New(conf *config.Config, logger logger.Logger, statsFactory stats.Stats) ptrans.DestinationTransformer {
	t := &transformer{
		conf:         conf,
		logger:       logger.Child("warehouse-transformer"),
		statsFactory: statsFactory,
		now:          time.Now,
	}

	t.config.enableIDResolution = conf.GetReloadableBoolVar(false, "Warehouse.enableIDResolution")
	t.config.enableArraySupport = conf.GetReloadableBoolVar(false, "Warehouse.clickhouse.enableArraySupport")
	t.config.populateSrcDestInfoInContext = conf.GetReloadableBoolVar(true, "Warehouse.populateSrcDestInfoInContext")
	t.config.maxColumnsInEvent = conf.GetReloadableIntVar(200, 1, "Warehouse.maxColumnsInEvent")
	return t
}

func (t *transformer) Transform(_ context.Context, clientEvents []ptrans.TransformerEvent, _ int) (res ptrans.Response) {
	if len(clientEvents) == 0 {
		return
	}

	startTime := t.now()
	metadata := clientEvents[0].Metadata

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

	for _, event := range clientEvents {
		r, err := t.processWarehouseMessage(event)
		if err != nil {
			res.FailedEvents = append(res.FailedEvents, t.transformerResponseFromErr(event, err))
			continue
		}

		res.Events = append(res.Events, lo.Map(r, func(item map[string]any, index int) ptrans.TransformerResponse {
			return ptrans.TransformerResponse{
				Output:     item,
				Metadata:   event.Metadata,
				StatusCode: http.StatusOK,
			}
		})...)
	}
	return
}

func (t *transformer) processWarehouseMessage(event ptrans.TransformerEvent) ([]map[string]any, error) {
	if err := t.enhanceContextWithSourceDestInfo(event); err != nil {
		return nil, fmt.Errorf("enhancing context with source and destination info: %w", err)
	}
	return t.handleEvent(event)
}

func (t *transformer) enhanceContextWithSourceDestInfo(event ptrans.TransformerEvent) error {
	if !t.config.populateSrcDestInfoInContext.Load() {
		return nil
	}

	messageContext, ok := event.Message["context"]
	if !ok || messageContext == nil {
		messageContext = map[string]any{}
		event.Message["context"] = messageContext
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

func (t *transformer) handleEvent(event ptrans.TransformerEvent) ([]map[string]any, error) {
	itrOpts := prepareIntegrationOptions(event)
	dstOpts := prepareDestinationOptions(event.Metadata.DestinationType, event.Destination.Config)
	jsonPathsInfo := extractJSONPathInfo(append(itrOpts.jsonPaths, dstOpts.jsonPaths...))
	eventType := strings.ToLower(event.Metadata.EventType)

	pi := &processingInfo{
		event:         event,
		itrOpts:       itrOpts,
		dstOpts:       dstOpts,
		jsonPathsInfo: jsonPathsInfo,
	}

	switch eventType {
	case "extract":
		return t.handleExtractEvent(pi)
	case "track":
		return t.handleTrackEvent(pi)
	case "identify":
		return t.handleIdentifyEvent(pi)
	case "page":
		return t.handlePageEvent(pi)
	case "screen":
		return t.handleScreenEvent(pi)
	case "group":
		return t.handleGroupEvent(pi)
	case "alias":
		return t.handleAliasEvent(pi)
	case "merge":
		return t.handleMergeEvent(pi)
	default:
		return nil, response.NewTransformerError(fmt.Sprintf("Unknown event type: %q", eventType), http.StatusBadRequest)
	}
}

func (t *transformer) transformerResponseFromErr(event ptrans.TransformerEvent, err error) ptrans.TransformerResponse {
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

func storeRudderEvent(pi *processingInfo, data map[string]any, columnType map[string]string) error {
	if !pi.dstOpts.storeFullEvent {
		return nil
	}

	eventName, err := SafeColumnName(pi.event.Metadata.DestinationType, pi.itrOpts, "rudder_event")
	if err != nil {
		return fmt.Errorf("safe column name: %w", err)
	}

	eventJSON, err := json.Marshal(pi.event.Message)
	if err != nil {
		return fmt.Errorf("marshalling event: %w", err)
	}

	data[eventName] = string(eventJSON)
	columnType[eventName] = utils.GetFullEventColumnTypeByDestType(pi.event.Metadata.DestinationType)
	return nil
}

func excludeRudderCreatedTableNames(name string, skipReservedKeywordsEscaping bool) string {
	if utils.IsRudderIsolatedTable(name) || (utils.IsRudderCreatedTable(name) && !skipReservedKeywordsEscaping) {
		return "_" + name
	}
	return name
}
