package warehouse

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/internal/enricher"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/warehouse/internal/response"
	wtypes "github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/warehouse/internal/types"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/warehouse/internal/utils"
	"github.com/rudderlabs/rudder-server/processor/types"

	"github.com/rudderlabs/rudder-server/utils/timeutil"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	violationErrors     = "violationErrors"
	redshiftStringLimit = 512
)

// Compile-time check to ensure ValidationError struct remains unchanged.
var _ = struct {
	Type     string
	Message  string
	Meta     map[string]string
	Property string
}(types.ValidationError{})

// Compile-time check to ensure Geolocation struct remains unchanged
var _ = struct {
	IP       string
	City     string
	Country  string
	Region   string
	Postal   string
	Location string
	Timezone string
}(enricher.Geolocation{})

var unicodePattern = regexp.MustCompile(`\\u[0-9a-fA-F]{4}`)

type Opts func(t *Transformer)

func WithNow(now func() time.Time) Opts {
	return func(t *Transformer) {
		t.now = now
	}
}

func WithUUIDGenerator(uuidGenerator func() string) Opts {
	return func(t *Transformer) {
		t.uuidGenerator = uuidGenerator
	}
}

func WithSorter(sorter func([]string) []string) Opts {
	return func(t *Transformer) {
		t.sorter = sorter
	}
}

func New(conf *config.Config, logger logger.Logger, statsFactory stats.Stats, opts ...Opts) *Transformer {
	t := &Transformer{
		conf:          conf,
		logger:        logger.Child("warehouse-transformer"),
		statsFactory:  statsFactory,
		now:           timeutil.Now,
		uuidGenerator: uuid.NewString,
		sorter: func(x []string) []string {
			sort.Strings(x)
			return x
		},
	}
	for _, opt := range opts {
		opt(t)
	}

	t.stats.matchedEvents = t.statsFactory.NewStat("warehouse_dest_transform_matched_events", stats.HistogramType)
	t.stats.mismatchedEvents = t.statsFactory.NewStat("warehouse_dest_transform_mismatched_events", stats.HistogramType)
	t.stats.comparisonTime = t.statsFactory.NewStat("warehouse_dest_transform_comparison_time", stats.TimerType)

	t.config.enableIDResolution = conf.GetReloadableBoolVar(false, "Warehouse.enableIDResolution")
	t.config.populateSrcDestInfoInContext = conf.GetReloadableBoolVar(true, "WH_POPULATE_SRC_DEST_INFO_IN_CONTEXT")
	t.config.maxColumnsInEvent = conf.GetReloadableIntVar(1600, 1, "WH_MAX_COLUMNS_IN_EVENT")
	t.config.maxLoggedEvents = conf.GetReloadableIntVar(100, 1, "Warehouse.Transformer.Sampling.maxLoggedEvents")
	t.config.concurrentTransformations = conf.GetReloadableIntVar(1, 1, "Warehouse.concurrentTransformations")
	t.config.instanceID = conf.GetString("INSTANCE_ID", "1")

	var err error
	t.loggedSamplesUploader, err = getSamplingUploader(t.conf, t.logger)
	if err != nil {
		t.logger.Errorn("Failed to create wt sampling file manager", obskit.Error(err))
	}
	return t
}

func (t *Transformer) Transform(_ context.Context, clientEvents []types.TransformerEvent) (res types.Response) {
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

	results := make(chan types.TransformerResponse, len(clientEvents))
	done := make(chan struct{})

	g := errgroup.Group{}
	g.SetLimit(t.config.concurrentTransformations.Load())

	go func() {
		defer close(done)
		for r := range results {
			if r.Error != "" {
				res.FailedEvents = append(res.FailedEvents, r)
			} else {
				res.Events = append(res.Events, r)
			}
		}
	}()

	for i := range clientEvents {
		event := clientEvents[i]
		eventMetadata := event.Metadata
		eventMetadata.SourceDefinitionType = "" // TODO: Currently, it's getting ignored during JSON marshalling Remove this once we start using it.

		g.Go(func() error {
			r, err := t.processWarehouseMessage(c, &event)
			if err != nil {
				results <- transformerResponseFromErr(&eventMetadata, err)
				return nil
			}
			for _, item := range r {
				results <- types.TransformerResponse{
					Output:     item,
					Metadata:   eventMetadata,
					StatusCode: http.StatusOK,
				}
			}
			return nil
		})
	}

	_ = g.Wait()
	close(results)
	<-done
	return
}

func (t *Transformer) processWarehouseMessage(cache *cache, event *types.TransformerEvent) ([]map[string]any, error) {
	wEvent := wtypes.New(event, t.uuidGenerator, t.now)
	if err := t.checkValidContext(wEvent); err != nil {
		return nil, fmt.Errorf("checking valid context: %w", err)
	}
	return t.handleEvent(wEvent, cache)
}

func (t *Transformer) checkValidContext(event *wtypes.TransformerEvent) error {
	if !t.config.populateSrcDestInfoInContext.Load() {
		return nil
	}
	contextVal, exists := event.Message["context"]
	if !exists || contextVal == nil {
		return nil
	}
	if !utils.IsObject(contextVal) {
		return response.ErrContextNotMap
	}
	return nil
}

func (t *Transformer) eventContext(tec *transformEventContext) any {
	contextVal, exists := tec.event.Message["context"]
	if !t.config.populateSrcDestInfoInContext.Load() {
		return contextVal
	}
	if !exists || contextVal == nil {
		contextVal = map[string]interface{}{}
	}
	clonedContext := maps.Clone(contextVal.(map[string]any))
	clonedContext["sourceId"] = tec.event.Metadata.SourceID
	clonedContext["sourceType"] = tec.event.Metadata.SourceType
	clonedContext["destinationId"] = tec.event.Metadata.DestinationID
	clonedContext["destinationType"] = tec.event.Metadata.DestinationType
	return clonedContext
}

func (t *Transformer) handleEvent(event *wtypes.TransformerEvent, cache *cache) ([]map[string]any, error) {
	intrOpts := extractIntrOpts(event.Metadata.DestinationType, event.Message)
	destOpts := extractDestOpts(event.Metadata.DestinationType, event.Metadata.DestinationConfig)
	jsonPathsInfo := extractJSONPathInfo(append(intrOpts.jsonPaths, destOpts.jsonPaths...))

	eventType := strings.ToLower(event.Metadata.EventType)

	tec := &transformEventContext{
		event:         event,
		intrOpts:      &intrOpts,
		destOpts:      &destOpts,
		jsonPathsInfo: &jsonPathsInfo,
		cache:         cache,
		sorter:        t.sorter,
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

func transformerResponseFromErr(metadata *types.Metadata, err error) types.TransformerResponse {
	var te *response.TransformerError
	if ok := errors.As(err, &te); ok {
		return types.TransformerResponse{
			Output:     nil,
			Metadata:   *metadata,
			Error:      te.Error(),
			StatusCode: te.StatusCode(),
		}
	}
	return types.TransformerResponse{
		Output:     nil,
		Metadata:   *metadata,
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
