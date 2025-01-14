package transformer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/response"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/rules"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	violationErrors     = "violationErrors"
	redshiftStringLimit = 512
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func New(conf *config.Config, logger logger.Logger, statsFactory stats.Stats) ptrans.DestinationTransformer {
	t := &transformer{
		conf:         conf,
		logger:       logger.Child("warehouse-transformer"),
		statsFactory: statsFactory,
		now:          time.Now,
	}

	t.config.enableIDResolution = conf.GetReloadableBoolVar(false, "Warehouse.enableIDResolution")
	t.config.populateSrcDestInfoInContext = conf.GetReloadableBoolVar(true, "WH_POPULATE_SRC_DEST_INFO_IN_CONTEXT")
	t.config.maxColumnsInEvent = conf.GetReloadableIntVar(200, 1, "WH_MAX_COLUMNS_IN_EVENT")
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
	case "track":
		return t.handleTrackEvent(pi)
	case "identify":
		return t.handleIdentifyEvent(pi)
	case "page":
		return t.handlePageEvent(pi)
	case "screen":
		return t.handleScreenEvent(pi)
	case "alias":
		return t.handleAliasEvent(pi)
	case "group":
		return t.handleGroupEvent(pi)
	case "extract":
		return t.handleExtractEvent(pi)
	case "merge":
		return t.handleMergeEvent(pi)
	default:
		return nil, response.NewTransformerError(fmt.Sprintf("Unknown event type: %q", eventType), http.StatusBadRequest)
	}
}

func prepareIntegrationOptions(event ptrans.TransformerEvent) (opts integrationsOptions) {
	src := misc.MapLookup(event.Message, "integrations", event.Metadata.DestinationType, "options")
	if src == nil || !utils.IsObject(src) {
		return
	}
	var jsonPaths []any

	srcMap := src.(map[string]any)

	setOption(srcMap, "skipReservedKeywordsEscaping", &opts.skipReservedKeywordsEscaping)
	setOption(srcMap, "useBlendoCasing", &opts.useBlendoCasing)
	setOption(srcMap, "skipTracksTable", &opts.skipTracksTable)
	setOption(srcMap, "skipUsersTable", &opts.skipUsersTable)
	setOption(srcMap, "jsonPaths", &jsonPaths)

	for _, jp := range jsonPaths {
		if jpStr, ok := jp.(string); ok {
			opts.jsonPaths = append(opts.jsonPaths, jpStr)
		}
	}
	return
}

func prepareDestinationOptions(destType string, destConfig map[string]any) (opts destConfigOptions) {
	var jsonPaths string

	setOption(destConfig, "skipTracksTable", &opts.skipTracksTable)
	setOption(destConfig, "skipUsersTable", &opts.skipUsersTable)
	setOption(destConfig, "underscoreDivideNumbers", &opts.underscoreDivideNumbers)
	setOption(destConfig, "allowUsersContextTraits", &opts.allowUsersContextTraits)
	setOption(destConfig, "storeFullEvent", &opts.storeFullEvent)
	setOption(destConfig, "jsonPaths", &jsonPaths)

	if len(jsonPaths) > 0 && utils.IsJSONPathSupportedAsPartOfConfig(destType) {
		opts.jsonPaths = strings.Split(jsonPaths, ",")
	}
	return
}

func setOption[T any](src map[string]any, key string, dest *T) {
	if val, ok := src[key].(T); ok {
		*dest = val
	}
}

func (t *transformer) handleTrackEvent(pi *processingInfo) ([]map[string]any, error) {
	commonProps, commonColumnTypes, transformerEventName, err := t.trackCommonProps(pi)
	if err != nil {
		return nil, fmt.Errorf("track common properties: %w", err)
	}

	tracksResponse, err := t.tracksResponse(pi, commonProps, commonColumnTypes)
	if err != nil {
		return nil, fmt.Errorf("track response: %w", err)
	}

	trackEventsResponse, err := t.trackEventsResponse(pi, transformerEventName, commonProps, commonColumnTypes)
	if err != nil {
		return nil, fmt.Errorf("track events response: %w", err)
	}
	return append(tracksResponse, trackEventsResponse...), nil
}

func (t *transformer) trackCommonProps(pi *processingInfo) (map[string]any, map[string]string, string, error) {
	commonProps := make(map[string]any)
	commonColumnTypes := make(map[string]string)

	if err := t.setDataAndColumnTypeFromInput(pi, pi.event.Message["context"], commonProps, commonColumnTypes, &prefixInfo{
		completePrefix: "track_context_",
		completeLevel:  2,
		prefix:         "context_",
	}); err != nil {
		return nil, nil, "", fmt.Errorf("track common props: setting data and column types from message: %w", err)
	}
	if err := t.setDataAndColumnTypeFromRules(pi, commonProps, commonColumnTypes, rules.TrackRules); err != nil {
		return nil, nil, "", fmt.Errorf("track common props: setting data and column types from rules: %w", err)
	}
	if err := t.setDataAndColumnTypeFromRules(pi, commonProps, commonColumnTypes, rules.DefaultRules); err != nil {
		return nil, nil, "", fmt.Errorf("track common props: setting data and column types from rules: %w", err)
	}

	eventColName, err := SafeColumnName(pi.event.Metadata.DestinationType, pi.itrOpts, "event")
	if err != nil {
		return nil, nil, "", fmt.Errorf("track common props: safe column name: %w", err)
	}
	eventTextColName, err := SafeColumnName(pi.event.Metadata.DestinationType, pi.itrOpts, "event_text")
	if err != nil {
		return nil, nil, "", fmt.Errorf("track common props: safe column name: %w", err)
	}

	var eventName, transformerEventName string
	if d, dok := commonProps[eventTextColName]; dok {
		eventName, _ = d.(string)
	}
	transformerEventName = TransformTableName(pi.itrOpts, pi.dstOpts, eventName)

	commonProps[eventColName] = transformerEventName
	commonColumnTypes[eventColName] = "string"
	return commonProps, commonColumnTypes, transformerEventName, nil
}

func (t *transformer) tracksResponse(pi *processingInfo, commonProps map[string]any, commonColumnTypes map[string]string) ([]map[string]any, error) {
	if pi.itrOpts.skipTracksTable || pi.dstOpts.skipTracksTable {
		return nil, nil
	}

	event := make(map[string]any)
	columnTypes := make(map[string]string)

	event = lo.Assign(event, commonProps)

	if err := t.setDataAndColumnTypeFromRules(pi, event, columnTypes, rules.TrackTableRules); err != nil {
		return nil, fmt.Errorf("tracks response: setting data and column types from rules: %w", err)
	}
	if err := storeRudderEvent(pi, event, columnTypes); err != nil {
		return nil, fmt.Errorf("tracks response: storing rudder event: %w", err)
	}

	table, err := SafeTableName(pi.event.Metadata.DestinationType, pi.itrOpts, "tracks")
	if err != nil {
		return nil, fmt.Errorf("tracks response: safe table name: %w", err)
	}
	columns, err := t.getColumns(pi.event.Metadata.DestinationType, event, lo.Assign(columnTypes, commonColumnTypes))
	if err != nil {
		return nil, fmt.Errorf("tracks response: getting columns: %w", err)
	}

	output := map[string]any{
		"data": event,
		"metadata": map[string]any{
			"table":      table,
			"columns":    columns,
			"receivedAt": pi.event.Metadata.ReceivedAt,
		},
		"userId": "",
	}
	return []map[string]any{output}, nil
}

func (t *transformer) trackEventsResponse(pi *processingInfo, transformerEventName string, commonProps map[string]any, commonColumnTypes map[string]string) ([]map[string]any, error) {
	if len(transformerEventName) == 0 || len(strings.TrimSpace(transformerEventName)) == 0 {
		return nil, nil
	}

	event := make(map[string]any)
	columnTypes := make(map[string]string)

	if err := t.setDataAndColumnTypeFromInput(pi, pi.event.Message["properties"], event, columnTypes, &prefixInfo{
		completePrefix: "track_properties_",
		completeLevel:  2,
	}); err != nil {
		return nil, fmt.Errorf("track events response: setting data and column types from message: %w", err)
	}
	if err := t.setDataAndColumnTypeFromInput(pi, pi.event.Message["userProperties"], event, columnTypes, &prefixInfo{
		completePrefix: "track_userProperties_",
		completeLevel:  2,
	}); err != nil {
		return nil, fmt.Errorf("track events response: setting data and column types from message: %w", err)
	}
	if err := t.setDataAndColumnTypeFromRules(pi, commonProps, commonColumnTypes, rules.TrackEventTableRules); err != nil {
		return nil, fmt.Errorf("track events response: setting data and column types from rules: %w", err)
	}

	eventTableEvent := lo.Assign(event, commonProps)

	columnName := TransformColumnName(pi.event.Metadata.DestinationType, pi.itrOpts, pi.dstOpts, transformerEventName)
	table, err := SafeTableName(pi.event.Metadata.DestinationType, pi.itrOpts, columnName)
	if err != nil {
		return nil, fmt.Errorf("track events response: safe table name: %w", err)
	}
	excludeTable := excludeRudderCreatedTableNames(table, pi.itrOpts.skipReservedKeywordsEscaping)

	columns, err := t.getColumns(pi.event.Metadata.DestinationType, eventTableEvent, lo.Assign(columnTypes, commonColumnTypes))
	if err != nil {
		return nil, fmt.Errorf("track events response: getting columns: %w", err)
	}

	mergeEvents, err := t.handleMergeEvent(pi)
	if err != nil {
		return nil, fmt.Errorf("track events response: merge event: %w", err)
	}

	trackOutput := map[string]any{
		"data": eventTableEvent,
		"metadata": map[string]any{
			"table":      excludeTable,
			"columns":    columns,
			"receivedAt": pi.event.Metadata.ReceivedAt,
		},
		"userId": "",
	}
	return append([]map[string]any{trackOutput}, mergeEvents...), nil
}

func (t *transformer) handleExtractEvent(pi *processingInfo) ([]map[string]any, error) {
	event := make(map[string]any)
	columnTypes := make(map[string]string)

	if err := t.setDataAndColumnTypeFromInput(pi, pi.event.Message["context"], event, columnTypes, &prefixInfo{
		completePrefix: "extract_context_",
		completeLevel:  2,
		prefix:         "context_",
	}); err != nil {
		return nil, fmt.Errorf("extract: setting data and column types from input: %w", err)
	}
	if err := t.setDataAndColumnTypeFromInput(pi, pi.event.Message["properties"], event, columnTypes, &prefixInfo{
		completePrefix: "extract_properties_",
		completeLevel:  2,
	}); err != nil {
		return nil, fmt.Errorf("extract: setting data and column types from input: %w", err)
	}

	eventColName, err := SafeColumnName(pi.event.Metadata.DestinationType, pi.itrOpts, "event")
	if err != nil {
		return nil, fmt.Errorf("extract: safe column name: %w", err)
	}

	eventName, _ := pi.event.Message[eventColName].(string)

	event[eventColName] = TransformTableName(pi.itrOpts, pi.dstOpts, eventName)
	columnTypes[eventColName] = model.StringDataType

	if err = t.setDataAndColumnTypeFromRules(pi, event, columnTypes, rules.ExtractRules); err != nil {
		return nil, fmt.Errorf("extract: setting data and column types from rules: %w", err)
	}

	if val := event[eventColName]; val == nil || utils.IsBlank(val) {
		return nil, response.ErrExtractEventNameEmpty
	}

	columnName := TransformColumnName(pi.event.Metadata.DestinationType, pi.itrOpts, pi.dstOpts, event[eventColName].(string))
	tableName, err := SafeTableName(pi.event.Metadata.DestinationType, pi.itrOpts, columnName)
	if err != nil {
		return nil, fmt.Errorf("extract: safe table name: %w", err)
	}
	excludeTableName := excludeRudderCreatedTableNames(tableName, pi.itrOpts.skipReservedKeywordsEscaping)

	columns, err := t.getColumns(pi.event.Metadata.DestinationType, event, columnTypes)
	if err != nil {
		return nil, fmt.Errorf("extract: getting columns: %w", err)
	}

	extractOutput := map[string]any{
		"data": event,
		"metadata": map[string]any{
			"table":      excludeTableName,
			"columns":    columns,
			"receivedAt": pi.event.Metadata.ReceivedAt,
		},
		"userId": "",
	}
	return []map[string]any{extractOutput}, nil
}

func (t *transformer) handleIdentifyEvent(pi *processingInfo) ([]map[string]any, error) {
	commonProps, commonColumnTypes, err := t.identifyCommonProps(pi)
	if err != nil {
		return nil, fmt.Errorf("identifies: common properties: %w", err)
	}

	identifiesResponse, err := t.identifiesResponse(pi, commonProps, commonColumnTypes)
	if err != nil {
		return nil, fmt.Errorf("identifies response: %w", err)
	}

	mergeEvents, err := t.handleMergeEvent(pi)
	if err != nil {
		return nil, fmt.Errorf("identifies: merge event: %w", err)
	}

	usersResponse, err := t.usersResponse(pi, commonProps, commonColumnTypes)
	if err != nil {
		return nil, fmt.Errorf("identifies: users response: %w", err)
	}
	return append(append(identifiesResponse, mergeEvents...), usersResponse...), nil
}

func (t *transformer) identifyCommonProps(pi *processingInfo) (map[string]any, map[string]string, error) {
	commonProps := make(map[string]any)
	commonColumnTypes := make(map[string]string)

	if err := t.setDataAndColumnTypeFromInput(pi, pi.event.Message["userProperties"], commonProps, commonColumnTypes, &prefixInfo{
		completePrefix: "identify_userProperties_",
		completeLevel:  2,
	}); err != nil {
		return nil, nil, fmt.Errorf("identify common props: setting data and column types from message: %w", err)
	}
	if pi.dstOpts.allowUsersContextTraits {
		contextTraits := misc.MapLookup(pi.event.Message, "context", "traits")

		if err := t.setDataAndColumnTypeFromInput(pi, contextTraits, commonProps, commonColumnTypes, &prefixInfo{
			completePrefix: "identify_context_traits_",
			completeLevel:  3,
		}); err != nil {
			return nil, nil, fmt.Errorf("identify common props: setting data and column types from message: %w", err)
		}
	}
	if err := t.setDataAndColumnTypeFromInput(pi, pi.event.Message["traits"], commonProps, commonColumnTypes, &prefixInfo{
		completePrefix: "identify_traits_",
		completeLevel:  2,
	}); err != nil {
		return nil, nil, fmt.Errorf("identify common props: setting data and column types from message: %w", err)
	}
	if err := t.setDataAndColumnTypeFromInput(pi, pi.event.Message["context"], commonProps, commonColumnTypes, &prefixInfo{
		completePrefix: "identify_context_",
		completeLevel:  2,
		prefix:         "context_",
	}); err != nil {
		return nil, nil, fmt.Errorf("identify common props: setting data and column types from message: %w", err)
	}

	userIDColName, err := SafeColumnName(pi.event.Metadata.DestinationType, pi.itrOpts, "user_id")
	if err != nil {
		return nil, nil, fmt.Errorf("identify common props: safe column name: %w", err)
	}
	if k, ok := commonProps[userIDColName]; ok && k != nil {
		revUserIDCol := "_" + userIDColName

		commonProps[revUserIDCol] = commonProps[userIDColName]
		commonColumnTypes[revUserIDCol] = commonColumnTypes[userIDColName]

		delete(commonProps, userIDColName)
		delete(commonColumnTypes, userIDColName)
	}
	return commonProps, commonColumnTypes, nil
}

func (t *transformer) identifiesResponse(pi *processingInfo, commonProps map[string]any, commonColumnTypes map[string]string) ([]map[string]any, error) {
	event := make(map[string]any)
	columnTypes := make(map[string]string)

	event = lo.Assign(event, commonProps)

	if err := t.setDataAndColumnTypeFromRules(pi, event, columnTypes, rules.DefaultRules); err != nil {
		return nil, fmt.Errorf("identifies response: setting data and column types from rules: %w", err)
	}
	if err := storeRudderEvent(pi, event, columnTypes); err != nil {
		return nil, fmt.Errorf("identifies response: storing rudder event: %w", err)
	}

	identifiesTable, err := SafeTableName(pi.event.Metadata.DestinationType, pi.itrOpts, "identifies")
	if err != nil {
		return nil, fmt.Errorf("identifies response: safe table name: %w", err)
	}
	identifiesColumns, err := t.getColumns(pi.event.Metadata.DestinationType, event, lo.Assign(commonColumnTypes, columnTypes))
	if err != nil {
		return nil, fmt.Errorf("identifies response: getting columns: %w", err)
	}

	identifiesOutput := map[string]any{
		"data": event,
		"metadata": map[string]any{
			"table":      identifiesTable,
			"columns":    identifiesColumns,
			"receivedAt": pi.event.Metadata.ReceivedAt,
		},
		"userId": "",
	}
	return []map[string]any{identifiesOutput}, nil
}

func (t *transformer) usersResponse(pi *processingInfo, commonProps map[string]any, commonColumnTypes map[string]string) ([]map[string]any, error) {
	userID := misc.MapLookup(pi.event.Message, "userId")
	if userID == nil || utils.IsBlank(userID) {
		return nil, nil
	}
	if pi.itrOpts.skipUsersTable || pi.dstOpts.skipUsersTable {
		return nil, nil
	}

	event := make(map[string]any)
	columnTypes := make(map[string]string)

	event = lo.Assign(event, commonProps)

	var rulesMap map[string]rules.FunctionalRules
	if utils.IsDataLake(pi.event.Metadata.DestinationType) {
		rulesMap = rules.IdentifyRules
	} else {
		rulesMap = rules.IdentifyRulesNonDataLake
	}

	if err := t.setDataAndColumnTypeFromRules(pi, event, columnTypes, rulesMap); err != nil {
		return nil, fmt.Errorf("users response: setting data and column types from rules: %w", err)
	}

	idColName, err := SafeColumnName(pi.event.Metadata.DestinationType, pi.itrOpts, "id")
	if err != nil {
		return nil, fmt.Errorf("users response: safe column name: %w", err)
	}
	idDataType := t.getDataType(pi.event.Metadata.DestinationType, idColName, userID, false)

	event[idColName] = convertValIfDateTime(userID, idDataType)
	columnTypes[idColName] = idDataType

	receivedAtColName, err := SafeColumnName(pi.event.Metadata.DestinationType, pi.itrOpts, "received_at")
	if err != nil {
		return nil, fmt.Errorf("users response: safe column name: %w", err)
	}

	event[receivedAtColName] = convertValIfDateTime(pi.event.Metadata.ReceivedAt, "datetime")
	columnTypes[receivedAtColName] = "datetime"

	tableName, err := SafeTableName(pi.event.Metadata.DestinationType, pi.itrOpts, "users")
	if err != nil {
		return nil, fmt.Errorf("users response: safe table name: %w", err)
	}
	columns, err := t.getColumns(pi.event.Metadata.DestinationType, event, lo.Assign(commonColumnTypes, columnTypes))
	if err != nil {
		return nil, fmt.Errorf("users response: getting columns: %w", err)
	}

	usersOutput := map[string]any{
		"data": event,
		"metadata": map[string]any{
			"table":      tableName,
			"columns":    columns,
			"receivedAt": pi.event.Metadata.ReceivedAt,
		},
		"userId": "",
	}
	return []map[string]any{usersOutput}, nil
}

func (t *transformer) handlePageEvent(pi *processingInfo) ([]map[string]any, error) {
	event := make(map[string]any)
	columnTypes := make(map[string]string)

	if err := t.setDataAndColumnTypeFromInput(pi, pi.event.Message["properties"], event, columnTypes, &prefixInfo{
		completePrefix: "page_properties_",
		completeLevel:  2,
	}); err != nil {
		return nil, fmt.Errorf("page: setting data and column types from input: %w", err)
	}
	if err := t.setDataAndColumnTypeFromInput(pi, pi.event.Message["context"], event, columnTypes, &prefixInfo{
		completePrefix: "page_context_",
		completeLevel:  2,
		prefix:         "context_",
	}); err != nil {
		return nil, fmt.Errorf("page: setting data and column types from input: %w", err)
	}
	if err := t.setDataAndColumnTypeFromRules(pi, event, columnTypes, rules.DefaultRules); err != nil {
		return nil, fmt.Errorf("page: setting data and column types from rules: %w", err)
	}
	if err := t.setDataAndColumnTypeFromRules(pi, event, columnTypes, rules.PageRules); err != nil {
		return nil, fmt.Errorf("page: setting data and column types from rules: %w", err)
	}

	if err := storeRudderEvent(pi, event, columnTypes); err != nil {
		return nil, fmt.Errorf("page: storing rudder event: %w", err)
	}

	tableName, err := SafeTableName(pi.event.Metadata.DestinationType, pi.itrOpts, "pages")
	if err != nil {
		return nil, fmt.Errorf("page: safe table name: %w", err)
	}
	columns, err := t.getColumns(pi.event.Metadata.DestinationType, event, columnTypes)
	if err != nil {
		return nil, fmt.Errorf("page: getting columns: %w", err)
	}

	mergeEvents, err := t.handleMergeEvent(pi)
	if err != nil {
		return nil, fmt.Errorf("page: merge event: %w", err)
	}

	pageOutput := map[string]any{
		"data": event,
		"metadata": map[string]any{
			"table":      tableName,
			"columns":    columns,
			"receivedAt": pi.event.Metadata.ReceivedAt,
		},
		"userId": "",
	}
	return append([]map[string]any{pageOutput}, mergeEvents...), nil
}

func (t *transformer) handleScreenEvent(pi *processingInfo) ([]map[string]any, error) {
	event := make(map[string]any)
	columnTypes := make(map[string]string)

	if err := t.setDataAndColumnTypeFromInput(pi, pi.event.Message["properties"], event, columnTypes, &prefixInfo{
		completePrefix: "screen_properties_",
		completeLevel:  2,
	}); err != nil {
		return nil, fmt.Errorf("screen: setting data and column types from input: %w", err)
	}
	if err := t.setDataAndColumnTypeFromInput(pi, pi.event.Message["context"], event, columnTypes, &prefixInfo{
		completePrefix: "screen_context_",
		completeLevel:  2,
		prefix:         "context_",
	}); err != nil {
		return nil, fmt.Errorf("screen: setting data and column types from input: %w", err)
	}
	if err := t.setDataAndColumnTypeFromRules(pi, event, columnTypes, rules.DefaultRules); err != nil {
		return nil, fmt.Errorf("screen: setting data and column types from rules: %w", err)
	}
	if err := t.setDataAndColumnTypeFromRules(pi, event, columnTypes, rules.ScreenRules); err != nil {
		return nil, fmt.Errorf("screen: setting data and column types from rules: %w", err)
	}

	if err := storeRudderEvent(pi, event, columnTypes); err != nil {
		return nil, fmt.Errorf("screen: storing rudder event: %w", err)
	}

	tableName, err := SafeTableName(pi.event.Metadata.DestinationType, pi.itrOpts, "screens")
	if err != nil {
		return nil, fmt.Errorf("screen: safe table name: %w", err)
	}
	columns, err := t.getColumns(pi.event.Metadata.DestinationType, event, columnTypes)
	if err != nil {
		return nil, fmt.Errorf("screen: getting columns: %w", err)
	}

	mergeEvents, err := t.handleMergeEvent(pi)
	if err != nil {
		return nil, fmt.Errorf("screen: merge event: %w", err)
	}

	screenOutput := map[string]any{
		"data": event,
		"metadata": map[string]any{
			"table":      tableName,
			"columns":    columns,
			"receivedAt": pi.event.Metadata.ReceivedAt,
		},
		"userId": "",
	}
	return append([]map[string]any{screenOutput}, mergeEvents...), nil
}

func (t *transformer) handleGroupEvent(pi *processingInfo) ([]map[string]any, error) {
	event := make(map[string]any)
	columnTypes := make(map[string]string)

	if err := t.setDataAndColumnTypeFromInput(pi, pi.event.Message["traits"], event, columnTypes, &prefixInfo{
		completePrefix: "group_traits_",
		completeLevel:  2,
	}); err != nil {
		return nil, fmt.Errorf("group: setting data and column types from input: %w", err)
	}
	if err := t.setDataAndColumnTypeFromInput(pi, pi.event.Message["context"], event, columnTypes, &prefixInfo{
		completePrefix: "group_context_",
		completeLevel:  2,
		prefix:         "context_",
	}); err != nil {
		return nil, fmt.Errorf("group: setting data and column types from input: %w", err)
	}
	if err := t.setDataAndColumnTypeFromRules(pi, event, columnTypes, rules.DefaultRules); err != nil {
		return nil, fmt.Errorf("group: setting data and column types from rules: %w", err)
	}
	if err := t.setDataAndColumnTypeFromRules(pi, event, columnTypes, rules.GroupRules); err != nil {
		return nil, fmt.Errorf("group: setting data and column types from rules: %w", err)
	}

	if err := storeRudderEvent(pi, event, columnTypes); err != nil {
		return nil, fmt.Errorf("group: storing rudder event: %w", err)
	}

	tableName, err := SafeTableName(pi.event.Metadata.DestinationType, pi.itrOpts, "groups")
	if err != nil {
		return nil, fmt.Errorf("group: safe table name: %w", err)
	}
	columns, err := t.getColumns(pi.event.Metadata.DestinationType, event, columnTypes)
	if err != nil {
		return nil, fmt.Errorf("group: getting columns: %w", err)
	}

	mergeEvents, err := t.handleMergeEvent(pi)
	if err != nil {
		return nil, fmt.Errorf("group: merge event: %w", err)
	}

	groupOutput := map[string]any{
		"data": event,
		"metadata": map[string]any{
			"table":      tableName,
			"columns":    columns,
			"receivedAt": pi.event.Metadata.ReceivedAt,
		},
		"userId": "",
	}
	return append([]map[string]any{groupOutput}, mergeEvents...), nil
}

func (t *transformer) handleAliasEvent(pi *processingInfo) ([]map[string]any, error) {
	event := make(map[string]any)
	columnTypes := make(map[string]string)

	if err := t.setDataAndColumnTypeFromInput(pi, pi.event.Message["traits"], event, columnTypes, &prefixInfo{
		completePrefix: "alias_traits_",
		completeLevel:  2,
	}); err != nil {
		return nil, fmt.Errorf("alias: setting data and column types from input: %w", err)
	}
	if err := t.setDataAndColumnTypeFromInput(pi, pi.event.Message["context"], event, columnTypes, &prefixInfo{
		completePrefix: "alias_context_",
		completeLevel:  2,
		prefix:         "context_",
	}); err != nil {
		return nil, fmt.Errorf("alias: setting data and column types from input: %w", err)
	}
	if err := t.setDataAndColumnTypeFromRules(pi, event, columnTypes, rules.DefaultRules); err != nil {
		return nil, fmt.Errorf("alias: setting data and column types from rules: %w", err)
	}
	if err := t.setDataAndColumnTypeFromRules(pi, event, columnTypes, rules.AliasRules); err != nil {
		return nil, fmt.Errorf("alias: setting data and column types from rules: %w", err)
	}

	if err := storeRudderEvent(pi, event, columnTypes); err != nil {
		return nil, fmt.Errorf("alias: storing rudder event: %w", err)
	}

	tableName, err := SafeTableName(pi.event.Metadata.DestinationType, pi.itrOpts, "aliases")
	if err != nil {
		return nil, fmt.Errorf("alias: safe table name: %w", err)
	}
	columns, err := t.getColumns(pi.event.Metadata.DestinationType, event, columnTypes)
	if err != nil {
		return nil, fmt.Errorf("alias: getting columns: %w", err)
	}

	mergeEvents, err := t.handleMergeEvent(pi)
	if err != nil {
		return nil, fmt.Errorf("merge event: %w", err)
	}

	aliasOutput := map[string]any{
		"data": event,
		"metadata": map[string]any{
			"table":      tableName,
			"columns":    columns,
			"receivedAt": pi.event.Metadata.ReceivedAt,
		},
		"userId": "",
	}
	return append([]map[string]any{aliasOutput}, mergeEvents...), nil
}

func (t *transformer) handleMergeEvent(pi *processingInfo) ([]map[string]any, error) {
	if !t.config.enableIDResolution.Load() {
		return nil, nil
	}
	if !utils.IsIdentityEnabled(pi.event.Metadata.DestinationType) {
		return nil, nil
	}

	mergeProp1, mergeProp2, err := mergeProps(pi.event.Message, pi.event.Metadata)
	if err != nil {
		return nil, fmt.Errorf("merge: merge properties: %w", err)
	}
	if isMergePropEmpty(mergeProp1) {
		return nil, nil
	}

	tableName, err := mergeRuleTable(pi.event.Metadata.DestinationType, pi.itrOpts)
	if err != nil {
		return nil, fmt.Errorf("merge: merge rules table: %w", err)
	}
	columns, err := mergeRuleColumns(pi.event.Metadata.DestinationType, pi.itrOpts)
	if err != nil {
		return nil, fmt.Errorf("merge: merge columns: %w", err)
	}

	event := map[string]any{
		columns.Prop1Type:  utils.ToString(mergeProp1.Type),
		columns.Prop1Value: utils.ToString(mergeProp1.Value),
	}
	columnTypes := map[string]any{
		columns.Prop1Type:  model.StringDataType,
		columns.Prop1Value: model.StringDataType,
	}
	metadata := map[string]any{
		"table":        tableName,
		"columns":      columnTypes,
		"isMergeRule":  true,
		"receivedAt":   pi.event.Metadata.ReceivedAt,
		"mergePropOne": event[columns.Prop1Value],
	}

	if !isMergePropEmpty(mergeProp2) {
		event[columns.Prop2Type] = utils.ToString(mergeProp2.Type)
		event[columns.Prop2Value] = utils.ToString(mergeProp2.Value)
		columnTypes[columns.Prop2Type] = model.StringDataType
		columnTypes[columns.Prop2Value] = model.StringDataType

		metadata["mergePropTwo"] = event[columns.Prop2Value]
	}

	mergeOutput := map[string]any{
		"data":     event,
		"metadata": metadata,
		"userId":   "",
	}
	return []map[string]any{mergeOutput}, nil
}

func mergeProps(message types.SingularEventT, metadata ptrans.Metadata) (*mergeRule, *mergeRule, error) {
	switch strings.ToLower(metadata.EventType) {
	case "merge":
		return mergePropsForMergeEventType(message)
	case "alias":
		return mergePropsForAliasEventType(message)
	default:
		return mergePropsForDefaultEventType(message)
	}
}

func mergePropsForMergeEventType(message types.SingularEventT) (*mergeRule, *mergeRule, error) {
	mergeProperties := misc.MapLookup(message, "mergeProperties")
	if mergeProperties == nil {
		return nil, nil, response.ErrMergePropertiesMissing
	}
	mergePropertiesArr, ok := mergeProperties.([]any)
	if !ok {
		return nil, nil, response.ErrMergePropertiesNotArray
	}
	if len(mergePropertiesArr) != 2 {
		return nil, nil, response.ErrMergePropertiesNotSufficient
	}

	mergePropertiesMap0, ok := mergePropertiesArr[0].(map[string]any)
	if !ok {
		return nil, nil, response.ErrMergePropertyOneInvalid
	}
	mergePropertiesMap1, ok := mergePropertiesArr[1].(map[string]any)
	if !ok {
		return nil, nil, response.ErrMergePropertyTwoInvalid
	}

	mergeProperties0Type := misc.MapLookup(mergePropertiesMap0, "type")
	mergeProperties0Value := misc.MapLookup(mergePropertiesMap0, "value")
	mergeProperties1Type := misc.MapLookup(mergePropertiesMap1, "type")
	mergeProperties1Value := misc.MapLookup(mergePropertiesMap1, "value")

	if mergeProperties0Type == nil || mergeProperties0Value == nil || mergeProperties1Type == nil || mergeProperties1Value == nil {
		return nil, nil, response.ErrMergePropertyEmpty
	}
	if utils.IsBlank(mergeProperties0Type) || utils.IsBlank(mergeProperties0Value) || utils.IsBlank(mergeProperties1Type) || utils.IsBlank(mergeProperties1Value) {
		return nil, nil, response.ErrMergePropertyEmpty
	}

	mergeProp1 := &mergeRule{Type: mergeProperties0Type, Value: mergeProperties0Value}
	mergeProp2 := &mergeRule{Type: mergeProperties1Type, Value: mergeProperties1Value}
	return mergeProp1, mergeProp2, nil
}

func mergePropsForAliasEventType(message types.SingularEventT) (*mergeRule, *mergeRule, error) {
	userID := misc.MapLookup(message, "userId")
	previousID := misc.MapLookup(message, "previousId")

	mergeProp1 := &mergeRule{Type: "user_id", Value: userID}
	mergeProp2 := &mergeRule{Type: "user_id", Value: previousID}
	return mergeProp1, mergeProp2, nil
}

func mergePropsForDefaultEventType(message types.SingularEventT) (*mergeRule, *mergeRule, error) {
	anonymousID := misc.MapLookup(message, "anonymousId")
	userID := misc.MapLookup(message, "userId")

	var mergeProp1, mergeProp2 *mergeRule
	if anonymousID == nil || utils.IsBlank(anonymousID) {
		mergeProp1 = &mergeRule{Type: "user_id", Value: userID}
	} else {
		mergeProp1 = &mergeRule{Type: "anonymous_id", Value: anonymousID}
		mergeProp2 = &mergeRule{Type: "user_id", Value: userID}
	}
	return mergeProp1, mergeProp2, nil
}

func mergeRuleTable(destType string, options integrationsOptions) (string, error) {
	return SafeTableName(destType, options, "rudder_identity_merge_rules")
}

func mergeRuleColumns(destType string, options integrationsOptions) (*mergeRulesColumns, error) {
	var (
		columns [4]string
		err     error
	)

	for i, col := range []string{
		"merge_property_1_type", "merge_property_1_value", "merge_property_2_type", "merge_property_2_value",
	} {
		if columns[i], err = SafeColumnName(destType, options, col); err != nil {
			return nil, fmt.Errorf("safe column name for %s: %w", col, err)
		}
	}

	rulesColumns := &mergeRulesColumns{
		Prop1Type: columns[0], Prop1Value: columns[1], Prop2Type: columns[2], Prop2Value: columns[3],
	}
	return rulesColumns, nil
}

func isMergePropEmpty(mergeProp *mergeRule) bool {
	return mergeProp == nil || mergeProp.Type == nil || mergeProp.Value == nil || utils.IsBlank(mergeProp.Type) || utils.IsBlank(mergeProp.Value)
}

func (t *transformer) getDataType(destType, key string, val any, isJSONKey bool) string {
	if typeName := getPrimitiveType(val); typeName != "" {
		return typeName
	}
	if strVal, ok := val.(string); ok && utils.ValidTimestamp(strVal) {
		return model.DateTimeDataType
	}
	if override := getDataTypeOverride(destType, key, val, isJSONKey); override != "" {
		return override
	}
	return model.StringDataType
}

func getPrimitiveType(val any) string {
	switch v := val.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return model.IntDataType
	case float64:
		return getFloatType(v)
	case float32:
		return getFloatType(float64(v))
	case bool:
		return model.BooleanDataType
	default:
		return ""
	}
}

func getFloatType(v float64) string {
	if v == float64(int64(v)) {
		return model.IntDataType
	}
	return model.FloatDataType
}

func getDataTypeOverride(destType, key string, val any, isJSONKey bool) string {
	switch destType {
	case whutils.POSTGRES, whutils.SNOWFLAKE, whutils.SnowpipeStreaming:
		if key == violationErrors || isJSONKey {
			return model.JSONDataType
		}
		return model.StringDataType
	case whutils.RS:
		return getDataTypeOverrideForRedshift(val, isJSONKey)
	default:
		return ""
	}
}

func getDataTypeOverrideForRedshift(val any, isJSONKey bool) string {
	if isJSONKey {
		return model.JSONDataType
	}
	if val == nil {
		return model.StringDataType
	}
	if jsonVal, _ := json.Marshal(val); len(jsonVal) > redshiftStringLimit {
		return model.TextDataType
	}
	return model.StringDataType
}

func (t *transformer) getColumns(destType string, data map[string]any, columnTypes map[string]string) (map[string]any, error) {
	columns := make(map[string]any)

	// uuid_ts and loaded_at datatypes are passed from here to create appropriate columns.
	// Corresponding values are inserted when loading into the warehouse
	uuidTS := "uuid_ts"
	if destType == whutils.SNOWFLAKE || destType == whutils.SnowpipeStreaming {
		uuidTS = "UUID_TS"
	}
	columns[uuidTS] = "datetime"

	if destType == whutils.BQ {
		columns["loaded_at"] = "datetime"
	}

	for key, value := range data {
		if dataType, ok := columnTypes[key]; ok {
			columns[key] = dataType
		} else {
			columns[key] = t.getDataType(destType, key, value, false)
		}
	}
	if len(columns) > t.config.maxColumnsInEvent.Load() && !utils.IsRudderSources(data) && !utils.IsDataLake(destType) {
		return nil, response.NewTransformerError(fmt.Sprintf("%s transformer: Too many columns outputted from the event", strings.ToLower(destType)), http.StatusBadRequest)
	}
	return columns, nil
}

func (t *transformer) setDataAndColumnTypeFromInput(
	processInfo *processingInfo, input any,
	data map[string]any, columnType map[string]string,
	prefixDetails *prefixInfo,
) error {
	if input == nil || !utils.IsObject(input) {
		return nil
	}

	inputMap := input.(map[string]any)

	if len(inputMap) == 0 {
		return nil
	}
	if t.shouldHandleStringLikeObject(prefixDetails, inputMap) {
		return t.handleStringLikeObject(processInfo, prefixDetails, data, columnType, inputMap)
	}
	for key, val := range inputMap {
		if val == nil || utils.IsBlank(val) {
			continue
		}

		if t.isValidJSONPath(processInfo, prefixDetails, key) {
			if err := t.handleValidJSONPath(processInfo, prefixDetails, key, val, data, columnType); err != nil {
				return fmt.Errorf("handling valid JSON path: %w", err)
			}
		} else if t.shouldProcessNestedObject(processInfo, prefixDetails, val) {
			if err := t.processNestedObject(processInfo, val.(map[string]any), data, columnType, prefixDetails, key); err != nil {
				return fmt.Errorf("processing nested object: %w", err)
			}
		} else {
			if err := t.processNonNestedObject(processInfo, prefixDetails, key, val, data, columnType); err != nil {
				return fmt.Errorf("handling non-nested object: %w", err)
			}
		}
	}
	return nil
}

func (t *transformer) shouldHandleStringLikeObject(prefixDetails *prefixInfo, inputMap map[string]any) bool {
	return (strings.HasSuffix(prefixDetails.completePrefix, "context_traits_") || prefixDetails.completePrefix == "group_traits_") && utils.IsStringLikeObject(inputMap)
}

func (t *transformer) handleStringLikeObject(processInfo *processingInfo, prefixDetails *prefixInfo, data map[string]any, columnType map[string]string, inputMap map[string]any) error {
	if prefixDetails.prefix == "context_traits_" {
		err := t.addColumnTypeAndValue(processInfo, prefixDetails.prefix, utils.StringLikeObjectToString(inputMap), false, data, columnType)
		if err != nil {
			return fmt.Errorf("adding column type and value: %w", err)
		}
		return nil
	}
	return nil
}

func (t *transformer) isValidJSONPath(processInfo *processingInfo, prefixDetails *prefixInfo, key string) bool {
	validLegacyJSONPath := isValidLegacyJSONPathKey(processInfo.event.Metadata.EventType, prefixDetails.prefix+key, prefixDetails.level, processInfo.jsonPathsInfo.legacyKeysMap)
	validJSONPath := isValidJSONPathKey(prefixDetails.completePrefix+key, prefixDetails.completeLevel, processInfo.jsonPathsInfo.keysMap)
	return validLegacyJSONPath || validJSONPath
}

func (t *transformer) handleValidJSONPath(processInfo *processingInfo, prefixDetails *prefixInfo, key string, val any, data map[string]any, columnType map[string]string) error {
	valJSON, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("marshalling value: %w", err)
	}
	return t.addColumnTypeAndValue(processInfo, prefixDetails.prefix+key, string(valJSON), true, data, columnType)
}

func (t *transformer) shouldProcessNestedObject(processInfo *processingInfo, prefixDetails *prefixInfo, val any) bool {
	return utils.IsObject(val) && (processInfo.event.Metadata.SourceCategory != "cloud" || prefixDetails.level < 3)
}

func (t *transformer) processNestedObject(
	processInfo *processingInfo, val map[string]any,
	data map[string]any, columnType map[string]string,
	prefixDetails *prefixInfo, key string,
) error {
	newPrefixDetails := &prefixInfo{
		completePrefix: prefixDetails.completePrefix + key + "_",
		completeLevel:  prefixDetails.completeLevel + 1,
		prefix:         prefixDetails.prefix + key + "_",
		level:          prefixDetails.level + 1,
	}
	return t.setDataAndColumnTypeFromInput(processInfo, val, data, columnType, newPrefixDetails)
}

func (t *transformer) processNonNestedObject(processInfo *processingInfo, prefixDetails *prefixInfo, key string, val any, data map[string]any, columnType map[string]string) error {
	finalValue := val
	if processInfo.event.Metadata.SourceCategory == "cloud" && prefixDetails.level >= 3 && utils.IsObject(val) {
		jsonData, err := json.Marshal(val)
		if err != nil {
			return fmt.Errorf("marshalling value: %w", err)
		}
		finalValue = string(jsonData)
	}
	return t.addColumnTypeAndValue(processInfo, prefixDetails.prefix+key, finalValue, false, data, columnType)
}

func (t *transformer) addColumnTypeAndValue(pi *processingInfo, key string, val any, isJSONKey bool, data map[string]any, columnType map[string]string) error {
	columnName := TransformColumnName(pi.event.Metadata.DestinationType, pi.itrOpts, pi.dstOpts, key)
	if len(columnName) == 0 {
		return nil
	}

	safeKey, err := SafeColumnName(pi.event.Metadata.DestinationType, pi.itrOpts, columnName)
	if err != nil {
		return fmt.Errorf("transforming column name: %w", err)
	}

	if rules.IsRudderReservedColumn(pi.event.Metadata.EventType, safeKey) {
		return nil
	}

	dataType := t.getDataType(pi.event.Metadata.DestinationType, key, val, isJSONKey)

	data[safeKey] = convertValIfDateTime(val, dataType)
	columnType[safeKey] = dataType
	return nil
}

func (t *transformer) setDataAndColumnTypeFromRules(
	pi *processingInfo,
	data map[string]any, columnType map[string]string,
	functionalRules map[string]rules.FunctionalRules,
) error {
	for colKey, functionalRule := range functionalRules {
		columnName, err := SafeColumnName(pi.event.Metadata.DestinationType, pi.itrOpts, colKey)
		if err != nil {
			return fmt.Errorf("safe column name: %w", err)
		}

		delete(data, columnName)
		delete(columnType, columnName)

		colVal, err := functionalRule(&pi.event)
		if err != nil {
			return fmt.Errorf("applying functional rule: %w", err)
		}
		if colVal == nil || utils.IsBlank(colVal) || utils.IsObject(colVal) {
			continue
		}

		dataType := t.getDataType(pi.event.Metadata.DestinationType, colKey, colVal, false)

		data[columnName] = convertValIfDateTime(colVal, dataType)
		columnType[columnName] = dataType
	}
	return nil
}

func convertValIfDateTime(val any, colType string) any {
	if colType == model.DateTimeDataType {
		return utils.ToTimestamp(val)
	}
	return val
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

func storeRudderEvent(pi *processingInfo, output map[string]any, columnType map[string]string) error {
	if !pi.dstOpts.storeFullEvent {
		return nil
	}

	columnName, err := SafeColumnName(pi.event.Metadata.DestinationType, pi.itrOpts, "rudder_event")
	if err != nil {
		return fmt.Errorf("safe column name: %w", err)
	}

	eventJSON, err := json.Marshal(pi.event.Message)
	if err != nil {
		return fmt.Errorf("marshalling event: %w", err)
	}

	output[columnName] = string(eventJSON)
	columnType[columnName] = utils.GetFullEventColumnTypeByDestType(pi.event.Metadata.DestinationType)
	return nil
}

func excludeRudderCreatedTableNames(name string, skipReservedKeywordsEscaping bool) string {
	if utils.IsRudderIsolatedTable(name) || (utils.IsRudderCreatedTable(name) && !skipReservedKeywordsEscaping) {
		return "_" + name
	}
	return name
}

func isValidJSONPathKey(key string, level int, jsonKeys map[string]int) bool {
	if val, exists := jsonKeys[key]; exists {
		return val == level
	}
	return false
}

func isValidLegacyJSONPathKey(eventType, key string, level int, jsonKeys map[string]int) bool {
	if eventType == "track" {
		return isValidJSONPathKey(key, level, jsonKeys)
	}
	return false
}

func extractJSONPathInfo(jsonPaths []string) jsonPathInfo {
	jp := jsonPathInfo{
		keysMap:       make(map[string]int),
		legacyKeysMap: make(map[string]int),
	}
	for _, jsonPath := range jsonPaths {
		if trimmedJSONPath := strings.TrimSpace(jsonPath); trimmedJSONPath != "" {
			splitPaths := strings.Split(jsonPath, ".")
			key := strings.Join(splitPaths, "_")
			pos := len(splitPaths) - 1

			if utils.HasJSONPathPrefix(jsonPath) {
				jp.keysMap[key] = pos
				continue
			}
			jp.legacyKeysMap[key] = pos
		}
	}
	return jp
}
