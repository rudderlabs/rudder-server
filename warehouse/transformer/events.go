package transformer

import (
	"fmt"
	"strings"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/response"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/rules"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func (t *Transformer) trackEvents(tec *transformEventContext) ([]map[string]any, error) {
	commonData, commonMetadata, transformedEventName, err := t.trackCommonProps(tec)
	if err != nil {
		return nil, fmt.Errorf("track common properties: %w", err)
	}

	tracksResponse, err := t.tracksResponse(tec, commonData, commonMetadata)
	if err != nil {
		return nil, fmt.Errorf("track response: %w", err)
	}

	trackEventsResponse, err := t.trackEventsResponse(tec, transformedEventName, commonData, commonMetadata)
	if err != nil {
		return nil, fmt.Errorf("track events response: %w", err)
	}
	return append(tracksResponse, trackEventsResponse...), nil
}

func (t *Transformer) trackCommonProps(tec *transformEventContext) (map[string]any, map[string]string, string, error) {
	commonData := make(map[string]any)
	commonMetadata := make(map[string]string)

	if err := setDataAndMetadataFromInput(tec, tec.event.Message["context"], commonData, commonMetadata, &prefixInfo{
		completePrefix: "track_context_",
		completeLevel:  2,
		prefix:         "context_",
	}); err != nil {
		return nil, nil, "", fmt.Errorf("track common props: setting data and column types from message: %w", err)
	}
	if err := setDataAndMetadataFromRules(tec, commonData, commonMetadata, rules.TrackRules); err != nil {
		return nil, nil, "", fmt.Errorf("track common props: setting data and column types from rules: %w", err)
	}
	if err := setDataAndMetadataFromRules(tec, commonData, commonMetadata, rules.DefaultRules); err != nil {
		return nil, nil, "", fmt.Errorf("track common props: setting data and column types from rules: %w", err)
	}

	eventColName, err := safeColumnNameCached(tec, "event")
	if err != nil {
		return nil, nil, "", fmt.Errorf("track common props: safe column name: %w", err)
	}
	eventTextColName, err := safeColumnNameCached(tec, "event_text")
	if err != nil {
		return nil, nil, "", fmt.Errorf("track common props: safe column name: %w", err)
	}

	var eventName, transformedEventName string
	if d, dok := commonData[eventTextColName]; dok {
		eventName, _ = d.(string)
	}
	transformedEventName = transformTableNameCached(tec, eventName)

	commonData[eventColName] = transformedEventName
	commonMetadata[eventColName] = model.StringDataType
	return commonData, commonMetadata, transformedEventName, nil
}

func (t *Transformer) tracksResponse(tec *transformEventContext, commonData map[string]any, commonMetadata map[string]string) ([]map[string]any, error) {
	if tec.intrOpts.skipTracksTable || tec.destOpts.skipTracksTable {
		return nil, nil
	}

	data := make(map[string]any)
	metadata := make(map[string]string)

	data = lo.Assign(data, commonData)

	if err := setDataAndMetadataFromRules(tec, data, metadata, rules.TrackTableRules); err != nil {
		return nil, fmt.Errorf("tracks response: setting data and column types from rules: %w", err)
	}
	if err := storeRudderEvent(tec, data, metadata); err != nil {
		return nil, fmt.Errorf("tracks response: storing rudder event: %w", err)
	}

	table, err := safeTableNameCached(tec, "tracks")
	if err != nil {
		return nil, fmt.Errorf("tracks response: safe table name: %w", err)
	}
	columns, err := t.getColumns(tec.event.Metadata.DestinationType, data, lo.Assign(metadata, commonMetadata))
	if err != nil {
		return nil, fmt.Errorf("tracks response: getting columns: %w", err)
	}

	output := map[string]any{
		"data": data,
		"metadata": map[string]any{
			"table":      table,
			"columns":    columns,
			"receivedAt": tec.event.Metadata.ReceivedAt,
		},
		"userId": "",
	}
	return []map[string]any{output}, nil
}

func (t *Transformer) trackEventsResponse(tec *transformEventContext, transformerEventName string, commonData map[string]any, commonMetadata map[string]string) ([]map[string]any, error) {
	if len(transformerEventName) == 0 || len(strings.TrimSpace(transformerEventName)) == 0 {
		return nil, nil
	}

	data := make(map[string]any)
	metadata := make(map[string]string)

	if err := setDataAndMetadataFromInput(tec, tec.event.Message["properties"], data, metadata, &prefixInfo{
		completePrefix: "track_properties_",
		completeLevel:  2,
	}); err != nil {
		return nil, fmt.Errorf("track events response: setting data and column types from message: %w", err)
	}
	if err := setDataAndMetadataFromInput(tec, tec.event.Message["userProperties"], data, metadata, &prefixInfo{
		completePrefix: "track_userProperties_",
		completeLevel:  2,
	}); err != nil {
		return nil, fmt.Errorf("track events response: setting data and column types from message: %w", err)
	}
	if err := setDataAndMetadataFromRules(tec, commonData, commonMetadata, rules.TrackEventTableRules); err != nil {
		return nil, fmt.Errorf("track events response: setting data and column types from rules: %w", err)
	}

	eventTableEvent := lo.Assign(data, commonData)

	columnName := transformColumnNameCached(tec, transformerEventName)
	table, err := safeTableNameCached(tec, columnName)
	if err != nil {
		return nil, fmt.Errorf("track events response: safe table name: %w", err)
	}
	excludeTable := excludeRudderCreatedTableNames(table, tec.intrOpts.skipReservedKeywordsEscaping)

	columns, err := t.getColumns(tec.event.Metadata.DestinationType, eventTableEvent, lo.Assign(metadata, commonMetadata))
	if err != nil {
		return nil, fmt.Errorf("track events response: getting columns: %w", err)
	}

	mergeOutput, err := t.mergeEvents(tec)
	if err != nil {
		return nil, fmt.Errorf("track events response: merge event: %w", err)
	}

	trackOutput := map[string]any{
		"data": eventTableEvent,
		"metadata": map[string]any{
			"table":      excludeTable,
			"columns":    columns,
			"receivedAt": tec.event.Metadata.ReceivedAt,
		},
		"userId": "",
	}
	return append([]map[string]any{trackOutput}, mergeOutput...), nil
}

func (t *Transformer) extractEvents(tec *transformEventContext) ([]map[string]any, error) {
	data := make(map[string]any)
	metadata := make(map[string]string)

	if err := setDataAndMetadataFromInput(tec, tec.event.Message["context"], data, metadata, &prefixInfo{
		completePrefix: "extract_context_",
		completeLevel:  2,
		prefix:         "context_",
	}); err != nil {
		return nil, fmt.Errorf("extract: setting data and column types from input: %w", err)
	}
	if err := setDataAndMetadataFromInput(tec, tec.event.Message["properties"], data, metadata, &prefixInfo{
		completePrefix: "extract_properties_",
		completeLevel:  2,
	}); err != nil {
		return nil, fmt.Errorf("extract: setting data and column types from input: %w", err)
	}

	eventColName, err := safeColumnNameCached(tec, "event")
	if err != nil {
		return nil, fmt.Errorf("extract: safe column name: %w", err)
	}

	eventName, _ := tec.event.Message[eventColName].(string)
	transformedEventName := transformTableNameCached(tec, eventName)
	if utils.IsBlank(transformedEventName) {
		return nil, response.ErrExtractEventNameEmpty
	}

	data[eventColName] = transformedEventName
	metadata[eventColName] = model.StringDataType

	if err = setDataAndMetadataFromRules(tec, data, metadata, rules.ExtractRules); err != nil {
		return nil, fmt.Errorf("extract: setting data and column types from rules: %w", err)
	}

	columnName := transformColumnNameCached(tec, transformedEventName)
	tableName, err := safeTableNameCached(tec, columnName)
	if err != nil {
		return nil, fmt.Errorf("extract: safe table name: %w", err)
	}
	excludeTableName := excludeRudderCreatedTableNames(tableName, tec.intrOpts.skipReservedKeywordsEscaping)

	columns, err := t.getColumns(tec.event.Metadata.DestinationType, data, metadata)
	if err != nil {
		return nil, fmt.Errorf("extract: getting columns: %w", err)
	}

	extractOutput := map[string]any{
		"data": data,
		"metadata": map[string]any{
			"table":      excludeTableName,
			"columns":    columns,
			"receivedAt": tec.event.Metadata.ReceivedAt,
		},
		"userId": "",
	}
	return []map[string]any{extractOutput}, nil
}

func excludeRudderCreatedTableNames(name string, skipReservedKeywordsEscaping bool) string {
	if utils.IsRudderIsolatedTable(name) || (utils.IsRudderCreatedTable(name) && !skipReservedKeywordsEscaping) {
		return "_" + name
	}
	return name
}

func (t *Transformer) identifyEvents(tec *transformEventContext) ([]map[string]any, error) {
	commonData, commonMetadata, err := t.identifyCommonProps(tec)
	if err != nil {
		return nil, fmt.Errorf("identifies: common properties: %w", err)
	}

	identifiesResponse, err := t.identifiesResponse(tec, commonData, commonMetadata)
	if err != nil {
		return nil, fmt.Errorf("identifies response: %w", err)
	}

	mergeOutput, err := t.mergeEvents(tec)
	if err != nil {
		return nil, fmt.Errorf("identifies: merge event: %w", err)
	}

	usersResponse, err := t.usersResponse(tec, commonData, commonMetadata)
	if err != nil {
		return nil, fmt.Errorf("identifies: users response: %w", err)
	}
	return append(append(identifiesResponse, mergeOutput...), usersResponse...), nil
}

func (t *Transformer) identifyCommonProps(tec *transformEventContext) (map[string]any, map[string]string, error) {
	commonData := make(map[string]any)
	commonMetadata := make(map[string]string)

	if err := setDataAndMetadataFromInput(tec, tec.event.Message["userProperties"], commonData, commonMetadata, &prefixInfo{
		completePrefix: "identify_userProperties_",
		completeLevel:  2,
	}); err != nil {
		return nil, nil, fmt.Errorf("identify common props: setting data and column types from message: %w", err)
	}
	if tec.destOpts.allowUsersContextTraits {
		contextTraits := misc.MapLookup(tec.event.Message, "context", "traits")

		if err := setDataAndMetadataFromInput(tec, contextTraits, commonData, commonMetadata, &prefixInfo{
			completePrefix: "identify_context_traits_",
			completeLevel:  3,
		}); err != nil {
			return nil, nil, fmt.Errorf("identify common props: setting data and column types from message: %w", err)
		}
	}
	if err := setDataAndMetadataFromInput(tec, tec.event.Message["traits"], commonData, commonMetadata, &prefixInfo{
		completePrefix: "identify_traits_",
		completeLevel:  2,
	}); err != nil {
		return nil, nil, fmt.Errorf("identify common props: setting data and column types from message: %w", err)
	}
	if err := setDataAndMetadataFromInput(tec, tec.event.Message["context"], commonData, commonMetadata, &prefixInfo{
		completePrefix: "identify_context_",
		completeLevel:  2,
		prefix:         "context_",
	}); err != nil {
		return nil, nil, fmt.Errorf("identify common props: setting data and column types from message: %w", err)
	}

	userIDColName, err := safeColumnNameCached(tec, "user_id")
	if err != nil {
		return nil, nil, fmt.Errorf("identify common props: safe column name: %w", err)
	}
	if k, ok := commonData[userIDColName]; ok && k != nil {
		revUserIDCol := "_" + userIDColName

		commonData[revUserIDCol] = commonData[userIDColName]
		commonMetadata[revUserIDCol] = commonMetadata[userIDColName]

		delete(commonData, userIDColName)
		delete(commonMetadata, userIDColName)
	}
	return commonData, commonMetadata, nil
}

func (t *Transformer) identifiesResponse(tec *transformEventContext, commonData map[string]any, commonMetadata map[string]string) ([]map[string]any, error) {
	data := lo.Assign(commonData)
	metadata := make(map[string]string)

	if err := setDataAndMetadataFromRules(tec, data, metadata, rules.DefaultRules); err != nil {
		return nil, fmt.Errorf("identifies response: setting data and column types from rules: %w", err)
	}
	if err := storeRudderEvent(tec, data, metadata); err != nil {
		return nil, fmt.Errorf("identifies response: storing rudder event: %w", err)
	}

	identifiesTable, err := safeTableNameCached(tec, "identifies")
	if err != nil {
		return nil, fmt.Errorf("identifies response: safe table name: %w", err)
	}
	identifiesColumns, err := t.getColumns(tec.event.Metadata.DestinationType, data, lo.Assign(commonMetadata, metadata))
	if err != nil {
		return nil, fmt.Errorf("identifies response: getting columns: %w", err)
	}

	identifiesOutput := map[string]any{
		"data": data,
		"metadata": map[string]any{
			"table":      identifiesTable,
			"columns":    identifiesColumns,
			"receivedAt": tec.event.Metadata.ReceivedAt,
		},
		"userId": "",
	}
	return []map[string]any{identifiesOutput}, nil
}

func (t *Transformer) usersResponse(tec *transformEventContext, commonData map[string]any, commonMetadata map[string]string) ([]map[string]any, error) {
	userID := misc.MapLookup(tec.event.Message, "userId")
	if utils.IsBlank(userID) {
		return nil, nil
	}
	if shouldSkipUsersTable(tec) {
		return nil, nil
	}

	data := lo.Assign(commonData)
	metadata := make(map[string]string)

	var rulesMap map[string]rules.Rules
	if utils.IsDataLake(tec.event.Metadata.DestinationType) {
		rulesMap = rules.IdentifyRules
	} else {
		rulesMap = rules.IdentifyRulesNonDataLake
	}

	if err := setDataAndMetadataFromRules(tec, data, metadata, rulesMap); err != nil {
		return nil, fmt.Errorf("users response: setting data and column types from rules: %w", err)
	}

	idColName, err := safeColumnNameCached(tec, "id")
	if err != nil {
		return nil, fmt.Errorf("users response: safe column name: %w", err)
	}
	idDataType := dataTypeFor(tec.event.Metadata.DestinationType, idColName, userID, false)

	data[idColName] = convertValIfDateTime(userID, idDataType)
	metadata[idColName] = idDataType

	receivedAtColName, err := safeColumnNameCached(tec, "received_at")
	if err != nil {
		return nil, fmt.Errorf("users response: safe column name: %w", err)
	}

	data[receivedAtColName] = convertValIfDateTime(tec.event.Metadata.ReceivedAt, model.DateTimeDataType)
	metadata[receivedAtColName] = model.DateTimeDataType

	tableName, err := safeTableNameCached(tec, "users")
	if err != nil {
		return nil, fmt.Errorf("users response: safe table name: %w", err)
	}
	columns, err := t.getColumns(tec.event.Metadata.DestinationType, data, lo.Assign(commonMetadata, metadata))
	if err != nil {
		return nil, fmt.Errorf("users response: getting columns: %w", err)
	}

	usersOutput := map[string]any{
		"data": data,
		"metadata": map[string]any{
			"table":      tableName,
			"columns":    columns,
			"receivedAt": tec.event.Metadata.ReceivedAt,
		},
		"userId": "",
	}
	return []map[string]any{usersOutput}, nil
}

func shouldSkipUsersTable(tec *transformEventContext) bool {
	return tec.event.Metadata.DestinationType == whutils.SnowpipeStreaming || tec.destOpts.skipUsersTable || tec.intrOpts.skipUsersTable
}

func (t *Transformer) pageEvents(tec *transformEventContext) ([]map[string]any, error) {
	data := make(map[string]any)
	metadata := make(map[string]string)

	if err := setDataAndMetadataFromInput(tec, tec.event.Message["properties"], data, metadata, &prefixInfo{
		completePrefix: "page_properties_",
		completeLevel:  2,
	}); err != nil {
		return nil, fmt.Errorf("page: setting data and column types from input: %w", err)
	}
	if err := setDataAndMetadataFromInput(tec, tec.event.Message["context"], data, metadata, &prefixInfo{
		completePrefix: "page_context_",
		completeLevel:  2,
		prefix:         "context_",
	}); err != nil {
		return nil, fmt.Errorf("page: setting data and column types from input: %w", err)
	}
	if err := setDataAndMetadataFromRules(tec, data, metadata, rules.DefaultRules); err != nil {
		return nil, fmt.Errorf("page: setting data and column types from rules: %w", err)
	}
	if err := setDataAndMetadataFromRules(tec, data, metadata, rules.PageRules); err != nil {
		return nil, fmt.Errorf("page: setting data and column types from rules: %w", err)
	}

	if err := storeRudderEvent(tec, data, metadata); err != nil {
		return nil, fmt.Errorf("page: storing rudder event: %w", err)
	}

	tableName, err := safeTableNameCached(tec, "pages")
	if err != nil {
		return nil, fmt.Errorf("page: safe table name: %w", err)
	}
	columns, err := t.getColumns(tec.event.Metadata.DestinationType, data, metadata)
	if err != nil {
		return nil, fmt.Errorf("page: getting columns: %w", err)
	}

	mergeOutput, err := t.mergeEvents(tec)
	if err != nil {
		return nil, fmt.Errorf("page: merge event: %w", err)
	}

	pageOutput := map[string]any{
		"data": data,
		"metadata": map[string]any{
			"table":      tableName,
			"columns":    columns,
			"receivedAt": tec.event.Metadata.ReceivedAt,
		},
		"userId": "",
	}
	return append([]map[string]any{pageOutput}, mergeOutput...), nil
}

func (t *Transformer) screenEvents(tec *transformEventContext) ([]map[string]any, error) {
	data := make(map[string]any)
	metadata := make(map[string]string)

	if err := setDataAndMetadataFromInput(tec, tec.event.Message["properties"], data, metadata, &prefixInfo{
		completePrefix: "screen_properties_",
		completeLevel:  2,
	}); err != nil {
		return nil, fmt.Errorf("screen: setting data and column types from input: %w", err)
	}
	if err := setDataAndMetadataFromInput(tec, tec.event.Message["context"], data, metadata, &prefixInfo{
		completePrefix: "screen_context_",
		completeLevel:  2,
		prefix:         "context_",
	}); err != nil {
		return nil, fmt.Errorf("screen: setting data and column types from input: %w", err)
	}
	if err := setDataAndMetadataFromRules(tec, data, metadata, rules.DefaultRules); err != nil {
		return nil, fmt.Errorf("screen: setting data and column types from rules: %w", err)
	}
	if err := setDataAndMetadataFromRules(tec, data, metadata, rules.ScreenRules); err != nil {
		return nil, fmt.Errorf("screen: setting data and column types from rules: %w", err)
	}

	if err := storeRudderEvent(tec, data, metadata); err != nil {
		return nil, fmt.Errorf("screen: storing rudder event: %w", err)
	}

	tableName, err := safeTableNameCached(tec, "screens")
	if err != nil {
		return nil, fmt.Errorf("screen: safe table name: %w", err)
	}
	columns, err := t.getColumns(tec.event.Metadata.DestinationType, data, metadata)
	if err != nil {
		return nil, fmt.Errorf("screen: getting columns: %w", err)
	}

	mergeOutput, err := t.mergeEvents(tec)
	if err != nil {
		return nil, fmt.Errorf("screen: merge event: %w", err)
	}

	screenOutput := map[string]any{
		"data": data,
		"metadata": map[string]any{
			"table":      tableName,
			"columns":    columns,
			"receivedAt": tec.event.Metadata.ReceivedAt,
		},
		"userId": "",
	}
	return append([]map[string]any{screenOutput}, mergeOutput...), nil
}

func (t *Transformer) groupEvents(tec *transformEventContext) ([]map[string]any, error) {
	data := make(map[string]any)
	metadata := make(map[string]string)

	if err := setDataAndMetadataFromInput(tec, tec.event.Message["traits"], data, metadata, &prefixInfo{
		completePrefix: "group_traits_",
		completeLevel:  2,
	}); err != nil {
		return nil, fmt.Errorf("group: setting data and column types from input: %w", err)
	}
	if err := setDataAndMetadataFromInput(tec, tec.event.Message["context"], data, metadata, &prefixInfo{
		completePrefix: "group_context_",
		completeLevel:  2,
		prefix:         "context_",
	}); err != nil {
		return nil, fmt.Errorf("group: setting data and column types from input: %w", err)
	}
	if err := setDataAndMetadataFromRules(tec, data, metadata, rules.DefaultRules); err != nil {
		return nil, fmt.Errorf("group: setting data and column types from rules: %w", err)
	}
	if err := setDataAndMetadataFromRules(tec, data, metadata, rules.GroupRules); err != nil {
		return nil, fmt.Errorf("group: setting data and column types from rules: %w", err)
	}

	if err := storeRudderEvent(tec, data, metadata); err != nil {
		return nil, fmt.Errorf("group: storing rudder event: %w", err)
	}

	tableName, err := safeTableNameCached(tec, "groups")
	if err != nil {
		return nil, fmt.Errorf("group: safe table name: %w", err)
	}
	columns, err := t.getColumns(tec.event.Metadata.DestinationType, data, metadata)
	if err != nil {
		return nil, fmt.Errorf("group: getting columns: %w", err)
	}

	mergeOutput, err := t.mergeEvents(tec)
	if err != nil {
		return nil, fmt.Errorf("group: merge event: %w", err)
	}

	groupOutput := map[string]any{
		"data": data,
		"metadata": map[string]any{
			"table":      tableName,
			"columns":    columns,
			"receivedAt": tec.event.Metadata.ReceivedAt,
		},
		"userId": "",
	}
	return append([]map[string]any{groupOutput}, mergeOutput...), nil
}

func (t *Transformer) aliasEvents(tec *transformEventContext) ([]map[string]any, error) {
	data := make(map[string]any)
	metadata := make(map[string]string)

	if err := setDataAndMetadataFromInput(tec, tec.event.Message["traits"], data, metadata, &prefixInfo{
		completePrefix: "alias_traits_",
		completeLevel:  2,
	}); err != nil {
		return nil, fmt.Errorf("alias: setting data and column types from input: %w", err)
	}
	if err := setDataAndMetadataFromInput(tec, tec.event.Message["context"], data, metadata, &prefixInfo{
		completePrefix: "alias_context_",
		completeLevel:  2,
		prefix:         "context_",
	}); err != nil {
		return nil, fmt.Errorf("alias: setting data and column types from input: %w", err)
	}
	if err := setDataAndMetadataFromRules(tec, data, metadata, rules.DefaultRules); err != nil {
		return nil, fmt.Errorf("alias: setting data and column types from rules: %w", err)
	}
	if err := setDataAndMetadataFromRules(tec, data, metadata, rules.AliasRules); err != nil {
		return nil, fmt.Errorf("alias: setting data and column types from rules: %w", err)
	}

	if err := storeRudderEvent(tec, data, metadata); err != nil {
		return nil, fmt.Errorf("alias: storing rudder event: %w", err)
	}

	tableName, err := safeTableNameCached(tec, "aliases")
	if err != nil {
		return nil, fmt.Errorf("alias: safe table name: %w", err)
	}
	columns, err := t.getColumns(tec.event.Metadata.DestinationType, data, metadata)
	if err != nil {
		return nil, fmt.Errorf("alias: getting columns: %w", err)
	}

	mergeOutput, err := t.mergeEvents(tec)
	if err != nil {
		return nil, fmt.Errorf("merge event: %w", err)
	}

	aliasOutput := map[string]any{
		"data": data,
		"metadata": map[string]any{
			"table":      tableName,
			"columns":    columns,
			"receivedAt": tec.event.Metadata.ReceivedAt,
		},
		"userId": "",
	}
	return append([]map[string]any{aliasOutput}, mergeOutput...), nil
}
