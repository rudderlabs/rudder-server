package transformer

import (
	"fmt"
	"strings"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/rules"
)

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
	if err := t.setDataAndColumnTypeFromRules(pi, commonProps, commonColumnTypes,
		lo.Assign(rules.TrackRules, rules.DefaultRules), rules.DefaultFunctionalRules,
	); err != nil {
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

	if err := t.setDataAndColumnTypeFromRules(pi, event, columnTypes,
		nil, rules.TrackTableFunctionalRules,
	); err != nil {
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
	if err := t.setDataAndColumnTypeFromRules(pi, commonProps, commonColumnTypes,
		nil, rules.TrackEventTableFunctionalRules,
	); err != nil {
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
