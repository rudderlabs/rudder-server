package transformer

import (
	"errors"
	"fmt"

	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/datatype"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/rules"
)

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

	d, ok := pi.event.Message[eventColName]
	if !ok {
		return nil, errors.New("extract: cannot create event table with empty event name, event name is missing in the payload")
	}
	eventName, ok := d.(string)
	if !ok || len(eventName) == 0 {
		return nil, errors.New("extract: cannot create event table with empty event name, event name is not a string")
	}

	event[eventColName] = TransformTableName(pi.event.Metadata.DestinationType, pi.itrOpts, pi.dstOpts, eventName)
	columnTypes[eventColName] = datatype.TypeString

	if err = t.setDataAndColumnTypeFromRules(pi, event, columnTypes,
		rules.ExtractRules, rules.ExtractFunctionalRules,
	); err != nil {
		return nil, fmt.Errorf("extract: setting data and column types from rules: %w", err)
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
