package snowpipestreaming

import (
	"strings"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const jsonDataType = "json"

// prepareValuesHP prepares an event's values for the high-performance architecture, after discards were detected.
//
// The classic SDK re-parses JSON strings, the high-performance architecture doesn't: a JSON string lands as text in a
// VARIANT column, or wrapped in an ARRAY column (verified). So for json columns, arrays and objects are sent as native
// JSON, and strings holding a JSON array or object are parsed. For every other column, arrays and objects are sent
// as their JSON text, as the classic path does.
func prepareValuesHP(e *event, schema whutils.ModelTableSchema) {
	for column, value := range e.Message.Data {
		switch v := value.(type) {
		case []any, map[string]any:
			if schema[column] == jsonDataType {
				continue
			}
			marshalled, err := jsonrs.Marshal(v)
			if err != nil {
				e.Message.Data[column] = nil
				continue
			}
			e.Message.Data[column] = string(marshalled)
		case string:
			if schema[column] != jsonDataType {
				continue
			}
			if parsed, ok := parseJSONContainer(v); ok {
				e.Message.Data[column] = parsed
			}
		}
	}
}

// parseJSONContainer parses s if it holds a JSON array or object. Scalars stay strings: "123" is a string value.
func parseJSONContainer(s string) (any, bool) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" || (trimmed[0] != '{' && trimmed[0] != '[') {
		return nil, false
	}
	var parsed any
	if err := jsonrs.Unmarshal([]byte(trimmed), &parsed); err != nil {
		return nil, false
	}
	return parsed, true
}

// discardRowsHP converts discards to rows. Fields are matched to columns by name, so they are provider-cased.
func discardRowsHP(discardInfos []discardInfo) []model.Row {
	rows := convertDiscardedInfosToRows(discardInfos)
	for i, row := range rows {
		cased := make(model.Row, len(row))
		for k, v := range row {
			cased[whutils.ToProviderCase(whutils.SnowpipeStreaming, k)] = v
		}
		rows[i] = cased
	}
	return rows
}
