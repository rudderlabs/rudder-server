package transformer

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/response"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func (t *transformer) getColumns(destType string, data map[string]any, columnTypes map[string]string) (map[string]any, error) {
	columns := make(map[string]any)

	// uuid_ts and loaded_at datatypes are passed from here to create appropriate columns.
	// Corresponding values are inserted when loading into the warehouse
	uuidTS := "uuid_ts"
	if destType == whutils.SNOWFLAKE {
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
