package marshal

import (
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
)

// MarshalArrow accepts a slice of rows with which it creates a table object.
// We need to append row by row as opposed to the arrow go provided way of
// column by column since the wrapper ParquetWriter uses the number of rows
// to execute intermediate flush depending on the size of the objects,
// determined by row, which are currently written.
func MarshalArrow(records []interface{}, schemaHandler *schema.SchemaHandler) (*map[string]*layout.Table, error) {
	res := make(map[string]*layout.Table)
	if ln := len(records); ln <= 0 {
		return &res, nil
	}
	for i := 0; i < len(records[0].([]interface{})); i++ {
		pathStr := schemaHandler.GetRootInName() + common.PAR_GO_PATH_DELIMITER + schemaHandler.Infos[i+1].InName
		table := layout.NewEmptyTable()
		res[pathStr] = table
		table.Path = common.StrToPath(pathStr)

		schema := schemaHandler.SchemaElements[schemaHandler.MapIndex[pathStr]]
		isOptional := true
		if *schema.RepetitionType != parquet.FieldRepetitionType_OPTIONAL {
			isOptional = false
		}

		if isOptional {
			table.MaxDefinitionLevel = 1
		} else {
			table.MaxDefinitionLevel = 0
		}
		table.MaxRepetitionLevel = 0
		table.Schema = schemaHandler.SchemaElements[schemaHandler.MapIndex[pathStr]]
		table.RepetitionType = *table.Schema.RepetitionType
		table.Info = schemaHandler.Infos[i+1]
		// Pre-allocate these arrays for efficiency.
		table.Values = make([]interface{}, 0, len(records))
		table.RepetitionLevels = make([]int32, 0, len(records))
		table.DefinitionLevels = make([]int32, 0, len(records))

		for j := 0; j < len(records); j++ {
			rec := records[j].([]interface{})[i]
			table.Values = append(table.Values, rec)

			table.RepetitionLevels = append(table.RepetitionLevels, 0)
			if rec == nil || !isOptional {
				table.DefinitionLevels = append(table.DefinitionLevels, 0)
			} else {
				table.DefinitionLevels = append(table.DefinitionLevels, 1)
			}
		}
	}
	return &res, nil
}
