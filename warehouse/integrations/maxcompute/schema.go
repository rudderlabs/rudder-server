package maxcompute

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"

	odpsclient "github.com/rudderlabs/rudder-server/warehouse/integrations/maxcompute/client"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var logicalTypes = map[string]string{
	"string": "STRING", "text": "STRING",
	"int": "BIGINT", "float": "DOUBLE",
	"boolean": "BOOLEAN", "datetime": "TIMESTAMP",
}

var missingSchemaListPattern = regexp.MustCompile(`(?i)\bschema\s+\S+\s+does not exist\b`)

func missingSchemaListError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "ODPS-0420095") && missingSchemaListPattern.MatchString(err.Error())
}

func columnType(logical string) (string, error) {
	if t, ok := logicalTypes[strings.ToLower(logical)]; ok {
		return t, nil
	}
	return "", fmt.Errorf("unsupported MaxCompute logical type %q", logical)
}

// sdkType only maps types understood by the Rudder schema engine; unknown
// catalog types are excluded rather than falsely reported as strings.
func sdkType(t datatype.DataType) (string, bool) {
	if t == nil {
		return "", false
	}
	switch strings.ToUpper(t.Name()) {
	case "TINYINT", "SMALLINT", "INT", "BIGINT":
		return "int", true
	case "FLOAT", "DOUBLE", "DECIMAL":
		return "float", true
	case "CHAR", "VARCHAR", "STRING":
		return "string", true
	case "BOOLEAN":
		return "boolean", true
	case "DATE", "DATETIME", "TIMESTAMP":
		return "datetime", true
	}
	return "", false
}

func (m *MaxCompute) FetchSchema(ctx context.Context) (model.Schema, error) {
	if m.catalog != nil {
		return m.catalog(ctx)
	}
	if m.odps == nil {
		return nil, fmt.Errorf("MaxCompute client is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	schema := make(model.Schema)
	var listErr error
	// Tables.List paginates through the SDK catalog. The list callback does
	// not support early termination, so check context before each table load.
	odps.NewTables(m.odps, m.odps.DefaultProjectName(), m.namespace()).List(func(t *odps.Table, err error) {
		if listErr != nil {
			return
		}
		if err != nil {
			listErr = odpsclient.SafeError(err)
			return
		}
		if err = ctx.Err(); err != nil {
			listErr = err
			return
		}
		if t == nil || strings.HasPrefix(t.Name(), warehouseutils.StagingTablePrefix(provider)) {
			return
		}
		if err = t.Load(); err != nil {
			listErr = fmt.Errorf("loading MaxCompute table %q: %w", t.Name(), odpsclient.SafeError(err))
			return
		}
		if t.Owner() == "" {
			listErr = fmt.Errorf("MaxCompute table owner unavailable")
			return
		}
		if t.Type() == odps.VirtualView || t.Type() == odps.MaterializedView {
			return
		}
		columns := make(model.TableSchema)
		for _, c := range t.Schema().Columns {
			if strings.EqualFold(c.Name, "pt") {
				continue
			}
			if logical, ok := sdkType(c.Type); ok {
				columns[c.Name] = logical
			}
		}
		for _, c := range t.Schema().PartitionColumns {
			if strings.EqualFold(c.Name, "pt") {
				continue
			}
			if logical, ok := sdkType(c.Type); ok {
				columns[c.Name] = logical
			}
		}
		if len(columns) != 0 {
			schema[t.Name()] = columns
		}
	})
	if listErr != nil {
		if missingSchemaListError(listErr) {
			return make(model.Schema), nil
		}
		return nil, fmt.Errorf("listing MaxCompute tables: %w", listErr)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return schema, nil
}
