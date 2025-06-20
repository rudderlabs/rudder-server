package snowflake

import (
	"fmt"
	"strings"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type tableManager interface {
	createTableQuery(schemaIdentifier, tableName string, columns model.TableSchema) string
	addColumnsQuery(schemaIdentifier, tableName string, columnsInfo []whutils.ColumnInfo) (string, error)
}

func newTableManager(config *config.Config, warehouse model.Warehouse) tableManager {
	if warehouse.GetBoolDestinationConfig(model.EnableIcebergSetting) {
		externalVolume := warehouse.GetStringDestinationConfig(config, model.ExternalVolumeSetting)
		return newIcebergTableManager(externalVolume)
	}
	return newStandardTableManager()
}

// for standard Snowflake tables
type standardTableManager struct {
	dataTypesMap map[string]string
}

func newStandardTableManager() tableManager {
	return &standardTableManager{
		dataTypesMap: map[string]string{
			"boolean":  "boolean",
			"int":      "number",
			"bigint":   "number",
			"float":    "double precision",
			"string":   "varchar",
			"datetime": "timestamp_tz",
			"json":     "variant",
		},
	}
}

func (m *standardTableManager) createTableQuery(schemaIdentifier, tableName string, columns model.TableSchema) string {
	return fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s.%q ( %v )`,
		schemaIdentifier, tableName, columnsWithDataTypes(columns, m.dataTypesMap),
	)
}

func (m *standardTableManager) addColumnsQuery(schemaIdentifier, tableName string, columnsInfo []whutils.ColumnInfo) (string, error) {
	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf(`
		ALTER TABLE
		  %s.%q
		ADD COLUMN`,
		schemaIdentifier,
		tableName,
	))

	for _, columnInfo := range columnsInfo {
		queryBuilder.WriteString(fmt.Sprintf(` IF NOT EXISTS %q %s,`, columnInfo.Name, m.dataTypesMap[columnInfo.Type]))
	}
	return strings.TrimSuffix(queryBuilder.String(), ",") + ";", nil
}

// for snowflake managed Iceberg tables
type icebergTableManager struct {
	dataTypesMap   map[string]string
	externalVolume string
}

func newIcebergTableManager(externalVolume string) tableManager {
	return &icebergTableManager{
		dataTypesMap: map[string]string{
			"boolean":  "boolean",
			"int":      "number(10,0)",
			"bigint":   "number(19,0)",
			"float":    "double precision",
			"string":   "varchar",
			"datetime": "timestamp_ntz(6)",
			"json":     "varchar",
		},
		externalVolume: externalVolume,
	}
}

func (m *icebergTableManager) createTableQuery(schemaIdentifier, tableName string, columns model.TableSchema) string {
	baseLocation := fmt.Sprintf("%s/%s", schemaIdentifier, tableName)
	return fmt.Sprintf(
		`CREATE OR REPLACE ICEBERG TABLE %s.%q ( %v )
		CATALOG = 'SNOWFLAKE'
		EXTERNAL_VOLUME = '%s'
		BASE_LOCATION = '%s'`,
		schemaIdentifier, tableName, columnsWithDataTypes(columns, m.dataTypesMap),
		m.externalVolume,
		baseLocation,
	)
}

func (m *icebergTableManager) addColumnsQuery(schemaIdentifier, tableName string, columnsInfo []whutils.ColumnInfo) (string, error) {
	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf(`
		ALTER ICEBERG TABLE
		  %s.%q
		ADD COLUMN`,
		schemaIdentifier,
		tableName,
	))
	for _, columnInfo := range columnsInfo {
		dataType, ok := m.dataTypesMap[columnInfo.Type]
		if !ok {
			return "", fmt.Errorf("invalid data type: %s", columnInfo.Type)
		}
		queryBuilder.WriteString(fmt.Sprintf(` IF NOT EXISTS %q %s,`, columnInfo.Name, dataType))
	}
	return strings.TrimSuffix(queryBuilder.String(), ",") + ";", nil
}

func columnsWithDataTypes(columns model.TableSchema, dataTypesMap map[string]string) string {
	var arr []string
	sortedColumns := getSortedColumnsFromTableSchema(columns)
	for _, name := range sortedColumns {
		arr = append(arr, fmt.Sprintf(`"%s" %s`, name, dataTypesMap[columns[name]]))
	}
	return strings.Join(arr, ",")
}
