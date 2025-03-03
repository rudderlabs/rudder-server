package snowflake

import (
	"testing"

	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/stretchr/testify/require"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestTableManager(t *testing.T) {
	columns := model.TableSchema{
		"col1": "boolean",
		"col2": "int",
		"col3": "bigint",
		"col4": "float",
		"col5": "string",
		"col6": "datetime",
		"col7": "json",
	}
	standardWarehouse := model.Warehouse{
		Destination: backendconfig.DestinationT{
			Config: map[string]interface{}{
				"enableIceberg": false,
			},
		},
	}
	icebergWarehouse := model.Warehouse{
		Destination: backendconfig.DestinationT{
			Config: map[string]interface{}{
				"enableIceberg":  true,
				"externalVolume": "myvolume",
			},
		},
	}

	t.Run("TestCreateTableQuery", func(t *testing.T) {
		tests := []struct {
			name                     string
			warehouse                model.Warehouse
			columns                  model.TableSchema
			expectedCreateTableQuery string
		}{
			{
				name:                     "standard table",
				warehouse:                standardWarehouse,
				columns:                  columns,
				expectedCreateTableQuery: "CREATE TABLE IF NOT EXISTS myschema.\"mytable\" ( \"col1\" boolean,\"col2\" number,\"col3\" number,\"col4\" double precision,\"col5\" varchar,\"col6\" timestamp_tz,\"col7\" variant )",
			},
			{
				name:      "iceberg table",
				warehouse: icebergWarehouse,
				columns:   columns,
				expectedCreateTableQuery: `CREATE OR REPLACE ICEBERG TABLE myschema."mytable" ( "col1" boolean,"col2" number(10,0),"col3" number(19,0),"col4" double precision,"col5" varchar,"col6" timestamp_ltz(6),"col7" varchar )
		CATALOG = 'SNOWFLAKE'
		EXTERNAL_VOLUME = 'myvolume'
		BASE_LOCATION = 'myschema/mytable'`,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				manager := newTableManager(config.New(), tt.warehouse)
				result := manager.createTableQuery("myschema", "mytable", tt.columns)
				require.Equal(t, tt.expectedCreateTableQuery, result)
			})
		}
	})

	t.Run("TestAddColumnsQuery", func(t *testing.T) {
		tests := []struct {
			name                    string
			warehouse               model.Warehouse
			additionalColumns       []whutils.ColumnInfo
			expectedAddColumnsQuery string
			expectedError           string
		}{
			{
				name:      "standard table manager",
				warehouse: standardWarehouse,
				additionalColumns: []whutils.ColumnInfo{
					{Name: "new_col1", Type: "string"},
					{Name: "new_col2", Type: "int"},
				},
				expectedAddColumnsQuery: `
		ALTER TABLE
		  myschema."mytable"
		ADD COLUMN "new_col1" varchar, "new_col2" number;`,
			},
			{
				name:      "iceberg table",
				warehouse: icebergWarehouse,
				additionalColumns: []whutils.ColumnInfo{
					{Name: "new_col1", Type: "string"},
					{Name: "new_col2", Type: "int"},
				},
				expectedAddColumnsQuery: `
		ALTER ICEBERG TABLE
		  myschema."mytable"
		ADD COLUMN "new_col1" varchar, "new_col2" number(10,0);`,
			},
			{
				name:      "iceberg table - invalid data type",
				warehouse: icebergWarehouse,
				additionalColumns: []whutils.ColumnInfo{
					{Name: "col1", Type: "hello"},
				},
				expectedError: "invalid data type: hello",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				manager := newTableManager(config.New(), tt.warehouse)
				result, err := manager.addColumnsQuery("myschema", "mytable", tt.additionalColumns)

				if tt.expectedError != "" {
					require.EqualError(t, err, tt.expectedError)
				} else {
					require.NoError(t, err)
					require.Equal(t, tt.expectedAddColumnsQuery, result)
				}
			})
		}
	})
}
