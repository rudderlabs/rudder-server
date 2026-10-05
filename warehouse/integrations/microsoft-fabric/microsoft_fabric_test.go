package microsoftfabric

import (
	"context"
	"database/sql"
	"net/url"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/microsoft/go-mssqldb/azuread"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type uploaderStub struct {
	warehouseutils.Uploader
	canAppend       bool
	useNewRecord    bool
	schemasInUpload map[string]model.TableSchema
	schemasInWH     map[string]model.TableSchema
	loadFiles       map[string][]warehouseutils.LoadFile
}

func (u *uploaderStub) CanAppend() bool { return u.canAppend }
func (u *uploaderStub) ShouldOnDedupUseNewRecord() bool {
	return u.useNewRecord
}

func (u *uploaderStub) GetTableSchemaInUpload(table string) model.TableSchema {
	return u.schemasInUpload[table]
}

func (u *uploaderStub) GetTableSchemaInWarehouse(table string) model.TableSchema {
	return u.schemasInWH[table]
}

func (u *uploaderStub) GetLoadFilesMetadata(_ context.Context, options warehouseutils.GetLoadFilesOptions) ([]warehouseutils.LoadFile, error) {
	return u.loadFiles[options.Table], nil
}

func testWarehouse(preferAppend bool) model.Warehouse {
	return model.Warehouse{
		Type:      warehouseutils.MicrosoftFabric,
		Namespace: "schema.with.dot",
		Destination: backendconfig.DestinationT{
			Config: map[string]any{
				"host":              "configured.fabric.example",
				"database":          "warehouse",
				"tenantId":          "tenant",
				"clientId":          "client",
				"clientSecret":      "secret",
				"fabricWorkspaceId": "11111111-1111-1111-1111-111111111111",
				"lakehouseId":       "22222222-2222-2222-2222-222222222222",
				"oneLakeHost":       "onelake.dfs.fabric.microsoft.com",
				"preferAppend":      preferAppend,
			},
		},
	}
}

func newSQLMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db, mock
}

func TestPersistedTypeMappings(t *testing.T) {
	require.Equal(t, map[string]string{
		"boolean":  "bit",
		"int":      "bigint",
		"float":    "float",
		"datetime": "datetime2(6)",
		"string":   "varchar(max)",
		"text":     "varchar(max)",
		"json":     "varchar(max)",
	}, dataTypesMap)

	columns, err := columnsWithDataTypes(model.TableSchema{
		"payload": "json", "description": "text", "at": "datetime", "name": "string", "enabled": "boolean", "count": "int", "amount": "float",
	})
	require.NoError(t, err)
	require.Equal(t, "[amount] float NULL,[at] datetime2(6) NULL,[count] bigint NULL,[description] varchar(max) NULL,[enabled] bit NULL,[name] varchar(max) NULL,[payload] varchar(max) NULL", columns)
	_, err = columnsWithDataTypes(model.TableSchema{"bad": "array(string)"})
	require.ErrorContains(t, err, "schema_evolution")
}

func TestAddColumnsMapsTextToVarcharMax(t *testing.T) {
	db, mock := newSQLMock(t)
	mock.ExpectExec(`IF COL_LENGTH\(N'schema\.with\.dot\.tracks', N'description'\) IS NULL ALTER TABLE \[schema\.with\.dot\]\.\[tracks\] ADD \[description\] varchar\(max\) NULL;`).WillReturnResult(sqlmock.NewResult(0, 0))
	fabric := &MicrosoftFabric{db: sqlmw.New(db), namespace: "schema.with.dot"}
	require.NoError(t, fabric.AddColumns(context.Background(), "tracks", []warehouseutils.ColumnInfo{{Name: "description", Type: model.TextDataType}}))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestConnectionDSNUsesCompleteHostFixedPortAndEntraSPN(t *testing.T) {
	fabric := &MicrosoftFabric{warehouse: testWarehouse(false), connectTimeout: 3 * time.Second, conf: config.New()}
	dsn := fabric.connectionDSN()
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	require.Equal(t, "configured.fabric.example:1433", u.Host)
	require.Equal(t, "client@tenant", u.User.Username())
	password, ok := u.User.Password()
	require.True(t, ok)
	require.Equal(t, "secret", password)
	require.Equal(t, azuread.ActiveDirectoryServicePrincipal, u.Query().Get("fedauth"))
	require.Equal(t, "warehouse", u.Query().Get("database"))
	require.Equal(t, "true", u.Query().Get("encrypt"))
	require.NotContains(t, u.Host, ".database.windows.net")
}

func TestConnectBootstrapsBeforeOpeningSQL(t *testing.T) {
	bootstrap := newBootstrapper(nil)
	bootstrap.newCredential = func(_, _, _ string) (azcore.TokenCredential, error) {
		return nil, context.Canceled
	}
	fabric := New(config.New(), logger.NOP, stats.NOP)
	fabric.bootstrapper = bootstrap

	_, err := fabric.Connect(context.Background(), testWarehouse(false))
	require.ErrorContains(t, err, "spn_token_bootstrap")
	require.ErrorIs(t, err, context.Canceled)
}

func TestShouldMerge(t *testing.T) {
	for _, tc := range []struct {
		name         string
		table        string
		preferAppend bool
		canAppend    bool
		want         bool
	}{
		{name: "default merge", table: "tracks", want: true},
		{name: "preference only is insufficient", table: "tracks", preferAppend: true, want: true},
		{name: "direct append", table: "tracks", preferAppend: true, canAppend: true, want: false},
		{name: "users always merge", table: warehouseutils.UsersTable, preferAppend: true, canAppend: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fabric := &MicrosoftFabric{warehouse: testWarehouse(tc.preferAppend), uploader: &uploaderStub{canAppend: tc.canAppend}}
			require.Equal(t, tc.want, fabric.ShouldMerge(tc.table))
		})
	}
}

func TestLoadTableAppendUsesStagingBeforeTargetInsert(t *testing.T) {
	db, mock := newSQLMock(t)
	location := "https://onelake.dfs.fabric.microsoft.com/11111111-1111-1111-1111-111111111111/22222222-2222-2222-2222-222222222222/Files/load/tracks.parquet"
	mock.ExpectExec(`SELECT TOP 0 \* INTO \[schema\.with\.dot\]\.\[rudder_staging_tracks_[0-9a-f]+\] FROM \[schema\.with\.dot\]\.\[tracks\];`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`COPY INTO \[schema\.with\.dot\]\.\[rudder_staging_tracks_[0-9a-f]+\] \(\[id\],\[received_at\]\) FROM 'https://onelake\.dfs\.fabric\.microsoft\.com/.+' WITH \(FILE_TYPE = 'PARQUET'\);`).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(`INSERT INTO \[schema\.with\.dot\]\.\[tracks\] \(\[id\],\[received_at\]\) SELECT \[id\],\[received_at\] FROM \[schema\.with\.dot\]\.\[rudder_staging_tracks_[0-9a-f]+\];`).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(`IF OBJECT_ID\(N'\[schema\.with\.dot\]\.\[rudder_staging_tracks_[0-9a-f]+\]', 'U'\) IS NOT NULL DROP TABLE \[schema\.with\.dot\]\.\[rudder_staging_tracks_[0-9a-f]+\];`).WillReturnResult(sqlmock.NewResult(0, 0))
	fabric := &MicrosoftFabric{
		db:        sqlmw.New(db),
		conf:      config.New(),
		namespace: "schema.with.dot",
		warehouse: testWarehouse(true),
		uploader: &uploaderStub{
			canAppend:       true,
			schemasInUpload: map[string]model.TableSchema{"tracks": {"id": "string", "received_at": "datetime"}},
			loadFiles:       map[string][]warehouseutils.LoadFile{"tracks": {{Location: location}}},
		},
	}
	_, err := fabric.LoadTable(context.Background(), "tracks")
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadTableUsesUniqueStagingAndSingleMerge(t *testing.T) {
	db, mock := newSQLMock(t)
	location := "https://onelake.dfs.fabric.microsoft.com/11111111-1111-1111-1111-111111111111/22222222-2222-2222-2222-222222222222/Files/load/tracks.parquet"
	mock.ExpectExec(`SELECT TOP 0 \* INTO \[schema\.with\.dot\]\.\[rudder_staging_tracks_[0-9a-f]+\] FROM \[schema\.with\.dot\]\.\[tracks\];`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`COPY INTO \[schema\.with\.dot\]\.\[rudder_staging_tracks_[0-9a-f]+\] \(\[id\],\[received_at\],\[value\]\) FROM 'https://onelake\.dfs\.fabric\.microsoft\.com/.+' WITH \(FILE_TYPE = 'PARQUET'\);`).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(`(?s)MERGE INTO \[schema\.with\.dot\]\.\[tracks\] AS target USING .*WHEN MATCHED THEN UPDATE SET.*WHEN NOT MATCHED THEN INSERT`).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(`IF OBJECT_ID\(N'\[schema\.with\.dot\]\.\[rudder_staging_tracks_[0-9a-f]+\]', 'U'\) IS NOT NULL DROP TABLE \[schema\.with\.dot\]\.\[rudder_staging_tracks_[0-9a-f]+\];`).WillReturnResult(sqlmock.NewResult(0, 0))
	fabric := &MicrosoftFabric{
		db:        sqlmw.New(db),
		conf:      config.New(),
		namespace: "schema.with.dot",
		warehouse: testWarehouse(false),
		uploader: &uploaderStub{
			canAppend:    true,
			useNewRecord: true,
			schemasInUpload: map[string]model.TableSchema{"tracks": {
				"id": "string", "received_at": "datetime", "value": "string",
			}},
			loadFiles: map[string][]warehouseutils.LoadFile{"tracks": {{Location: location}}},
		},
	}
	_, err := fabric.LoadTable(context.Background(), "tracks")
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCopyRejectsLocationOutsideConfiguredLakehouse(t *testing.T) {
	fabric := &MicrosoftFabric{warehouse: testWarehouse(false), conf: config.New()}
	err := fabric.validateOneLakeLocation("https://attacker.example/file.parquet")
	require.ErrorContains(t, err, "outside the configured OneLake Lakehouse")
	err = fabric.validateOneLakeLocation("https://onelake.dfs.fabric.microsoft.com/11111111-1111-1111-1111-111111111111/22222222-2222-2222-2222-222222222222/Files/f.parquet")
	require.NoError(t, err)
	err = fabric.validateOneLakeLocation("https://onelake.dfs.fabric.microsoft.com/11111111-1111-1111-1111-111111111111/22222222-2222-2222-2222-222222222222.Lakehouse/Files/f.parquet")
	require.ErrorContains(t, err, "outside the configured OneLake Lakehouse")
}

func TestAlterColumnIsExplicitlyBlocked(t *testing.T) {
	_, err := (&MicrosoftFabric{}).AlterColumn(context.Background(), "tracks", "value", "string")
	require.ErrorContains(t, err, "automatic ALTER COLUMN is unsupported")
}

func TestMergeStatementDiscardsCompositeMatch(t *testing.T) {
	statement := mergeStatement("schema", warehouseutils.DiscardsTable, "staging", []string{"row_id", "table_name", "column_name", "received_at"}, true)
	require.Contains(t, statement, "PARTITION BY [row_id], [column_name], [table_name]")
	require.Contains(t, statement, "target.[table_name] = source.[table_name]")
	require.Contains(t, statement, "target.[column_name] = source.[column_name]")
}

func TestErrorMappingsCoverFabricPrefixes(t *testing.T) {
	fabric := &MicrosoftFabric{}
	for _, tc := range []struct {
		name string
		err  string
		want model.JobErrorType
	}{
		{name: "bootstrap", err: "spn_token_bootstrap: unauthorized", want: model.PermissionError},
		{name: "lakehouse access", err: "lakehouse_access: upload failed with HTTP 403", want: model.PermissionError},
		{name: "lakehouse missing", err: "lakehouse_not_found: listing files failed with HTTP 404", want: model.ResourceNotFoundError},
		{name: "copy", err: "copy_into: loading Parquet into tracks", want: model.ResourceNotFoundError},
		{name: "schema", err: "schema_evolution: unsupported Rudder type", want: model.AlterColumnError},
		{name: "merge", err: "merge: loading tracks from staging", want: model.ConcurrentQueriesError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got model.JobErrorType
			for _, mapping := range fabric.ErrorMappings() {
				if mapping.Format.MatchString(tc.err) {
					got = mapping.Type
					break
				}
			}
			require.Equal(t, tc.want, got)
		})
	}
}
