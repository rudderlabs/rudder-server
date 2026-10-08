package microsoftfabric_test

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseclient "github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	microsoftfabric "github.com/rudderlabs/rudder-server/warehouse/integrations/microsoft-fabric"
	whth "github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/router"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

const fabricTestCredentialsKey = "MICROSOFT_FABRIC_INTEGRATION_TEST_CREDENTIALS"

type fabricTestCredentials struct {
	Host              string `json:"host"`
	Database          string `json:"database"`
	FabricWorkspaceID string `json:"fabricWorkspaceId"`
	LakehouseID       string `json:"lakehouseId"`
	TenantID          string `json:"tenantId"`
	ClientID          string `json:"clientId"`
	ClientSecret      string `json:"clientSecret"`
}

type eventRecordMode int

const (
	recordsNormal eventRecordMode = iota
	recordsMerge
	recordsAppend
)

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}

	credentialsJSON, exists := os.LookupEnv(fabricTestCredentialsKey)
	if !exists {
		if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
			t.Fatalf("%s environment variable not set", fabricTestCredentialsKey)
		}
		t.Skipf("Skipping %s as %s is not set", t.Name(), fabricTestCredentialsKey)
	}

	var credentials fabricTestCredentials
	require.NoError(t, jsonrs.Unmarshal([]byte(credentialsJSON), &credentials))

	misc.Init()
	validations.Init()
	whutils.Init()

	t.Run("Events flow", func(t *testing.T) {
		testFabricEventsFlow(t, credentials)
	})
	t.Run("Validation", func(t *testing.T) {
		testFabricValidation(t, credentials)
	})
	t.Run("Load Table", func(t *testing.T) {
		testFabricLoadTable(t, credentials)
	})
}

func testFabricEventsFlow(t *testing.T, credentials fabricTestCredentials) {
	testCases := []struct {
		name            string
		preferAppend    bool
		storeFullEvent  bool
		eventsFile2     string
		mode            eventRecordMode
		secondCounts    whth.EventsCountMap
		freshSecondUser bool
	}{
		{
			name:        "merge",
			eventsFile2: "../testdata/upload-job.events-1.json",
			mode:        recordsMerge,
		},
		{
			name:         "append",
			preferAppend: true,
			eventsFile2:  "../testdata/upload-job.events-1.json",
			mode:         recordsAppend,
			secondCounts: whth.EventsCountMap{
				"identifies": 8, "users": 1, "tracks": 8, "product_track": 8,
				"pages": 8, "screens": 8, "aliases": 8, "groups": 8,
			},
		},
		{
			name:            "store full event",
			storeFullEvent:  true,
			eventsFile2:     "../testdata/upload-job.events-2.json",
			mode:            recordsNormal,
			freshSecondUser: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			httpPort, err := kithelper.GetFreePort()
			require.NoError(t, err)

			c := testcompose.New(t, compose.FilePaths([]string{"../testdata/docker-compose.jobsdb.yml", "../testdata/docker-compose.transformer.yml"}))
			c.Start(context.Background())

			workspaceID := whutils.RandHex()
			sourceID := whutils.RandHex()
			destinationID := whutils.RandHex()
			writeKey := whutils.RandHex()
			namespace := whth.RandSchema(whutils.MicrosoftFabric)
			configMap := fabricDestinationConfig(credentials, namespace)
			configMap[model.PreferAppendSetting.String()] = tc.preferAppend
			configMap["storeFullEvent"] = tc.storeFullEvent
			oneLake, err := filemanager.New(&filemanager.Settings{
				Provider: whutils.OneLake,
				Config:   configMap,
				Conf:     config.Default,
			})
			require.NoError(t, err)
			t.Cleanup(func() { cleanupFabricObjectsForSource(t, oneLake, sourceID) })

			destinationBuilder := backendconfigtest.NewDestinationBuilder(whutils.MicrosoftFabric).
				WithID(destinationID).
				WithRevisionID(destinationID)
			for key, value := range configMap {
				destinationBuilder.WithConfigOption(key, value)
			}
			destination := destinationBuilder.Build()
			workspaceConfig := backendconfigtest.NewConfigBuilder().
				WithSource(
					backendconfigtest.NewSourceBuilder().
						WithID(sourceID).
						WithWriteKey(writeKey).
						WithWorkspaceID(workspaceID).
						WithConnection(destination).
						Build(),
				).
				WithWorkspaceID(workspaceID).
				Build()

			t.Setenv("RSERVER_WAREHOUSE_MICROSOFT_FABRIC_MAX_PARALLEL_LOADS", "8")
			t.Setenv("RSERVER_WAREHOUSE_MICROSOFT_FABRIC_SLOW_QUERY_THRESHOLD", "0s")
			jobsDBPort := c.Port("jobsDb", 5432)
			whth.BootstrapSvc(t, workspaceConfig, httpPort, jobsDBPort)

			db := openFabricDB(t, credentials)
			t.Cleanup(func() { dropFabricSchema(t, db, namespace) })
			client := &warehouseclient.Client{SQL: db, Type: warehouseclient.SQLClient}
			jobsDB := whth.JobsDB(t, jobsDBPort)
			transformerURL := fmt.Sprintf("http://localhost:%d", c.Port("transformer", 9090))
			userID := whth.GetUserId(whutils.MicrosoftFabric)

			first := whth.TestConfig{
				WriteKey:        writeKey,
				Schema:          namespace,
				Tables:          fabricEventTables(),
				SourceID:        sourceID,
				DestinationID:   destinationID,
				Config:          configMap,
				WorkspaceID:     workspaceID,
				DestinationType: whutils.MicrosoftFabric,
				JobsDB:          jobsDB,
				HTTPPort:        httpPort,
				Client:          client,
				EventsFilePath:  "../testdata/upload-job.events-1.json",
				UserID:          userID,
				TransformerURL:  transformerURL,
				Destination:     destination,
			}
			first.VerifyEvents(t)

			second := first
			second.EventsFilePath = tc.eventsFile2
			second.WarehouseEventsMap = tc.secondCounts
			if tc.freshSecondUser {
				second.UserID = whth.GetUserId(whutils.MicrosoftFabric)
			}
			second.VerifyEvents(t)

			verifyFabricEventSchema(t, db, namespace, tc.storeFullEvent)
			verifyFabricEventRecords(t, db, sourceID, destinationID, namespace, tc.mode)
		})
	}
}

func testFabricValidation(t *testing.T, credentials fabricTestCredentials) {
	namespace := whth.RandSchema(whutils.MicrosoftFabric)
	db := openFabricDB(t, credentials)
	t.Cleanup(func() { dropFabricSchema(t, db, namespace) })
	destination := backendconfig.DestinationT{
		ID:     "test_destination_id",
		Config: fabricDestinationConfig(credentials, namespace),
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name:        whutils.MicrosoftFabric,
			DisplayName: "Microsoft Fabric",
		},
		Name:       "microsoft-fabric-integration-test",
		Enabled:    true,
		RevisionID: "test_destination_id",
	}
	whth.VerifyConfigurationTest(t, destination)
}

func testFabricLoadTable(t *testing.T, credentials fabricTestCredentials) {
	ctx := context.Background()
	namespace := whth.RandSchema(whutils.MicrosoftFabric)
	warehouse := fabricWarehouse(credentials, namespace)
	db := openFabricDB(t, credentials)
	t.Cleanup(func() { dropFabricSchema(t, db, namespace) })

	fm, err := filemanager.New(&filemanager.Settings{
		Provider: whutils.OneLake,
		Config:   warehouse.Destination.Config,
		Conf:     config.Default,
	})
	require.NoError(t, err)

	baseSchema := model.TableSchema{
		"test_bool":     model.BooleanDataType,
		"test_datetime": model.DateTimeDataType,
		"test_float":    model.FloatDataType,
		"test_int":      model.IntDataType,
		"test_string":   model.StringDataType,
		"id":            model.StringDataType,
		"received_at":   model.DateTimeDataType,
	}

	t.Run("schema does not exist", func(t *testing.T) {
		tableName := "schema_not_exists_test_table"
		location := uploadFabricFixture(t, fm, "../testdata/load.parquet", tableName)
		fabric := setupFabric(t, warehouse, newFabricUploader(t, []whutils.LoadFile{{Location: location}}, tableName, baseSchema, false, true))
		stats, err := fabric.LoadTable(ctx, tableName)
		require.Error(t, err)
		require.Nil(t, stats)
	})

	admin := setupFabric(t, warehouse, nil)
	require.NoError(t, admin.CreateSchema(ctx))

	t.Run("add columns is idempotent", func(t *testing.T) {
		tableName := "add_columns_idempotent_test_table"
		column := whutils.ColumnInfo{Name: "new_column", Type: model.StringDataType}
		require.NoError(t, admin.CreateTable(ctx, tableName, model.TableSchema{"id": model.StringDataType}))
		require.NoError(t, admin.AddColumns(ctx, tableName, []whutils.ColumnInfo{column}))
		require.NoError(t, admin.AddColumns(ctx, tableName, []whutils.ColumnInfo{column}))

		var columnCount int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table AND COLUMN_NAME = @column;`,
			sql.Named("schema", namespace), sql.Named("table", tableName), sql.Named("column", column.Name)).Scan(&columnCount))
		require.Equal(t, 1, columnCount)
	})

	t.Run("table does not exist", func(t *testing.T) {
		tableName := "table_not_exists_test_table"
		location := uploadFabricFixture(t, fm, "../testdata/load.parquet", tableName)
		fabric := setupFabric(t, warehouse, newFabricUploader(t, []whutils.LoadFile{{Location: location}}, tableName, baseSchema, false, true))
		stats, err := fabric.LoadTable(ctx, tableName)
		require.Error(t, err)
		require.Nil(t, stats)
	})

	t.Run("merge", func(t *testing.T) {
		for _, tc := range []struct {
			name         string
			useNewRecord bool
		}{
			{name: "without dedup", useNewRecord: false},
			{name: "with dedup", useNewRecord: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				tableName := "merge_" + strings.ReplaceAll(tc.name, " ", "_")
				location := uploadFabricFixture(t, fm, "../testdata/load.parquet", tableName)
				fabric := setupFabric(t, warehouse, newFabricUploader(t, []whutils.LoadFile{{Location: location}}, tableName, baseSchema, false, tc.useNewRecord))
				require.NoError(t, fabric.CreateTable(ctx, tableName, baseSchema))

				first, err := fabric.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, int64(14), first.RowsInserted)
				second, err := fabric.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, int64(14), second.RowsUpdated)
				require.Equal(t, whth.SampleTestRecords(), fabricLoadRecords(t, db, namespace, tableName))
			})
		}
	})

	t.Run("append", func(t *testing.T) {
		tableName := "append_test_table"
		location := uploadFabricFixture(t, fm, "../testdata/load.parquet", tableName)
		appendWarehouse := warehouse
		appendWarehouse.Destination.Config = maps.Clone(warehouse.Destination.Config)
		appendWarehouse.Destination.Config[model.PreferAppendSetting.String()] = true
		fabric := setupFabric(t, appendWarehouse, newFabricUploader(t, []whutils.LoadFile{{Location: location}}, tableName, baseSchema, true, true))
		require.NoError(t, fabric.CreateTable(ctx, tableName, baseSchema))
		first, err := fabric.LoadTable(ctx, tableName)
		require.NoError(t, err)
		require.Equal(t, int64(14), first.RowsInserted)
		second, err := fabric.LoadTable(ctx, tableName)
		require.NoError(t, err)
		require.Equal(t, int64(14), second.RowsInserted)
		require.Equal(t, whth.AppendTestRecords(), fabricLoadRecords(t, db, namespace, tableName))
	})

	t.Run("load file does not exist", func(t *testing.T) {
		tableName := "missing_load_file_test_table"
		location := uploadFabricFixture(t, fm, "../testdata/load.parquet", tableName)
		objectName, err := fm.GetObjectNameFromLocation(location)
		require.NoError(t, err)
		require.NoError(t, fm.Delete(ctx, []string{objectName}))
		fabric := setupFabric(t, warehouse, newFabricUploader(t, []whutils.LoadFile{{Location: location}}, tableName, baseSchema, false, true))
		require.NoError(t, fabric.CreateTable(ctx, tableName, baseSchema))
		stats, err := fabric.LoadTable(ctx, tableName)
		require.Error(t, err)
		require.Nil(t, stats)
	})

	t.Run("column count mismatch", func(t *testing.T) {
		tableName := "column_count_mismatch_test_table"
		location := uploadFabricFixture(t, fm, "../testdata/load.parquet", tableName)
		uploadSchema := maps.Clone(baseSchema)
		uploadSchema["unexpected_column"] = model.StringDataType
		fabric := setupFabric(t, warehouse, newFabricUploader(t, []whutils.LoadFile{{Location: location}}, tableName, uploadSchema, false, true))
		require.NoError(t, fabric.CreateTable(ctx, tableName, baseSchema))
		stats, err := fabric.LoadTable(ctx, tableName)
		require.Error(t, err)
		require.Nil(t, stats)
	})

	t.Run("schema mismatch", func(t *testing.T) {
		tableName := "schema_mismatch_test_table"
		location := uploadFabricFixture(t, fm, "../testdata/load.parquet", tableName)
		warehouseSchema := maps.Clone(baseSchema)
		warehouseSchema["test_string"] = model.FloatDataType
		fabric := setupFabric(t, warehouse, newFabricUploader(t, []whutils.LoadFile{{Location: location}}, tableName, baseSchema, false, true))
		require.NoError(t, fabric.CreateTable(ctx, tableName, warehouseSchema))
		stats, err := fabric.LoadTable(ctx, tableName)
		require.Error(t, err)
		require.Nil(t, stats)
		require.Equal(t, model.AlterColumnError, (&router.ErrorHandler{Mapper: fabric}).MatchUploadJobErrorType(err))
	})

	t.Run("discards", func(t *testing.T) {
		tableName := whutils.DiscardsTable
		filePath := writeFabricParquet(t, whutils.DiscardsSchema, fabricDiscardRows())
		location := uploadFabricFixture(t, fm, filePath, tableName)
		fabric := setupFabric(t, warehouse, newFabricUploader(t, []whutils.LoadFile{{Location: location}}, tableName, whutils.DiscardsSchema, false, true))
		require.NoError(t, fabric.CreateTable(ctx, tableName, whutils.DiscardsSchema))
		loadStats, err := fabric.LoadTable(ctx, tableName)
		require.NoError(t, err)
		require.Equal(t, int64(6), loadStats.RowsInserted)
		records := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT [column_name], [column_value], [reason], [received_at], [row_id], [table_name], [uuid_ts] FROM %s ORDER BY [row_id];`, fabricQualified(namespace, tableName)))
		require.Equal(t, whth.DiscardTestRecords(), records)
	})

	t.Run("multiple load files", func(t *testing.T) {
		tableName := "multiple_load_files_test_table"
		loadFiles := lo.RepeatBy(3, func(_ int) whutils.LoadFile {
			return whutils.LoadFile{Location: uploadFabricFixture(t, fm, "../testdata/load.parquet", tableName)}
		})
		fabric := setupFabric(t, warehouse, newFabricUploader(t, loadFiles, tableName, baseSchema, false, true))
		require.NoError(t, fabric.CreateTable(ctx, tableName, baseSchema))
		loadStats, err := fabric.LoadTable(ctx, tableName)
		require.NoError(t, err)
		require.Equal(t, int64(14), loadStats.RowsInserted)
		require.Equal(t, whth.SampleTestRecords(), fabricLoadRecords(t, db, namespace, tableName))
	})

	t.Run("prevents SQL injection via malicious identifiers", func(t *testing.T) {
		victimTable := "victim_secrets"
		require.NoError(t, admin.CreateTable(ctx, victimTable, model.TableSchema{"id": model.StringDataType}))

		maliciousNamespace := whth.RandSchema(whutils.MicrosoftFabric) + `];DROP SCHEMA dbo;--`
		maliciousTable := `evil];DROP TABLE victim_secrets;--`
		maliciousColumn := `value];DROP TABLE victim_secrets;--`
		maliciousSchema := model.TableSchema{
			"id":            model.StringDataType,
			"received_at":   model.DateTimeDataType,
			maliciousColumn: model.StringDataType,
		}
		maliciousWarehouse := fabricWarehouse(credentials, maliciousNamespace)
		maliciousFile := writeFabricParquet(t, maliciousSchema, []map[string]any{{
			"id":            "one",
			"received_at":   "2025-01-02T03:04:05Z",
			maliciousColumn: "safe",
		}})
		location := uploadFabricFixture(t, fm, maliciousFile, "malicious_identifiers_fixture")
		fabric := setupFabric(t, maliciousWarehouse, newFabricUploader(t, []whutils.LoadFile{{Location: location}}, maliciousTable, maliciousSchema, false, true))
		t.Cleanup(func() { dropFabricSchema(t, db, maliciousNamespace) })
		require.NoError(t, fabric.CreateSchema(ctx))
		require.NoError(t, fabric.CreateTable(ctx, maliciousTable, maliciousSchema))
		_, err := fabric.LoadTable(ctx, maliciousTable)
		require.NoError(t, err)

		var victimExists int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table;`, sql.Named("schema", namespace), sql.Named("table", victimTable)).Scan(&victimExists))
		require.Equal(t, 1, victimExists)
	})
}

func fabricDestinationConfig(credentials fabricTestCredentials, namespace string) map[string]any {
	return map[string]any{
		"host":                      credentials.Host,
		"database":                  credentials.Database,
		"fabricWorkspaceId":         credentials.FabricWorkspaceID,
		"lakehouseId":               credentials.LakehouseID,
		"tenantId":                  credentials.TenantID,
		"clientId":                  credentials.ClientID,
		"clientSecret":              credentials.ClientSecret,
		"namespace":                 namespace,
		"syncFrequency":             "30",
		"preferAppend":              false,
		"allowUsersContextTraits":   true,
		"cleanupObjectStorageFiles": true,
		"useRudderStorage":          false,
	}
}

func fabricWarehouse(credentials fabricTestCredentials, namespace string) model.Warehouse {
	return model.Warehouse{
		Source: backendconfig.SourceT{ID: "test_source_id"},
		Destination: backendconfig.DestinationT{
			ID: "test_destination_id",
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				Name:        whutils.MicrosoftFabric,
				DisplayName: "Microsoft Fabric",
			},
			Config:      fabricDestinationConfig(credentials, namespace),
			WorkspaceID: "test_workspace_id",
		},
		WorkspaceID: "test_workspace_id",
		Namespace:   namespace,
	}
}

func openFabricDB(t testing.TB, credentials fabricTestCredentials) *sql.DB {
	t.Helper()
	fabric := microsoftfabric.New(config.New(), logger.NOP, stats.NOP)
	fabric.SetConnectionTimeout(time.Minute)
	c, err := fabric.Connect(context.Background(), fabricWarehouse(credentials, ""))
	require.NoError(t, err)
	db := c.SQL
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func setupFabric(t testing.TB, warehouse model.Warehouse, uploader whutils.Uploader) *microsoftfabric.MicrosoftFabric {
	t.Helper()
	fabric := microsoftfabric.New(config.New(), logger.NOP, stats.NOP)
	fabric.SetConnectionTimeout(time.Minute)
	require.NoError(t, fabric.Setup(context.Background(), warehouse, uploader))
	t.Cleanup(func() { fabric.Cleanup(context.Background()) })
	return fabric
}

func newFabricUploader(
	t testing.TB,
	loadFiles []whutils.LoadFile,
	tableName string,
	tableSchema model.TableSchema,
	canAppend bool,
	useNewRecord bool,
) whutils.Uploader {
	t.Helper()
	ctrl := gomock.NewController(t)
	mockUploader := mockuploader.NewMockUploader(ctrl)
	mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()
	mockUploader.EXPECT().CanAppend().Return(canAppend).AnyTimes()
	mockUploader.EXPECT().ShouldOnDedupUseNewRecord().Return(useNewRecord).AnyTimes()
	mockUploader.EXPECT().GetLoadFilesMetadata(gomock.Any(), gomock.Any()).Return(loadFiles, nil).AnyTimes()
	mockUploader.EXPECT().GetTableSchemaInUpload(tableName).Return(tableSchema).AnyTimes()
	mockUploader.EXPECT().GetTableSchemaInWarehouse(tableName).Return(tableSchema).AnyTimes()
	mockUploader.EXPECT().GetLoadFileType().Return(whutils.LoadFileTypeParquet).AnyTimes()
	return mockUploader
}

func uploadFabricFixture(t testing.TB, fm filemanager.FileManager, fileName, tableName string) string {
	t.Helper()
	upload := whth.UploadLoadFile(t, fm, fileName, tableName)
	t.Cleanup(func() {
		objectName, err := fm.GetObjectNameFromLocation(upload.Location)
		if err != nil {
			t.Errorf("resolve OneLake cleanup object %q: %v", upload.Location, err)
			return
		}
		if err := fm.Delete(context.Background(), []string{objectName}); err != nil {
			t.Logf("delete OneLake integration object %q: %v", objectName, err)
		}
	})
	return upload.Location
}

func cleanupFabricObjectsForSource(t testing.TB, fm filemanager.FileManager, sourceID string) {
	t.Helper()
	for _, prefix := range []string{"rudder-warehouse-staging-logs/", "rudder-warehouse-load-objects/"} {
		session := fm.ListFilesWithPrefix(context.Background(), "", prefix, 5000)
		for {
			files, err := session.Next()
			if err != nil {
				t.Logf("list OneLake integration objects under %q: %v", prefix, err)
				break
			}
			if len(files) == 0 {
				break
			}
			keys := lo.FilterMap(files, func(file *filemanager.FileInfo, _ int) (string, bool) {
				return file.Key, strings.Contains(file.Key, "/"+sourceID+"/")
			})
			if len(keys) > 0 {
				if err := fm.Delete(context.Background(), keys); err != nil {
					t.Logf("delete OneLake integration objects for source %q: %v", sourceID, err)
				}
			}
		}
	}
}

func writeFabricParquet(t testing.TB, schema model.TableSchema, rows []map[string]any) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fabric.parquet")
	factory := encoding.NewFactory(config.New())
	writer, err := factory.NewLoadFileWriter(whutils.LoadFileTypeParquet, path, schema, whutils.MicrosoftFabric)
	require.NoError(t, err)
	columns := slices.Sorted(maps.Keys(schema))
	for _, row := range rows {
		loader := factory.NewEventLoader(writer, whutils.LoadFileTypeParquet, whutils.MicrosoftFabric)
		for _, column := range columns {
			loader.AddColumn(column, schema[column], row[column])
		}
		require.NoError(t, loader.Write())
	}
	require.NoError(t, writer.Close())
	return path
}

func fabricDiscardRows() []map[string]any {
	rows := whth.DiscardTestRecords()
	return lo.Map(rows, func(row []string, _ int) map[string]any {
		return map[string]any{
			"column_name": row[0], "column_value": row[1], "reason": row[2],
			"received_at": row[3], "row_id": row[4], "table_name": row[5], "uuid_ts": row[6],
		}
	})
}

func fabricLoadRecords(t testing.TB, db *sql.DB, namespace, tableName string) [][]string {
	t.Helper()
	return whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT [id], [received_at], [test_bool], [test_datetime], [test_float], [test_int], [test_string] FROM %s ORDER BY [id];`, fabricQualified(namespace, tableName)))
}

func verifyFabricEventSchema(t testing.TB, db *sql.DB, namespace string, storeFullEvent bool) {
	t.Helper()
	records := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT [table_name], [column_name], [data_type] FROM INFORMATION_SCHEMA.COLUMNS WHERE [table_schema] = %s;`, whutils.UnicodeStringLiteral(namespace)))
	schema := whth.ConvertRecordsToSchema(records)
	require.Equal(t, expectedFabricEventSchema(storeFullEvent), schema)
	if !storeFullEvent {
		return
	}
	for _, table := range fabricEventTables() {
		var maxLength int64
		require.NoError(t, db.QueryRowContext(context.Background(), `SELECT [character_maximum_length] FROM INFORMATION_SCHEMA.COLUMNS WHERE [table_schema] = @schema AND [table_name] = @table AND [column_name] = 'rudder_event';`, sql.Named("schema", namespace), sql.Named("table", table)).Scan(&maxLength))
		require.Equal(t, int64(-1), maxLength)
		var invalidJSON int
		require.NoError(t, db.QueryRowContext(context.Background(), fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE [rudder_event] IS NULL OR ISJSON([rudder_event]) <> 1;`, fabricQualified(namespace, table))).Scan(&invalidJSON))
		require.Zero(t, invalidJSON)
	}
}

func expectedFabricEventSchema(storeFullEvent bool) model.Schema {
	schema := model.Schema{
		"screens":       {"context_source_id": "varchar", "user_id": "varchar", "sent_at": "datetime2", "context_request_ip": "varchar", "original_timestamp": "datetime2", "url": "varchar", "context_source_type": "varchar", "_between": "varchar", "timestamp": "datetime2", "context_ip": "varchar", "context_destination_type": "varchar", "received_at": "datetime2", "title": "varchar", "uuid_ts": "datetime2", "context_destination_id": "varchar", "name": "varchar", "id": "varchar", "_as": "varchar"},
		"identifies":    {"context_ip": "varchar", "context_destination_id": "varchar", "email": "varchar", "context_request_ip": "varchar", "sent_at": "datetime2", "uuid_ts": "datetime2", "_as": "varchar", "logins": "bigint", "context_source_type": "varchar", "context_traits_logins": "bigint", "name": "varchar", "context_destination_type": "varchar", "_between": "varchar", "id": "varchar", "timestamp": "datetime2", "received_at": "datetime2", "user_id": "varchar", "context_traits_email": "varchar", "context_traits_as": "varchar", "context_traits_name": "varchar", "original_timestamp": "datetime2", "context_traits_between": "varchar", "context_source_id": "varchar"},
		"users":         {"context_traits_name": "varchar", "context_traits_between": "varchar", "context_request_ip": "varchar", "context_traits_logins": "bigint", "context_destination_id": "varchar", "email": "varchar", "logins": "bigint", "_as": "varchar", "context_source_id": "varchar", "uuid_ts": "datetime2", "context_source_type": "varchar", "context_traits_email": "varchar", "name": "varchar", "id": "varchar", "_between": "varchar", "context_ip": "varchar", "received_at": "datetime2", "sent_at": "datetime2", "context_traits_as": "varchar", "context_destination_type": "varchar", "timestamp": "datetime2", "original_timestamp": "datetime2"},
		"product_track": {"review_id": "varchar", "context_source_id": "varchar", "user_id": "varchar", "timestamp": "datetime2", "uuid_ts": "datetime2", "review_body": "varchar", "context_source_type": "varchar", "_as": "varchar", "_between": "varchar", "id": "varchar", "rating": "bigint", "event": "varchar", "original_timestamp": "datetime2", "context_destination_type": "varchar", "context_ip": "varchar", "context_destination_id": "varchar", "sent_at": "datetime2", "received_at": "datetime2", "event_text": "varchar", "product_id": "varchar", "context_request_ip": "varchar"},
		"tracks":        {"original_timestamp": "datetime2", "context_destination_id": "varchar", "event": "varchar", "context_request_ip": "varchar", "uuid_ts": "datetime2", "context_destination_type": "varchar", "user_id": "varchar", "sent_at": "datetime2", "context_source_type": "varchar", "context_ip": "varchar", "timestamp": "datetime2", "received_at": "datetime2", "context_source_id": "varchar", "event_text": "varchar", "id": "varchar"},
		"aliases":       {"context_request_ip": "varchar", "context_destination_type": "varchar", "context_destination_id": "varchar", "previous_id": "varchar", "context_ip": "varchar", "sent_at": "datetime2", "id": "varchar", "uuid_ts": "datetime2", "timestamp": "datetime2", "original_timestamp": "datetime2", "context_source_id": "varchar", "user_id": "varchar", "context_source_type": "varchar", "received_at": "datetime2"},
		"pages":         {"name": "varchar", "url": "varchar", "id": "varchar", "timestamp": "datetime2", "title": "varchar", "user_id": "varchar", "context_source_id": "varchar", "context_source_type": "varchar", "original_timestamp": "datetime2", "context_request_ip": "varchar", "received_at": "datetime2", "_between": "varchar", "context_destination_type": "varchar", "uuid_ts": "datetime2", "context_destination_id": "varchar", "sent_at": "datetime2", "context_ip": "varchar", "_as": "varchar"},
		"groups":        {"_as": "varchar", "user_id": "varchar", "context_destination_type": "varchar", "sent_at": "datetime2", "context_source_type": "varchar", "received_at": "datetime2", "context_ip": "varchar", "industry": "varchar", "timestamp": "datetime2", "group_id": "varchar", "uuid_ts": "datetime2", "context_source_id": "varchar", "context_request_ip": "varchar", "_between": "varchar", "original_timestamp": "datetime2", "name": "varchar", "_plan": "varchar", "context_destination_id": "varchar", "employees": "bigint", "id": "varchar"},
	}
	if storeFullEvent {
		for _, table := range fabricEventTables() {
			schema[table]["rudder_event"] = "varchar"
		}
	}
	return schema
}

func verifyFabricEventRecords(t testing.TB, db *sql.DB, sourceID, destinationID, namespace string, mode eventRecordMode) {
	t.Helper()
	userIDFormat := "userId_microsoft_fabric"
	userIDSQL := "LEFT([user_id], CHARINDEX('_', [user_id], CHARINDEX('_', [user_id], CHARINDEX('_', [user_id]) + 1) + 1) - 1)"
	userTableIDSQL := "LEFT([id], CHARINDEX('_', [id], CHARINDEX('_', [id], CHARINDEX('_', [id]) + 1) + 1) - 1)"
	uuidTSSQL := "CONVERT(varchar(10), [uuid_ts], 23)"

	type recordsFunc func(string, string, string, string) [][]string
	functions := map[string][3]recordsFunc{
		"identifies":    {whth.UploadJobIdentifiesRecords, whth.UploadJobIdentifiesMergeRecords, whth.UploadJobIdentifiesAppendRecords},
		"users":         {whth.UploadJobUsersRecords, whth.UploadJobUsersMergeRecord, whth.UploadJobUsersMergeRecord},
		"tracks":        {whth.UploadJobTracksRecords, whth.UploadJobTracksMergeRecords, whth.UploadJobTracksAppendRecords},
		"product_track": {whth.UploadJobProductTrackRecords, whth.UploadJobProductTrackMergeRecords, whth.UploadJobProductTrackAppendRecords},
		"pages":         {whth.UploadJobPagesRecords, whth.UploadJobPagesMergeRecords, whth.UploadJobPagesAppendRecords},
		"screens":       {whth.UploadJobScreensRecords, whth.UploadJobScreensMergeRecords, whth.UploadJobScreensAppendRecords},
		"aliases":       {whth.UploadJobAliasesRecords, whth.UploadJobAliasesMergeRecords, whth.UploadJobAliasesAppendRecords},
		"groups":        {whth.UploadJobGroupsRecords, whth.UploadJobGroupsMergeRecords, whth.UploadJobGroupsAppendRecords},
	}
	queries := map[string]string{
		"identifies":    fmt.Sprintf(`SELECT %s, %s, [context_traits_logins], [_as], [name], [logins], [email], [original_timestamp], [context_ip], [context_traits_as], [timestamp], [received_at], [context_destination_type], [sent_at], [context_source_type], [context_traits_between], [context_source_id], [context_traits_name], [context_request_ip], [_between], [context_traits_email], [context_destination_id], [id] FROM %s ORDER BY [id];`, userIDSQL, uuidTSSQL, fabricQualified(namespace, "identifies")),
		"users":         fmt.Sprintf(`SELECT [context_source_id], [context_destination_type], [context_request_ip], [context_traits_name], [context_traits_between], [_as], [logins], [sent_at], [context_traits_logins], [context_ip], [_between], [context_traits_email], [timestamp], [context_destination_id], [email], [context_traits_as], [context_source_type], %s, %s, [received_at], [name], [original_timestamp] FROM %s ORDER BY [id];`, userTableIDSQL, uuidTSSQL, fabricQualified(namespace, "users")),
		"tracks":        fmt.Sprintf(`SELECT [original_timestamp], [context_destination_id], [context_destination_type], %s, [context_source_type], [timestamp], [id], [event], [sent_at], [context_ip], [event_text], [context_source_id], [context_request_ip], [received_at], %s FROM %s ORDER BY [id];`, uuidTSSQL, userIDSQL, fabricQualified(namespace, "tracks")),
		"product_track": fmt.Sprintf(`SELECT [timestamp], %s, [product_id], [received_at], [context_source_id], [sent_at], [context_source_type], [context_ip], [context_destination_type], [original_timestamp], [context_request_ip], [context_destination_id], %s, [_as], [review_body], [_between], [review_id], [event_text], [id], [event], [rating] FROM %s ORDER BY [id];`, userIDSQL, uuidTSSQL, fabricQualified(namespace, "product_track")),
		"pages":         fmt.Sprintf(`SELECT %s, [context_source_id], [id], [title], [timestamp], [context_source_type], [_as], [received_at], [context_destination_id], [context_ip], [context_destination_type], [name], [original_timestamp], [_between], [context_request_ip], [sent_at], [url], %s FROM %s ORDER BY [id];`, userIDSQL, uuidTSSQL, fabricQualified(namespace, "pages")),
		"screens":       fmt.Sprintf(`SELECT [context_destination_type], [url], [context_source_type], [title], [original_timestamp], %s, [_between], [context_ip], [name], [context_request_ip], %s, [context_source_id], [id], [received_at], [context_destination_id], [timestamp], [sent_at], [_as] FROM %s ORDER BY [id];`, userIDSQL, uuidTSSQL, fabricQualified(namespace, "screens")),
		"aliases":       fmt.Sprintf(`SELECT [context_source_id], [context_destination_id], [context_ip], [sent_at], [id], %s, %s, [previous_id], [original_timestamp], [context_source_type], [received_at], [context_destination_type], [context_request_ip], [timestamp] FROM %s ORDER BY [id];`, userIDSQL, uuidTSSQL, fabricQualified(namespace, "aliases")),
		"groups":        fmt.Sprintf(`SELECT [context_destination_type], [id], [_between], [_plan], [original_timestamp], %s, [context_source_id], [sent_at], %s, [group_id], [industry], [context_request_ip], [context_source_type], [timestamp], [employees], [_as], [context_destination_id], [received_at], [name], [context_ip] FROM %s ORDER BY [id];`, uuidTSSQL, userIDSQL, fabricQualified(namespace, "groups")),
	}
	for _, table := range fabricEventTables() {
		records := whth.RetrieveRecordsFromWarehouse(t, db, queries[table])
		require.ElementsMatch(t, functions[table][mode](userIDFormat, sourceID, destinationID, whutils.MicrosoftFabric), records, table)
	}
}

func fabricEventTables() []string {
	return []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}
}

func dropFabricSchema(t testing.TB, db *sql.DB, namespace string) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `SELECT [table_name] FROM INFORMATION_SCHEMA.TABLES WHERE [table_schema] = @schema;`, sql.Named("schema", namespace))
	if err != nil {
		t.Logf("list tables for schema cleanup %q: %v", namespace, err)
		return
	}
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			t.Logf("scan table for schema cleanup %q: %v", namespace, err)
			continue
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		t.Logf("iterate tables for schema cleanup %q: %v", namespace, err)
	}
	_ = rows.Close()
	for _, table := range tables {
		if _, err := db.ExecContext(context.Background(), "DROP TABLE "+fabricQualified(namespace, table)); err != nil {
			t.Logf("drop table %q.%q: %v", namespace, table, err)
		}
	}
	if _, err := db.ExecContext(context.Background(), "DROP SCHEMA "+whutils.BracketQuoteIdentifier(namespace)); err != nil && !strings.Contains(strings.ToLower(err.Error()), "does not exist") {
		t.Logf("drop schema %q: %v", namespace, err)
	}
}

func fabricQualified(namespace, table string) string {
	return whutils.QuoteQualifiedIdentifier(whutils.BracketQuoteIdentifier, namespace, table)
}
