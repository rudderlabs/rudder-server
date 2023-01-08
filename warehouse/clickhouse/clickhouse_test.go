package clickhouse_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

func TestIntegrationClickHouse(t *testing.T) {
	if os.Getenv("SLOW") == "0" {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}

	clickhouse.Init()

	var dbs []*sql.DB
	for _, host := range []string{"wh-clickhouse", "wh-clickhouse01", "wh-clickhouse02", "wh-clickhouse03", "wh-clickhouse04"} {
		ch := clickhouse.NewHandle()
		db, err := ch.ConnectToClickhouse(clickhouse.Credentials{
			Host:          host,
			User:          "rudder",
			Password:      "rudder-password",
			DBName:        "rudderdb",
			Secure:        "false",
			SkipVerify:    "true",
			TLSConfigName: "",
			Port:          "9000",
		}, true)
		require.NoError(t, err)

		err = db.Ping()
		require.NoError(t, err)

		dbs = append(dbs, db)
	}

	var (
		provider = warehouseutils.CLICKHOUSE
		jobsDB   = testhelper.SetUpJobsDB(t)
		tables   = []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}
	)

	testCases := []struct {
		name                        string
		sourceID                    string
		destinationID               string
		writeKey                    string
		warehouseEvents             testhelper.EventsCountMap
		warehouseModifiedEvents     testhelper.EventsCountMap
		clusterSetup                func(t testing.TB)
		db                          *sql.DB
		s3EngineEnabledWorkspaceIDs []string
	}{
		{
			name:          "Single Setup",
			sourceID:      "1wRvLmEnMOOxNM79pwaZhyCqXRE",
			destinationID: "21Ev6TI6emCFDKph2Zn6XfTP7PI",
			writeKey:      "C5AWX39IVUWSP2NcHciWvqZTa2N",
			db:            dbs[0],
		},
		{
			name:          "Single Setup with S3 Engine",
			sourceID:      "1wRvLmEnMOOxNM79pwaZhyCqXRE",
			destinationID: "21Ev6TI6emCFDKph2Zn6XfTP7PI",
			writeKey:      "C5AWX39IVUWSP2NcHciWvqZTa2N",
			db:            dbs[0],
			s3EngineEnabledWorkspaceIDs: []string{
				"BpLnfgDsc2WD8F2qNfHK5a84jjJ",
			},
		},
		{
			name:          "Cluster Mode Setup",
			sourceID:      "1wRvLmEnMOOxNM79ghdZhyCqXRE",
			destinationID: "21Ev6TI6emCFDKhp2Zn6XfTP7PI",
			writeKey:      "95RxRTZHWUsaD6HEdz0ThbXfQ6p",
			db:            dbs[1],
			warehouseModifiedEvents: testhelper.EventsCountMap{
				"identifies":    8,
				"users":         2,
				"tracks":        8,
				"product_track": 8,
				"pages":         8,
				"screens":       8,
				"aliases":       8,
				"groups":        8,
			},
			clusterSetup: func(t testing.TB) {
				t.Helper()
				initializeClickhouseClusterMode(t, dbs[1:], tables)
			},
		},
		{
			name:          "Cluster Mode Setup with S3 Engine",
			sourceID:      "1wRvLmEnMOOxNM79ghdZhyCqXRE",
			destinationID: "21Ev6TI6emCFDKhp2Zn6XfTP7PI",
			writeKey:      "95RxRTZHWUsaD6HEdz0ThbXfQ6p",
			db:            dbs[1],
			warehouseEvents: testhelper.EventsCountMap{
				"identifies":    8,
				"users":         2,
				"tracks":        8,
				"product_track": 8,
				"pages":         8,
				"screens":       8,
				"aliases":       8,
				"groups":        8,
			},
			warehouseModifiedEvents: testhelper.EventsCountMap{
				"identifies":    8,
				"users":         2,
				"tracks":        8,
				"product_track": 8,
				"pages":         8,
				"screens":       8,
				"aliases":       8,
				"groups":        8,
			},
			s3EngineEnabledWorkspaceIDs: []string{
				"BpLnfgDsc2WD8F2qNfHK5a84jjJ",
			},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			testhelper.SetConfig(t, []warehouseutils.KeyValue{
				{
					Key:   "Warehouse.clickhouse.s3EngineEnabledWorkspaceIDs",
					Value: tc.s3EngineEnabledWorkspaceIDs,
				},
			})

			ts := testhelper.WareHouseTest{
				Schema:             "rudderdb",
				WriteKey:           tc.writeKey,
				SourceID:           tc.sourceID,
				DestinationID:      tc.destinationID,
				WarehouseEventsMap: tc.warehouseEvents,
				Tables:             tables,
				Provider:           provider,
				JobsDB:             jobsDB,
				UserID:             testhelper.GetUserId(provider),
				StatsToVerify: []string{
					"warehouse_clickhouse_commitTimeouts",
					"warehouse_clickhouse_execTimeouts",
					"warehouse_clickhouse_failedRetries",
					"warehouse_clickhouse_syncLoadFileTime",
					"warehouse_clickhouse_downloadLoadFilesTime",
					"warehouse_clickhouse_numRowsLoadFile",
				},
				Client: &client.Client{
					SQL:  tc.db,
					Type: client.SQLClient,
				},
			}
			ts.VerifyEvents(t)

			if tc.clusterSetup != nil {
				tc.clusterSetup(t)
			}

			ts.UserID = testhelper.GetUserId(provider)
			ts.WarehouseEventsMap = tc.warehouseModifiedEvents
			ts.VerifyModifiedEvents(t)
		})
	}
}

func TestConfigurationValidationClickhouse(t *testing.T) {
	if os.Getenv("SLOW") == "0" {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}

	misc.Init()
	validations.Init()
	warehouseutils.Init()
	clickhouse.Init()

	configurations := testhelper.PopulateTemplateConfigurations()
	destination := backendconfig.DestinationT{
		ID: "21Ev6TI6emCFDKph2Zn6XfTP7PI",
		Config: map[string]interface{}{
			"host":             configurations["clickHouseHost"],
			"database":         configurations["clickHouseDatabase"],
			"cluster":          "",
			"user":             configurations["clickHouseUser"],
			"password":         configurations["clickHousePassword"],
			"port":             configurations["clickHousePort"],
			"secure":           false,
			"namespace":        "",
			"bucketProvider":   "MINIO",
			"bucketName":       configurations["minioBucketName"],
			"accessKeyID":      configurations["minioAccesskeyID"],
			"secretAccessKey":  configurations["minioSecretAccessKey"],
			"useSSL":           false,
			"endPoint":         configurations["minioEndpoint"],
			"syncFrequency":    "30",
			"useRudderStorage": false,
		},
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			ID:          "1eBvkIRSwc2ESGMK9dj6OXq2G12",
			Name:        "CLICKHOUSE",
			DisplayName: "ClickHouse",
		},
		Name:       "clickhouse-demo",
		Enabled:    true,
		RevisionID: "29eeuTnqbBKn0XVTj5z9XQIbaru",
	}
	testhelper.VerifyConfigurationTest(t, destination)
}

func TestHandle_UseS3CopyEngineForLoading(t *testing.T) {
	S3EngineEnabledWorkspaceIDs := []string{"BpLnfgDsc2WD8F2qNfHK5a84jjJ"}

	testCases := []struct {
		name          string
		ObjectStorage string
		workspaceID   string
		useS3Engine   bool
	}{
		{
			name:          "incompatible object storage(AZURE BLOB)",
			ObjectStorage: warehouseutils.AZURE_BLOB,
			workspaceID:   "test-workspace-id",
		},
		{
			name:          "incompatible object storage(GCS)",
			ObjectStorage: warehouseutils.GCS,
			workspaceID:   "test-workspace-id",
		},
		{
			name:          "incompatible workspace",
			ObjectStorage: warehouseutils.S3,
			workspaceID:   "test-workspace-id",
		},
		{
			name:          "compatible workspace with incompatible object storage",
			ObjectStorage: warehouseutils.GCS,
			workspaceID:   "BpLnfgDsc2WD8F2qNfHK5a84jjJ",
		},
		{
			name:          "compatible workspace(S3)",
			ObjectStorage: warehouseutils.S3,
			workspaceID:   "BpLnfgDsc2WD8F2qNfHK5a84jjJ",
			useS3Engine:   true,
		},
		{
			name:          "compatible workspace(MINIO)",
			ObjectStorage: warehouseutils.MINIO,
			workspaceID:   "BpLnfgDsc2WD8F2qNfHK5a84jjJ",
			useS3Engine:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := config.New()
			c.Set("Warehouse.clickhouse.s3EngineEnabledWorkspaceIDs", S3EngineEnabledWorkspaceIDs)

			ch := clickhouse.NewHandle()
			clickhouse.WithConfig(ch, c)

			ch.Warehouse = warehouseutils.Warehouse{
				WorkspaceID: tc.workspaceID,
			}
			ch.ObjectStorage = tc.ObjectStorage

			require.Equal(t, tc.useS3Engine, ch.UseS3CopyEngineForLoading())
		})
	}
}

type mockUploader struct {
	minioPort   string
	tableSchema warehouseutils.TableSchemaT
	metadata    []warehouseutils.LoadFileT
}

func (*mockUploader) GetSchemaInWarehouse() warehouseutils.SchemaT     { return warehouseutils.SchemaT{} }
func (*mockUploader) GetLocalSchema() warehouseutils.SchemaT           { return warehouseutils.SchemaT{} }
func (*mockUploader) UpdateLocalSchema(_ warehouseutils.SchemaT) error { return nil }
func (*mockUploader) ShouldOnDedupUseNewRecord() bool                  { return false }
func (*mockUploader) UseRudderStorage() bool                           { return false }
func (*mockUploader) GetLoadFileGenStartTIme() time.Time               { return time.Time{} }
func (*mockUploader) GetLoadFileType() string                          { return "JSON" }
func (*mockUploader) GetFirstLastEvent() (time.Time, time.Time)        { return time.Time{}, time.Time{} }
func (*mockUploader) GetTableSchemaInWarehouse(_ string) warehouseutils.TableSchemaT {
	return warehouseutils.TableSchemaT{}
}

func (*mockUploader) GetSingleLoadFile(_ string) (warehouseutils.LoadFileT, error) {
	return warehouseutils.LoadFileT{}, nil
}

func (m *mockUploader) GetSampleLoadFileLocation(_ string) (string, error) {
	minioHostPort := fmt.Sprintf("localhost:%s", m.minioPort)

	sampleLocation := m.metadata[0].Location
	sampleLocation = strings.Replace(sampleLocation, minioHostPort, "minio:9000", 1)
	return sampleLocation, nil
}

func (m *mockUploader) GetTableSchemaInUpload(tableName string) warehouseutils.TableSchemaT {
	return m.tableSchema
}

func (m *mockUploader) GetLoadFilesMetadata(op warehouseutils.GetLoadFilesOptionsT) []warehouseutils.LoadFileT {
	return m.metadata
}

func TestHandle_LoadTableRoundTrip(t *testing.T) {
	misc.Init()
	warehouseutils.Init()
	clickhouse.Init()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	var (
		minioResource *destination.MINIOResource
		chResource    *dockertest.Resource
		databaseName  = "rudderdb"
		password      = "rudder-password"
		user          = "rudder"
		table         = "test_table"
		workspaceID   = "test_workspace_id"
		provider      = "MINIO"
	)

	g := errgroup.Group{}
	g.Go(func() error {
		chResource, err = pool.RunWithOptions(&dockertest.RunOptions{
			Repository: "yandex/clickhouse-server",
			Tag:        "21-alpine",
			Env: []string{
				fmt.Sprintf("CLICKHOUSE_DB=%s", databaseName),
				fmt.Sprintf("CLICKHOUSE_PASSWORD=%s", password),
				fmt.Sprintf("CLICKHOUSE_USER=%s", user),
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			if err := pool.Purge(chResource); err != nil {
				t.Log("Could not purge resource:", err)
			}
		})

		return nil
	})
	g.Go(func() error {
		minioResource, err = destination.SetupMINIO(pool, t)
		require.NoError(t, err)

		return nil
	})
	require.NoError(t, g.Wait())

	t.Logf("Setting up ClickHouse Minio network")
	network, err := pool.Client.CreateNetwork(dc.CreateNetworkOptions{
		Name: "clickhouse-minio-network",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := pool.Client.RemoveNetwork(network.ID); err != nil {
			t.Log("could not remove clickhouse minio network:", err)
		}
	})

	for _, resourceName := range []string{chResource.Container.Name, minioResource.ResourceName} {
		err = pool.Client.ConnectNetwork(network.ID, dc.NetworkConnectionOptions{
			Container:      resourceName,
			EndpointConfig: &dc.EndpointConfig{},
		})
		require.NoError(t, err)
	}

	testCases := []struct {
		name                        string
		fileName                    string
		S3EngineEnabledWorkspaceIDs []string
		disableNullable             bool
	}{
		{
			name:     "normal loading using downloading of load files",
			fileName: "testdata/load.csv.gz",
		},
		{
			name:                        "using s3 engine for loading",
			S3EngineEnabledWorkspaceIDs: []string{workspaceID},
			fileName:                    "testdata/load-copy.csv.gz",
		},
		{
			name:            "normal loading using downloading of load files with disable nullable",
			fileName:        "testdata/load.csv.gz",
			disableNullable: true,
		},
		{
			name:                        "using s3 engine for loading with disable nullable",
			S3EngineEnabledWorkspaceIDs: []string{workspaceID},
			fileName:                    "testdata/load-copy.csv.gz",
			disableNullable:             true,
		},
	}

	for i, tc := range testCases {
		i := i
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ch := clickhouse.NewHandle()
			ch.Logger = logger.NOP

			conf := config.New()
			conf.Set("Warehouse.clickhouse.s3EngineEnabledWorkspaceIDs", tc.S3EngineEnabledWorkspaceIDs)
			conf.Set("Warehouse.clickhouse.disableNullable", tc.disableNullable)

			clickhouse.WithConfig(ch, conf)

			warehouse := warehouseutils.Warehouse{
				Namespace:   fmt.Sprintf("test_namespace_%d", i),
				WorkspaceID: workspaceID,
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"bucketProvider":  provider,
						"host":            "localhost",
						"port":            chResource.GetPort("9000/tcp"),
						"database":        databaseName,
						"user":            user,
						"password":        password,
						"bucketName":      minioResource.BucketName,
						"accessKeyID":     minioResource.AccessKey,
						"secretAccessKey": minioResource.SecretKey,
						"endPoint":        minioResource.Endpoint,
					},
				},
			}
			mockUploader := &mockUploader{
				tableSchema: warehouseutils.TableSchemaT{
					"alter_test_bool":     "boolean",
					"alter_test_datetime": "datetime",
					"alter_test_float":    "float",
					"alter_test_int":      "int",
					"alter_test_string":   "string",
					"id":                  "string",
					"received_at":         "datetime",
					"test_array_bool":     "array(boolean)",
					"test_array_datetime": "array(datetime)",
					"test_array_float":    "array(float)",
					"test_array_int":      "array(int)",
					"test_array_string":   "array(string)",
					"test_bool":           "boolean",
					"test_datetime":       "datetime",
					"test_float":          "float",
					"test_int":            "int",
					"test_string":         "string",
				},
				minioPort: minioResource.Port,
			}

			t.Logf("Preparing load files metadata")
			f, err := os.Open(tc.fileName)
			require.NoError(t, err)

			defer func() { _ = f.Close() }()

			fmFactory := filemanager.FileManagerFactoryT{}
			fm, err := fmFactory.New(&filemanager.SettingsT{
				Provider: provider,
				Config: map[string]interface{}{
					"bucketName":      minioResource.BucketName,
					"accessKeyID":     minioResource.AccessKey,
					"secretAccessKey": minioResource.SecretKey,
					"endPoint":        minioResource.Endpoint,
					"forcePathStyle":  true,
					"disableSSL":      true,
					"region":          minioResource.SiteRegion,
					"enableSSE":       false,
				},
			})

			uploadOutput, err := fm.Upload(context.TODO(), f, fmt.Sprintf("test_prefix_%d", i))
			require.NoError(t, err)

			mockUploader.metadata = append(mockUploader.metadata, warehouseutils.LoadFileT{
				Location: uploadOutput.Location,
			})

			t.Logf("Setting up clickhouse")
			err = ch.Setup(warehouse, mockUploader)
			require.NoError(t, err)

			t.Logf("Waiting for ClickHouse to be ready")
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			require.Eventually(t, func() bool {
				return ch.Db.PingContext(ctx) == nil
			}, time.Minute, time.Second)

			err = ch.Db.PingContext(ctx)
			require.NoError(t, err)

			t.Logf("Verifying connection")
			_, err = ch.Connect(warehouse)
			require.NoError(t, err)

			t.Logf("Verifying empty schema")
			schema, unrecognizedSchema, err := ch.FetchSchema(warehouse)
			require.NoError(t, err)
			require.Empty(t, schema)
			require.Empty(t, unrecognizedSchema)

			t.Logf("Creating schema")
			err = ch.CreateSchema()
			require.NoError(t, err)

			t.Logf("Creating schema twice should not fail")
			err = ch.CreateSchema()
			require.NoError(t, err)

			t.Logf("Creating table")
			err = ch.CreateTable(table, map[string]string{
				"id":                  "string",
				"test_int":            "int",
				"test_float":          "float",
				"test_bool":           "boolean",
				"test_string":         "string",
				"test_datetime":       "datetime",
				"received_at":         "datetime",
				"test_array_bool":     "array(boolean)",
				"test_array_datetime": "array(datetime)",
				"test_array_float":    "array(float)",
				"test_array_int":      "array(int)",
				"test_array_string":   "array(string)",
			})
			require.NoError(t, err)

			t.Logf("Creating users table")
			err = ch.CreateTable(warehouseutils.UsersTable, map[string]string{
				"id":            "string",
				"user_id":       "string",
				"test_int":      "int",
				"test_float":    "float",
				"test_bool":     "boolean",
				"test_string":   "string",
				"test_datetime": "datetime",
				"received_at":   "datetime",
			})
			require.NoError(t, err)

			t.Logf("Adding columns")
			err = ch.AddColumns(table, []warehouseutils.ColumnInfo{
				{Name: "alter_test_int", Type: "int"},
				{Name: "alter_test_float", Type: "float"},
				{Name: "alter_test_bool", Type: "boolean"},
				{Name: "alter_test_string", Type: "string"},
				{Name: "alter_test_datetime", Type: "datetime"},
			})
			require.NoError(t, err)

			t.Logf("Verifying schema")
			schema, unrecognizedSchema, err = ch.FetchSchema(warehouse)
			require.NoError(t, err)
			require.NotEmpty(t, schema)
			require.Empty(t, unrecognizedSchema)

			t.Logf("Loading data into table")
			err = ch.LoadTable(table)
			require.NoError(t, err)

			t.Logf("Checking table count")
			count, err := ch.GetTotalCountInTable(context.TODO(), table)
			require.NoError(t, err)
			require.EqualValues(t, 2, count)

			t.Logf("Drop table")
			err = ch.DropTable(table)
			require.NoError(t, err)

			t.Logf("Drop users table")
			err = ch.DropTable(warehouseutils.UsersTable)
			require.NoError(t, err)

			t.Logf("Verifying empty schema")
			schema, unrecognizedSchema, err = ch.FetchSchema(warehouse)
			require.NoError(t, err)
			require.Empty(t, schema)
			require.Empty(t, unrecognizedSchema)
		})
	}
}

func initializeClickhouseClusterMode(t testing.TB, clusterDBs []*sql.DB, tables []string) {
	t.Helper()

	type ColumnInfoT struct {
		ColumnName string
		ColumnType string
	}

	tableColumnInfoMap := map[string][]ColumnInfoT{
		"identifies": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"product_track": {
			{
				ColumnName: "revenue",
				ColumnType: "Nullable(Float64)",
			},
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"tracks": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"users": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "SimpleAggregateFunction(anyLast, Nullable(String))",
			},
		},
		"pages": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"screens": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"aliases": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"groups": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
	}

	clusterDB := clusterDBs[0]

	// Rename tables to tables_shard
	for _, table := range tables {
		sqlStatement := fmt.Sprintf("RENAME TABLE %[1]s to %[1]s_shard ON CLUSTER rudder_cluster;", table)
		log.Printf("Renaming tables to sharded tables for distribution view for clickhouse cluster with sqlStatement: %s", sqlStatement)

		require.NoError(t, testhelper.WithConstantBackoff(func() error {
			_, err := clusterDB.Exec(sqlStatement)
			return err
		}))
	}

	// Create distribution views for tables
	for _, table := range tables {
		sqlStatement := fmt.Sprintf(`
			CREATE TABLE rudderdb.%[1]s ON CLUSTER 'rudder_cluster' AS rudderdb.%[1]s_shard ENGINE = Distributed(
			  'rudder_cluster',
			  rudderdb,
			  %[1]s_shard,
			  cityHash64(
				concat(
				  toString(
					toDate(received_at)
				  ),
				  id
				)
			  )
			);`,
			table,
		)
		log.Printf("Creating distribution view for clickhouse cluster with sqlStatement: %s", sqlStatement)

		require.NoError(t, testhelper.WithConstantBackoff(func() error {
			_, err := clusterDB.Exec(sqlStatement)
			return err
		}))
	}

	// Alter columns to all the cluster tables
	for _, clusterDB := range clusterDBs {
		for tableName, columnInfos := range tableColumnInfoMap {
			sqlStatement := fmt.Sprintf(`
				ALTER TABLE rudderdb.%[1]s_shard`,
				tableName,
			)
			for _, columnInfo := range columnInfos {
				sqlStatement += fmt.Sprintf(`
					ADD COLUMN IF NOT EXISTS %[1]s %[2]s,`,
					columnInfo.ColumnName,
					columnInfo.ColumnType,
				)
			}
			sqlStatement = strings.TrimSuffix(sqlStatement, ",")
			log.Printf("Altering columns for distribution view for clickhouse cluster with sqlStatement: %s", sqlStatement)

			require.NoError(t, testhelper.WithConstantBackoff(func() error {
				_, err := clusterDB.Exec(sqlStatement)
				return err
			}))
		}
	}
}
