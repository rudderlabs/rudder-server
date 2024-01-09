package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/runner"
	th "github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/postgres"
	whth "github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/tunnelling"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}

	c := testcompose.New(t, compose.FilePaths([]string{
		"testdata/docker-compose.postgres.yml",
		"testdata/docker-compose.ssh-server.yml",
		"../testdata/docker-compose.jobsdb.yml",
		"../testdata/docker-compose.minio.yml",
	}))
	c.Start(context.Background())

	misc.Init()
	validations.Init()
	warehouseutils.Init()

	jobsDBPort := c.Port("jobsDb", 5432)
	minioPort := c.Port("minio", 9000)
	postgresPort := c.Port("postgres", 5432)
	sshPort := c.Port("ssh-server", 2222)

	httpPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	workspaceID := warehouseutils.RandHex()
	sourceID := warehouseutils.RandHex()
	destinationID := warehouseutils.RandHex()
	writeKey := warehouseutils.RandHex()
	sourcesSourceID := warehouseutils.RandHex()
	sourcesDestinationID := warehouseutils.RandHex()
	sourcesWriteKey := warehouseutils.RandHex()
	tunnelledWriteKey := warehouseutils.RandHex()
	tunnelledSourceID := warehouseutils.RandHex()
	tunnelledDestinationID := warehouseutils.RandHex()

	destType := warehouseutils.POSTGRES

	namespace := whth.RandSchema(destType)
	sourcesNamespace := whth.RandSchema(destType)
	tunnelledNamespace := whth.RandSchema(destType)

	host := "localhost"
	database := "rudderdb"
	user := "rudder"
	password := "rudder-password"

	tunnelledHost := "db-private-postgres"
	tunnelledDatabase := "postgres"
	tunnelledPassword := "postgres"
	tunnelledUser := "postgres"
	tunnelledPort := "5432"

	tunnelledSSHUser := "rudderstack"
	tunnelledSSHHost := "localhost"
	tunnelledPrivateKey := "-----BEGIN OPENSSH PRIVATE KEY-----\\n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\\nbG9jYWwBAgMEBQYH\\n-----END OPENSSH PRIVATE KEY-----"

	bucketName := "testbucket"
	accessKeyID := "MYACCESSKEY"
	secretAccessKey := "MYSECRETKEY"
	region := "us-east-1"

	minioEndpoint := fmt.Sprintf("localhost:%d", minioPort)

	bootstrapSvc := func(t testing.TB, additionalEnvs map[string]string, preferAppend *bool) {
		var preferAppendStr string
		if preferAppend != nil {
			preferAppendStr = fmt.Sprintf(`"preferAppend": %v,`, *preferAppend)
		}
		templateConfigurations := map[string]any{
			"workspaceID":            workspaceID,
			"sourceID":               sourceID,
			"destinationID":          destinationID,
			"writeKey":               writeKey,
			"sourcesSourceID":        sourcesSourceID,
			"sourcesDestinationID":   sourcesDestinationID,
			"sourcesWriteKey":        sourcesWriteKey,
			"tunnelledWriteKey":      tunnelledWriteKey,
			"tunnelledSourceID":      tunnelledSourceID,
			"tunnelledDestinationID": tunnelledDestinationID,
			"host":                   host,
			"database":               database,
			"user":                   user,
			"password":               password,
			"port":                   strconv.Itoa(postgresPort),
			"namespace":              namespace,
			"sourcesNamespace":       sourcesNamespace,
			"tunnelledNamespace":     tunnelledNamespace,
			"tunnelledSSHUser":       tunnelledSSHUser,
			"tunnelledSSHPort":       strconv.Itoa(sshPort),
			"tunnelledSSHHost":       tunnelledSSHHost,
			"tunnelledPrivateKey":    tunnelledPrivateKey,
			"tunnelledHost":          tunnelledHost,
			"tunnelledDatabase":      tunnelledDatabase,
			"tunnelledPort":          tunnelledPort,
			"tunnelledUser":          tunnelledUser,
			"tunnelledPassword":      tunnelledPassword,
			"bucketName":             bucketName,
			"accessKeyID":            accessKeyID,
			"secretAccessKey":        secretAccessKey,
			"endPoint":               minioEndpoint,
			"preferAppend":           preferAppendStr,
		}
		workspaceConfigPath := workspaceConfig.CreateTempFile(t, "testdata/template.json", templateConfigurations)

		whth.EnhanceWithDefaultEnvs(t)
		t.Setenv("JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
		t.Setenv("WAREHOUSE_JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
		t.Setenv("MINIO_ACCESS_KEY_ID", accessKeyID)
		t.Setenv("MINIO_SECRET_ACCESS_KEY", secretAccessKey)
		t.Setenv("MINIO_MINIO_ENDPOINT", minioEndpoint)
		t.Setenv("MINIO_SSL", "false")
		t.Setenv("RSERVER_WAREHOUSE_WEB_PORT", strconv.Itoa(httpPort))
		t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)
		t.Setenv("RSERVER_WAREHOUSE_POSTGRES_MAX_PARALLEL_LOADS", "8")
		t.Setenv("RSERVER_WAREHOUSE_POSTGRES_SKIP_COMPUTING_USER_LATEST_TRAITS_WORKSPACE_IDS", workspaceID)
		t.Setenv("RSERVER_WAREHOUSE_POSTGRES_ENABLE_SQLSTATEMENT_EXECUTION_PLAN_WORKSPACE_IDS", workspaceID)
		t.Setenv("RSERVER_WAREHOUSE_POSTGRES_ENABLE_DELETE_BY_JOBS", "true")
		t.Setenv("RSERVER_WAREHOUSE_POSTGRES_ENABLE_DELETE_BY_JOBS", "true")
		for envKey, envValue := range additionalEnvs {
			t.Setenv(envKey, envValue)
		}

		svcDone := make(chan struct{})

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			r := runner.New(runner.ReleaseInfo{})
			_ = r.Run(ctx, []string{"postgres-integration-test"})

			close(svcDone)
		}()
		t.Cleanup(func() { <-svcDone })
		t.Cleanup(cancel)

		serviceHealthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
		health.WaitUntilReady(ctx, t, serviceHealthEndpoint, time.Minute, time.Second, "serviceHealthEndpoint")
	}

	t.Run("Events flow", func(t *testing.T) {
		dsn := fmt.Sprintf(
			"postgres://%s:%s@%s:%s/%s?sslmode=disable",
			"rudder", "rudder-password", "localhost", strconv.Itoa(postgresPort), "rudderdb",
		)
		db, err := sql.Open("postgres", dsn)
		require.NoError(t, err)
		require.NoError(t, db.Ping())

		jobsDB := whth.JobsDB(t, jobsDBPort)

		testCases := []struct {
			name                  string
			writeKey              string
			schema                string
			sourceID              string
			destinationID         string
			tables                []string
			stagingFilesEventsMap whth.EventsCountMap
			loadFilesEventsMap    whth.EventsCountMap
			tableUploadsEventsMap whth.EventsCountMap
			warehouseEventsMap    whth.EventsCountMap
			warehouseEventsMap2   whth.EventsCountMap
			sourceJob             bool
			stagingFilePrefix     string
			preferAppend          *bool
			jobRunID              string
			useSameUserID         bool
			additionalEnvs        map[string]string
		}{
			{
				name:     "Upload Job",
				writeKey: writeKey,
				schema:   namespace,
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				sourceID:          sourceID,
				destinationID:     destinationID,
				stagingFilePrefix: "testdata/upload-job",
				jobRunID:          misc.FastUUID().String(),
			},
			{
				name:     "Append Mode",
				writeKey: writeKey,
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				schema:        namespace,
				sourceID:      sourceID,
				destinationID: destinationID,
				warehouseEventsMap2: whth.EventsCountMap{
					"identifies":    8,
					"users":         1,
					"tracks":        8,
					"product_track": 8,
					"pages":         8,
					"screens":       8,
					"aliases":       8,
					"groups":        8,
				},
				preferAppend:      th.Ptr(true),
				stagingFilePrefix: "testdata/upload-job-append-mode",
				// an empty jobRunID means that the source is not an ETL one
				// see Uploader.CanAppend()
				jobRunID:      "",
				useSameUserID: true,
			},
			{
				name:     "Undefined preferAppend",
				writeKey: writeKey,
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				schema:        namespace,
				sourceID:      sourceID,
				destinationID: destinationID,
				warehouseEventsMap2: whth.EventsCountMap{
					// let's use the same data as "testdata/upload-job-append-mode"
					// but then for the 2nd sync we expect 4 for each table instead of 8 due to the merge
					"identifies":    4,
					"users":         1,
					"tracks":        4,
					"product_track": 4,
					"pages":         4,
					"screens":       4,
					"aliases":       4,
					"groups":        4,
				},
				preferAppend:      nil, // not defined in backend config
				stagingFilePrefix: "testdata/upload-job-append-mode",
				// an empty jobRunID means that the source is not an ETL one
				// see Uploader.CanAppend()
				jobRunID:      "",
				useSameUserID: true,
			},
			{
				name:     "Append Users",
				writeKey: writeKey,
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				schema:        namespace,
				sourceID:      sourceID,
				destinationID: destinationID,
				warehouseEventsMap: whth.EventsCountMap{
					// In the first sync we get 4 events for each table, 1 for users
					"identifies":    4,
					"users":         1,
					"tracks":        4,
					"product_track": 4,
					"pages":         4,
					"screens":       4,
					"aliases":       4,
					"groups":        4,
				},
				warehouseEventsMap2: whth.EventsCountMap{
					// WARNING: the uploader.CanAppend() method will return false due to the jobRunID
					// We will still merge the other tables because of that but not the users table
					// and that is because of these settings:
					// * Warehouse.postgres.skipDedupDestinationIDs
					// * Warehouse.postgres.skipComputingUserLatestTraits
					// See hyperverge users use case
					"identifies":    4,
					"users":         2, // same data as "testdata/upload-job-append-mode" but we have to append users
					"tracks":        4,
					"product_track": 4,
					"pages":         4,
					"screens":       4,
					"aliases":       4,
					"groups":        4,
				},
				preferAppend:      th.Ptr(true),
				stagingFilePrefix: "testdata/upload-job-append-mode",
				// we set the jobRunID to make sure the uploader says we cannot append!
				// same behaviour as redshift, see hyperverge users use case
				jobRunID:      misc.FastUUID().String(),
				useSameUserID: true,
				additionalEnvs: map[string]string{
					"RSERVER_WAREHOUSE_POSTGRES_SKIP_DEDUP_DESTINATION_IDS":        destinationID,
					"RSERVER_WAREHOUSE_POSTGRES_SKIP_COMPUTING_USER_LATEST_TRAITS": "true",
				},
			},
			{
				name:                  "Source Job",
				writeKey:              sourcesWriteKey,
				schema:                sourcesNamespace,
				tables:                []string{"tracks", "google_sheet"},
				sourceID:              sourcesSourceID,
				destinationID:         sourcesDestinationID,
				stagingFilesEventsMap: whth.SourcesStagingFilesEventsMap(),
				loadFilesEventsMap:    whth.SourcesLoadFilesEventsMap(),
				tableUploadsEventsMap: whth.SourcesTableUploadsEventsMap(),
				warehouseEventsMap:    whth.SourcesWarehouseEventsMap(),
				warehouseEventsMap2: whth.EventsCountMap{
					"google_sheet": 8,
					"tracks":       8,
				},
				sourceJob:         true,
				stagingFilePrefix: "testdata/sources-job",
				jobRunID:          misc.FastUUID().String(),
			},
		}

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				bootstrapSvc(t, tc.additionalEnvs, tc.preferAppend)

				sqlClient := &client.Client{
					SQL:  db,
					Type: client.SQLClient,
				}

				conf := map[string]any{
					"bucketProvider":   "MINIO",
					"bucketName":       bucketName,
					"accessKeyID":      accessKeyID,
					"secretAccessKey":  secretAccessKey,
					"useSSL":           false,
					"endPoint":         minioEndpoint,
					"useRudderStorage": false,
				}

				t.Log("verifying test case 1")
				ts1 := whth.TestConfig{
					WriteKey:              tc.writeKey,
					Schema:                tc.schema,
					Tables:                tc.tables,
					SourceID:              tc.sourceID,
					DestinationID:         tc.destinationID,
					StagingFilesEventsMap: tc.stagingFilesEventsMap,
					LoadFilesEventsMap:    tc.loadFilesEventsMap,
					TableUploadsEventsMap: tc.tableUploadsEventsMap,
					WarehouseEventsMap:    tc.warehouseEventsMap,
					Config:                conf,
					WorkspaceID:           workspaceID,
					DestinationType:       destType,
					JobsDB:                jobsDB,
					HTTPPort:              httpPort,
					Client:                sqlClient,
					JobRunID:              tc.jobRunID,
					TaskRunID:             misc.FastUUID().String(),
					StagingFilePath:       tc.stagingFilePrefix + ".staging-1.json",
					UserID:                whth.GetUserId(destType),
				}
				ts1.VerifyEvents(t)

				t.Log("verifying test case 2")
				ts2 := whth.TestConfig{
					WriteKey:              tc.writeKey,
					Schema:                tc.schema,
					Tables:                tc.tables,
					SourceID:              tc.sourceID,
					DestinationID:         tc.destinationID,
					StagingFilesEventsMap: tc.stagingFilesEventsMap,
					LoadFilesEventsMap:    tc.loadFilesEventsMap,
					TableUploadsEventsMap: tc.tableUploadsEventsMap,
					WarehouseEventsMap:    tc.warehouseEventsMap2,
					SourceJob:             tc.sourceJob,
					Config:                conf,
					WorkspaceID:           workspaceID,
					DestinationType:       destType,
					JobsDB:                jobsDB,
					HTTPPort:              httpPort,
					Client:                sqlClient,
					JobRunID:              tc.jobRunID,
					TaskRunID:             misc.FastUUID().String(),
					StagingFilePath:       tc.stagingFilePrefix + ".staging-2.json",
					UserID:                whth.GetUserId(destType),
				}
				if tc.sourceJob || tc.useSameUserID {
					ts2.UserID = ts1.UserID
				}
				ts2.VerifyEvents(t)
			})
		}
	})

	t.Run("Events flow with ssh tunnel", func(t *testing.T) {
		bootstrapSvc(t, nil, nil)

		dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
			tunnelledUser,
			tunnelledPassword,
			tunnelledHost,
			tunnelledPort,
			tunnelledDatabase,
		)
		tunnelInfo := &tunnelling.TunnelInfo{
			Config: map[string]interface{}{
				"sshUser":       tunnelledSSHUser,
				"sshPort":       strconv.Itoa(sshPort),
				"sshHost":       tunnelledSSHHost,
				"sshPrivateKey": strings.ReplaceAll(tunnelledPrivateKey, "\\n", "\n"),
			},
		}

		db, err := tunnelling.Connect(dsn, tunnelInfo.Config)
		require.NoError(t, err)
		require.NoError(t, db.Ping())

		jobsDB := whth.JobsDB(t, jobsDBPort)

		testcases := []struct {
			name                  string
			writeKey              string
			schema                string
			sourceID              string
			destinationID         string
			tables                []string
			stagingFilesEventsMap whth.EventsCountMap
			loadFilesEventsMap    whth.EventsCountMap
			tableUploadsEventsMap whth.EventsCountMap
			warehouseEventsMap    whth.EventsCountMap
			stagingFilePrefix     string
		}{
			{
				name:     "upload job through ssh tunnelling",
				writeKey: tunnelledWriteKey,
				schema:   tunnelledNamespace,
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				sourceID:          tunnelledSourceID,
				destinationID:     tunnelledDestinationID,
				stagingFilePrefix: "testdata/upload-ssh-job",
			},
		}

		for _, tc := range testcases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				sqlClient := &client.Client{
					SQL:  db,
					Type: client.SQLClient,
				}

				conf := map[string]interface{}{
					"bucketProvider":   "MINIO",
					"bucketName":       bucketName,
					"accessKeyID":      accessKeyID,
					"secretAccessKey":  secretAccessKey,
					"useSSL":           false,
					"endPoint":         minioEndpoint,
					"useRudderStorage": false,
				}

				t.Log("verifying test case 1")
				ts1 := whth.TestConfig{
					WriteKey:              tc.writeKey,
					Schema:                tc.schema,
					SourceID:              tc.sourceID,
					DestinationID:         tc.destinationID,
					Tables:                tc.tables,
					StagingFilesEventsMap: tc.stagingFilesEventsMap,
					LoadFilesEventsMap:    tc.loadFilesEventsMap,
					TableUploadsEventsMap: tc.tableUploadsEventsMap,
					WarehouseEventsMap:    tc.warehouseEventsMap,
					Config:                conf,
					WorkspaceID:           workspaceID,
					DestinationType:       destType,
					JobsDB:                jobsDB,
					HTTPPort:              httpPort,
					Client:                sqlClient,
					JobRunID:              misc.FastUUID().String(),
					TaskRunID:             misc.FastUUID().String(),
					StagingFilePath:       tc.stagingFilePrefix + ".staging-1.json",
					UserID:                whth.GetUserId(destType),
				}
				ts1.VerifyEvents(t)

				t.Log("verifying test case 2")
				ts2 := whth.TestConfig{
					WriteKey:              tc.writeKey,
					Schema:                tc.schema,
					SourceID:              tc.sourceID,
					DestinationID:         tc.destinationID,
					Tables:                tc.tables,
					StagingFilesEventsMap: tc.stagingFilesEventsMap,
					LoadFilesEventsMap:    tc.loadFilesEventsMap,
					TableUploadsEventsMap: tc.tableUploadsEventsMap,
					WarehouseEventsMap:    tc.warehouseEventsMap,
					Config:                conf,
					WorkspaceID:           workspaceID,
					DestinationType:       destType,
					JobsDB:                jobsDB,
					HTTPPort:              httpPort,
					Client:                sqlClient,
					JobRunID:              misc.FastUUID().String(),
					TaskRunID:             misc.FastUUID().String(),
					StagingFilePath:       tc.stagingFilePrefix + ".staging-2.json",
					UserID:                whth.GetUserId(destType),
				}
				ts2.VerifyEvents(t)
			})
		}
	})

	t.Run("Validations", func(t *testing.T) {
		dest := backendconfig.DestinationT{
			ID: destinationID,
			Config: map[string]interface{}{
				"host":             host,
				"database":         database,
				"user":             user,
				"password":         password,
				"port":             strconv.Itoa(postgresPort),
				"sslMode":          "disable",
				"namespace":        "",
				"bucketProvider":   "MINIO",
				"bucketName":       bucketName,
				"accessKeyID":      accessKeyID,
				"secretAccessKey":  secretAccessKey,
				"useSSL":           false,
				"endPoint":         minioEndpoint,
				"syncFrequency":    "30",
				"useRudderStorage": false,
			},
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				ID:          "1bJ4YC7INdkvBTzotNh0zta5jDm",
				Name:        "POSTGRES",
				DisplayName: "Postgres",
			},
			Name:       "postgres-demo",
			Enabled:    true,
			RevisionID: "29eeuu9kywWsRAybaXcxcnTVEl8",
		}
		whth.VerifyConfigurationTest(t, dest)
	})

	t.Run("Load Table", func(t *testing.T) {
		const (
			sourceID      = "test_source_id"
			destinationID = "test_destination_id"
			workspaceID   = "test_workspace_id"
		)

		namespace := whth.RandSchema(destType)

		schemaInUpload := model.TableSchema{
			"test_bool":     "boolean",
			"test_datetime": "datetime",
			"test_float":    "float",
			"test_int":      "int",
			"test_string":   "string",
			"id":            "string",
			"received_at":   "datetime",
		}
		schemaInWarehouse := model.TableSchema{
			"test_bool":           "boolean",
			"test_datetime":       "datetime",
			"test_float":          "float",
			"test_int":            "int",
			"test_string":         "string",
			"id":                  "string",
			"received_at":         "datetime",
			"extra_test_bool":     "boolean",
			"extra_test_datetime": "datetime",
			"extra_test_float":    "float",
			"extra_test_int":      "int",
			"extra_test_string":   "string",
		}

		warehouse := model.Warehouse{
			Source: backendconfig.SourceT{
				ID: sourceID,
			},
			Destination: backendconfig.DestinationT{
				ID: destinationID,
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: destType,
				},
				Config: map[string]any{
					"host":             host,
					"database":         database,
					"user":             user,
					"password":         password,
					"port":             strconv.Itoa(postgresPort),
					"sslMode":          "disable",
					"namespace":        "",
					"bucketProvider":   "MINIO",
					"bucketName":       bucketName,
					"accessKeyID":      accessKeyID,
					"secretAccessKey":  secretAccessKey,
					"useSSL":           false,
					"endPoint":         minioEndpoint,
					"syncFrequency":    "30",
					"useRudderStorage": false,
				},
			},
			WorkspaceID: workspaceID,
			Namespace:   namespace,
		}

		fm, err := filemanager.New(&filemanager.Settings{
			Provider: warehouseutils.MINIO,
			Config: map[string]any{
				"bucketName":       bucketName,
				"accessKeyID":      accessKeyID,
				"secretAccessKey":  secretAccessKey,
				"endPoint":         minioEndpoint,
				"forcePathStyle":   true,
				"s3ForcePathStyle": true,
				"disableSSL":       true,
				"region":           region,
				"enableSSE":        false,
				"bucketProvider":   warehouseutils.MINIO,
			},
		})
		require.NoError(t, err)

		t.Run("schema does not exists", func(t *testing.T) {
			ctx := context.Background()
			tableName := "schema_not_exists_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := mockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			pg := postgres.New(config.New(), logger.NOP, stats.NOP)
			err := pg.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			loadTableStat, err := pg.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("table does not exists", func(t *testing.T) {
			ctx := context.Background()
			tableName := "table_not_exists_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := mockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			pg := postgres.New(config.New(), logger.NOP, stats.NOP)
			err := pg.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = pg.CreateSchema(ctx)
			require.NoError(t, err)

			loadTableStat, err := pg.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("merge", func(t *testing.T) {
			t.Run("without dedup", func(t *testing.T) {
				ctx := context.Background()
				tableName := "merge_without_dedup_test_table"
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

				loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := mockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

				c := config.New()
				c.Set("Warehouse.postgres.EnableSQLStatementExecutionPlanWorkspaceIDs", workspaceID)

				appendWarehouse := th.Clone(t, warehouse)
				appendWarehouse.Destination.Config[model.PreferAppendSetting.String()] = true

				pg := postgres.New(c, logger.NOP, stats.NOP)
				err := pg.Setup(ctx, appendWarehouse, mockUploader)
				require.NoError(t, err)

				err = pg.CreateSchema(ctx)
				require.NoError(t, err)

				err = pg.CreateTable(ctx, tableName, schemaInWarehouse)
				require.NoError(t, err)

				loadTableStat, err := pg.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(14))
				require.Equal(t, loadTableStat.RowsUpdated, int64(0))

				loadTableStat, err = pg.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(14))
				require.Equal(t, loadTableStat.RowsUpdated, int64(0))

				records := whth.RetrieveRecordsFromWarehouse(t, pg.DB.DB,
					fmt.Sprintf(`
					SELECT
					  id,
					  received_at,
					  test_bool,
					  test_datetime,
					  test_float,
					  test_int,
					  test_string
					FROM
					  %q.%q
					ORDER BY
					  id;
					`,
						namespace,
						tableName,
					),
				)
				require.Equal(t, records, whth.AppendTestRecords())
			})
			t.Run("with dedup", func(t *testing.T) {
				ctx := context.Background()
				tableName := "merge_with_dedup_test_table"
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/dedup.csv.gz", tableName)

				loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := mockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

				c := config.New()
				c.Set("Warehouse.postgres.EnableSQLStatementExecutionPlanWorkspaceIDs", workspaceID)

				pg := postgres.New(config.New(), logger.NOP, stats.NOP)
				err := pg.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				err = pg.CreateSchema(ctx)
				require.NoError(t, err)

				err = pg.CreateTable(ctx, tableName, schemaInWarehouse)
				require.NoError(t, err)

				loadTableStat, err := pg.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(14))
				require.Equal(t, loadTableStat.RowsUpdated, int64(0))

				loadTableStat, err = pg.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(0))
				require.Equal(t, loadTableStat.RowsUpdated, int64(14))

				records := whth.RetrieveRecordsFromWarehouse(t, pg.DB.DB,
					fmt.Sprintf(`
					SELECT
					  id,
					  received_at,
					  test_bool,
					  test_datetime,
					  test_float,
					  test_int,
					  test_string
					FROM
					  %q.%q
					ORDER BY
					  id;
					`,
						namespace,
						tableName,
					),
				)
				require.Equal(t, records, whth.DedupTestRecords())
			})
		})
		t.Run("append", func(t *testing.T) {
			ctx := context.Background()
			tableName := "append_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := mockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			c := config.New()
			c.Set("Warehouse.postgres.skipDedupDestinationIDs", destinationID)

			appendWarehouse := th.Clone(t, warehouse)
			appendWarehouse.Destination.Config[model.PreferAppendSetting.String()] = true

			pg := postgres.New(c, logger.NOP, stats.NOP)
			err := pg.Setup(ctx, appendWarehouse, mockUploader)
			require.NoError(t, err)

			err = pg.CreateSchema(ctx)
			require.NoError(t, err)

			err = pg.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := pg.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			loadTableStat, err = pg.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := whth.RetrieveRecordsFromWarehouse(t, pg.DB.DB,
				fmt.Sprintf(`
					SELECT
					  id,
					  received_at,
					  test_bool,
					  test_datetime,
					  test_float,
					  test_int,
					  test_string
					FROM
					  %q.%q
					ORDER BY
					  id;
					`,
					namespace,
					tableName,
				),
			)
			require.Equal(t, records, whth.AppendTestRecords())
		})
		t.Run("load file does not exists", func(t *testing.T) {
			ctx := context.Background()
			tableName := "load_file_not_exists_test_table"

			loadFiles := []warehouseutils.LoadFile{{
				Location: "http://localhost:1234/testbucket/rudder-warehouse-load-objects/load_file_not_exists_test_table/test_source_id/f31af97e-03e8-46d0-8a1a-1786cb85b22c-load_file_not_exists_test_table/load.csv.gz",
			}}
			mockUploader := mockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			pg := postgres.New(config.New(), logger.NOP, stats.NOP)
			err := pg.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = pg.CreateSchema(ctx)
			require.NoError(t, err)

			err = pg.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := pg.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("mismatch in number of columns", func(t *testing.T) {
			ctx := context.Background()
			tableName := "mismatch_columns_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/mismatch-columns.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := mockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			pg := postgres.New(config.New(), logger.NOP, stats.NOP)
			err := pg.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = pg.CreateSchema(ctx)
			require.NoError(t, err)

			err = pg.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := pg.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("mismatch in schema", func(t *testing.T) {
			ctx := context.Background()
			tableName := "mismatch_schema_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/mismatch-schema.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := mockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			pg := postgres.New(config.New(), logger.NOP, stats.NOP)
			err := pg.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = pg.CreateSchema(ctx)
			require.NoError(t, err)

			err = pg.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := pg.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("discards", func(t *testing.T) {
			ctx := context.Background()
			tableName := warehouseutils.DiscardsTable

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/discards.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := mockUploader(t, loadFiles, tableName, warehouseutils.DiscardsSchema, warehouseutils.DiscardsSchema)

			pg := postgres.New(config.New(), logger.NOP, stats.NOP)
			err := pg.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = pg.CreateSchema(ctx)
			require.NoError(t, err)

			err = pg.CreateTable(ctx, tableName, warehouseutils.DiscardsSchema)
			require.NoError(t, err)

			loadTableStat, err := pg.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(6))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := whth.RetrieveRecordsFromWarehouse(t, pg.DB.DB,
				fmt.Sprintf(`
					SELECT
					  column_name,
					  column_value,
					  received_at,
					  row_id,
					  table_name,
					  uuid_ts
					FROM
					  %q.%q
					ORDER BY row_id ASC;
					`,
					namespace,
					tableName,
				),
			)
			require.Equal(t, records, whth.DiscardTestRecords())
		})
	})
}

func mockUploader(
	t testing.TB,
	loadFiles []warehouseutils.LoadFile,
	tableName string,
	schemaInUpload model.TableSchema,
	schemaInWarehouse model.TableSchema,
) warehouseutils.Uploader {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockUploader := mockuploader.NewMockUploader(ctrl)
	mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()
	mockUploader.EXPECT().GetLoadFilesMetadata(gomock.Any(), gomock.Any()).Return(loadFiles, nil).AnyTimes() // Try removing this
	mockUploader.EXPECT().GetTableSchemaInUpload(tableName).Return(schemaInUpload).AnyTimes()
	mockUploader.EXPECT().GetTableSchemaInWarehouse(tableName).Return(schemaInWarehouse).AnyTimes()
	mockUploader.EXPECT().CanAppend().Return(true).AnyTimes()

	return mockUploader
}
