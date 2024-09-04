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

	"github.com/lib/pq"

	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	th "github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/postgres"
	whth "github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/tunnelling"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}

	misc.Init()
	validations.Init()
	whutils.Init()

	destType := whutils.POSTGRES

	host := "localhost"
	database := "rudderdb"
	user := "rudder"
	password := "rudder-password"
	bucketName := "testbucket"
	accessKeyID := "MYACCESSKEY"
	secretAccessKey := "MYSECRETKEY"
	region := "us-east-1"

	t.Run("Events flow", func(t *testing.T) {
		httpPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.postgres.yml", "../testdata/docker-compose.jobsdb.yml", "../testdata/docker-compose.minio.yml"}))
		c.Start(context.Background())

		workspaceID := whutils.RandHex()
		jobsDBPort := c.Port("jobsDb", 5432)
		postgresPort := c.Port("postgres", 5432)
		minioEndpoint := fmt.Sprintf("localhost:%d", c.Port("minio", 9000))

		jobsDB := whth.JobsDB(t, jobsDBPort)

		testCases := []struct {
			name                  string
			tables                []string
			stagingFilesEventsMap whth.EventsCountMap
			loadFilesEventsMap    whth.EventsCountMap
			tableUploadsEventsMap whth.EventsCountMap
			warehouseEventsMap    whth.EventsCountMap
			warehouseEventsMap2   whth.EventsCountMap
			sourceJob             bool
			stagingFilePrefix     string
			jobRunID              string
			useSameUserID         bool
			additionalEnvs        func(destinationID string) map[string]string
			configOverride        map[string]any
		}{
			{
				name: "Upload Job",
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				stagingFilePrefix: "testdata/upload-job",
				jobRunID:          misc.FastUUID().String(),
			},
			{
				name: "Append Mode",
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
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
				configOverride: map[string]any{
					"preferAppend": true,
				},
				stagingFilePrefix: "testdata/upload-job-append-mode",
				// an empty jobRunID means that the source is not an ETL one
				// see Uploader.CanAppend()
				jobRunID:      "",
				useSameUserID: true,
			},
			{
				name: "Undefined preferAppend",
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
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
				stagingFilePrefix: "testdata/upload-job-append-mode",
				// an empty jobRunID means that the source is not an ETL one
				// see Uploader.CanAppend()
				jobRunID:      "",
				useSameUserID: true,
			},
			{
				name: "Append Users",
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
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
				configOverride: map[string]any{
					"preferAppend": true,
				},
				stagingFilePrefix: "testdata/upload-job-append-mode",
				// we set the jobRunID to make sure the uploader says we cannot append!
				// same behaviour as redshift, see hyperverge users use case
				jobRunID:      misc.FastUUID().String(),
				useSameUserID: true,
				additionalEnvs: func(destinationID string) map[string]string {
					return map[string]string{
						"RSERVER_WAREHOUSE_POSTGRES_SKIP_DEDUP_DESTINATION_IDS":        destinationID,
						"RSERVER_WAREHOUSE_POSTGRES_SKIP_COMPUTING_USER_LATEST_TRAITS": "true",
					}
				},
			},
			{
				name:                  "Source Job",
				tables:                []string{"tracks", "google_sheet"},
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
			t.Run(tc.name, func(t *testing.T) {
				var (
					sourceID      = whutils.RandHex()
					destinationID = whutils.RandHex()
					writeKey      = whutils.RandHex()
					namespace     = whth.RandSchema(destType)
				)

				destinationBuilder := backendconfigtest.NewDestinationBuilder(destType).
					WithID(destinationID).
					WithRevisionID(destinationID).
					WithConfigOption("host", host).
					WithConfigOption("database", database).
					WithConfigOption("user", user).
					WithConfigOption("password", password).
					WithConfigOption("port", strconv.Itoa(postgresPort)).
					WithConfigOption("sslMode", "disable").
					WithConfigOption("namespace", namespace).
					WithConfigOption("bucketProvider", whutils.MINIO).
					WithConfigOption("bucketName", bucketName).
					WithConfigOption("accessKeyID", accessKeyID).
					WithConfigOption("secretAccessKey", secretAccessKey).
					WithConfigOption("useSSL", false).
					WithConfigOption("endPoint", minioEndpoint).
					WithConfigOption("useRudderStorage", false).
					WithConfigOption("syncFrequency", "30")
				for k, v := range tc.configOverride {
					destinationBuilder = destinationBuilder.WithConfigOption(k, v)
				}

				workspaceConfig := backendconfigtest.NewConfigBuilder().
					WithSource(
						backendconfigtest.NewSourceBuilder().
							WithID(sourceID).
							WithWriteKey(writeKey).
							WithWorkspaceID(workspaceID).
							WithConnection(destinationBuilder.Build()).
							Build(),
					).
					WithWorkspaceID(workspaceID).
					Build()

				t.Setenv("RSERVER_WAREHOUSE_POSTGRES_MAX_PARALLEL_LOADS", "8")
				t.Setenv("RSERVER_WAREHOUSE_POSTGRES_SKIP_COMPUTING_USER_LATEST_TRAITS_WORKSPACE_IDS", workspaceID)
				t.Setenv("RSERVER_WAREHOUSE_POSTGRES_ENABLE_SQLSTATEMENT_EXECUTION_PLAN_WORKSPACE_IDS", workspaceID)
				t.Setenv("RSERVER_WAREHOUSE_POSTGRES_ENABLE_DELETE_BY_JOBS", "true")
				t.Setenv("RSERVER_WAREHOUSE_POSTGRES_ENABLE_DELETE_BY_JOBS", "true")
				if tc.additionalEnvs != nil {
					for envKey, envValue := range tc.additionalEnvs(destinationID) {
						t.Setenv(envKey, envValue)
					}
				}

				whth.BootstrapSvc(t, workspaceConfig, httpPort, jobsDBPort)

				dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
					user, password, host, strconv.Itoa(postgresPort), database,
				)
				db, err := sql.Open("postgres", dsn)
				require.NoError(t, err)
				require.NoError(t, db.Ping())
				t.Cleanup(func() {
					_ = db.Close()
				})

				sqlClient := &client.Client{
					SQL:  db,
					Type: client.SQLClient,
				}

				conf := map[string]any{
					"bucketProvider":   whutils.MINIO,
					"bucketName":       bucketName,
					"accessKeyID":      accessKeyID,
					"secretAccessKey":  secretAccessKey,
					"useSSL":           false,
					"endPoint":         minioEndpoint,
					"useRudderStorage": false,
				}

				t.Log("verifying test case 1")
				ts1 := whth.TestConfig{
					WriteKey:              writeKey,
					Schema:                namespace,
					Tables:                tc.tables,
					SourceID:              sourceID,
					DestinationID:         destinationID,
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
					WriteKey:              writeKey,
					Schema:                namespace,
					Tables:                tc.tables,
					SourceID:              sourceID,
					DestinationID:         destinationID,
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

	t.Run("Events flow with SSH Tunnel", func(t *testing.T) {
		httpPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.ssh-server.yml", "../testdata/docker-compose.jobsdb.yml", "../testdata/docker-compose.minio.yml"}))
		c.Start(context.Background())

		workspaceID := whutils.RandHex()
		jobsDBPort := c.Port("jobsDb", 5432)
		sshPort := c.Port("ssh-server", 2222)
		minioEndpoint := fmt.Sprintf("localhost:%d", c.Port("minio", 9000))

		tunnelledHost := "db-private-postgres"
		tunnelledDatabase := "postgres"
		tunnelledPassword := "postgres"
		tunnelledUser := "postgres"
		tunnelledPort := "5432"
		tunnelledSSHUser := "rudderstack"
		tunnelledSSHHost := "localhost"
		tunnelledPrivateKey := "-----BEGIN OPENSSH PRIVATE KEY-----\\n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\\nbG9jYWwBAgMEBQYH\\n-----END OPENSSH PRIVATE KEY-----"

		jobsDB := whth.JobsDB(t, jobsDBPort)

		testcases := []struct {
			name                  string
			tables                []string
			stagingFilesEventsMap whth.EventsCountMap
			loadFilesEventsMap    whth.EventsCountMap
			tableUploadsEventsMap whth.EventsCountMap
			warehouseEventsMap    whth.EventsCountMap
			stagingFilePrefix     string
		}{
			{
				name: "upload job through ssh tunnelling",
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				stagingFilePrefix: "testdata/upload-ssh-job",
			},
		}

		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				var (
					sourceID      = whutils.RandHex()
					destinationID = whutils.RandHex()
					writeKey      = whutils.RandHex()
					namespace     = whth.RandSchema(destType)
				)

				destinationBuilder := backendconfigtest.NewDestinationBuilder(destType).
					WithID(destinationID).
					WithRevisionID(destinationID).
					WithConfigOption("host", tunnelledHost).
					WithConfigOption("database", tunnelledDatabase).
					WithConfigOption("user", tunnelledUser).
					WithConfigOption("password", tunnelledPassword).
					WithConfigOption("port", tunnelledPort).
					WithConfigOption("sslMode", "disable").
					WithConfigOption("namespace", namespace).
					WithConfigOption("bucketProvider", whutils.MINIO).
					WithConfigOption("bucketName", bucketName).
					WithConfigOption("accessKeyID", accessKeyID).
					WithConfigOption("secretAccessKey", secretAccessKey).
					WithConfigOption("useSSH", true).
					WithConfigOption("useSSL", false).
					WithConfigOption("endPoint", minioEndpoint).
					WithConfigOption("useRudderStorage", false).
					WithConfigOption("syncFrequency", "30").
					WithConfigOption("sshUser", tunnelledSSHUser).
					WithConfigOption("sshHost", tunnelledSSHHost).
					WithConfigOption("sshPort", strconv.Itoa(sshPort)).
					WithConfigOption("sshPrivateKey", strings.ReplaceAll(tunnelledPrivateKey, "\\n", "\n"))

				workspaceConfig := backendconfigtest.NewConfigBuilder().
					WithSource(
						backendconfigtest.NewSourceBuilder().
							WithID(sourceID).
							WithWriteKey(writeKey).
							WithWorkspaceID(workspaceID).
							WithConnection(destinationBuilder.Build()).
							Build(),
					).
					WithWorkspaceID(workspaceID).
					Build()

				t.Setenv("RSERVER_WAREHOUSE_POSTGRES_MAX_PARALLEL_LOADS", "8")
				t.Setenv("RSERVER_WAREHOUSE_POSTGRES_SKIP_COMPUTING_USER_LATEST_TRAITS_WORKSPACE_IDS", workspaceID)
				t.Setenv("RSERVER_WAREHOUSE_POSTGRES_ENABLE_SQLSTATEMENT_EXECUTION_PLAN_WORKSPACE_IDS", workspaceID)
				t.Setenv("RSERVER_WAREHOUSE_POSTGRES_ENABLE_DELETE_BY_JOBS", "true")
				t.Setenv("RSERVER_WAREHOUSE_POSTGRES_ENABLE_DELETE_BY_JOBS", "true")

				whth.BootstrapSvc(t, workspaceConfig, httpPort, jobsDBPort)

				dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
					tunnelledUser, tunnelledPassword, tunnelledHost, tunnelledPort, tunnelledDatabase,
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
				t.Cleanup(func() {
					_ = db.Close()
				})

				sqlClient := &client.Client{
					SQL:  db,
					Type: client.SQLClient,
				}

				conf := map[string]interface{}{
					"bucketProvider":   whutils.MINIO,
					"bucketName":       bucketName,
					"accessKeyID":      accessKeyID,
					"secretAccessKey":  secretAccessKey,
					"useSSL":           false,
					"endPoint":         minioEndpoint,
					"useRudderStorage": false,
				}

				t.Log("verifying test case 1")
				ts1 := whth.TestConfig{
					WriteKey:              writeKey,
					Schema:                namespace,
					SourceID:              sourceID,
					DestinationID:         destinationID,
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
					WriteKey:              writeKey,
					Schema:                namespace,
					SourceID:              sourceID,
					DestinationID:         destinationID,
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
		c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.postgres.yml", "../testdata/docker-compose.minio.yml"}))
		c.Start(context.Background())

		postgresPort := c.Port("postgres", 5432)
		minioEndpoint := fmt.Sprintf("localhost:%d", c.Port("minio", 9000))

		dest := backendconfig.DestinationT{
			ID: "test_destination_id",
			Config: map[string]interface{}{
				"host":             host,
				"database":         database,
				"user":             user,
				"password":         password,
				"port":             strconv.Itoa(postgresPort),
				"sslMode":          "disable",
				"namespace":        "",
				"bucketProvider":   whutils.MINIO,
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
		c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.postgres.yml", "../testdata/docker-compose.minio.yml"}))
		c.Start(context.Background())

		postgresPort := c.Port("postgres", 5432)
		minioEndpoint := fmt.Sprintf("localhost:%d", c.Port("minio", 9000))

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
				ID: "test_source_id",
			},
			Destination: backendconfig.DestinationT{
				ID: "test_destination_id",
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
					"bucketProvider":   whutils.MINIO,
					"bucketName":       bucketName,
					"accessKeyID":      accessKeyID,
					"secretAccessKey":  secretAccessKey,
					"useSSL":           false,
					"endPoint":         minioEndpoint,
					"syncFrequency":    "30",
					"useRudderStorage": false,
				},
			},
			WorkspaceID: "test_workspace_id",
			Namespace:   namespace,
		}

		fm, err := filemanager.New(&filemanager.Settings{
			Provider: whutils.MINIO,
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
				"bucketProvider":   whutils.MINIO,
			},
		})
		require.NoError(t, err)

		t.Run("schema does not exists", func(t *testing.T) {
			ctx := context.Background()
			tableName := "schema_not_exists_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
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

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
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

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := mockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

				c := config.New()
				c.Set("Warehouse.postgres.EnableSQLStatementExecutionPlanWorkspaceIDs", "test_workspace_id")

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

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := mockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

				c := config.New()
				c.Set("Warehouse.postgres.EnableSQLStatementExecutionPlanWorkspaceIDs", "test_workspace_id")

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

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := mockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			c := config.New()
			c.Set("Warehouse.postgres.skipDedupDestinationIDs", "test_destination_id")

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

			loadFiles := []whutils.LoadFile{{
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

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
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

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
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
			tableName := whutils.DiscardsTable

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/discards.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := mockUploader(t, loadFiles, tableName, whutils.DiscardsSchema, whutils.DiscardsSchema)

			pg := postgres.New(config.New(), logger.NOP, stats.NOP)
			err := pg.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = pg.CreateSchema(ctx)
			require.NoError(t, err)

			err = pg.CreateTable(ctx, tableName, whutils.DiscardsSchema)
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

	t.Run("Logical Replication", func(t *testing.T) {
		c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.replication.yml", "../testdata/docker-compose.minio.yml"}))
		c.Start(context.Background())

		primaryDBPort := c.Port("primary", 5432)
		standbyDBPort := c.Port("standby", 5432)
		minioEndpoint := fmt.Sprintf("localhost:%d", c.Port("minio", 9000))

		namespace := whth.RandSchema(destType)

		warehouse := model.Warehouse{
			Source: backendconfig.SourceT{
				ID: "test_source_id",
			},
			Destination: backendconfig.DestinationT{
				ID: "test_destination_id",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: destType,
				},
				Config: map[string]any{
					"host":             host,
					"database":         database,
					"user":             user,
					"password":         password,
					"port":             strconv.Itoa(primaryDBPort),
					"sslMode":          "disable",
					"namespace":        "",
					"bucketProvider":   whutils.MINIO,
					"bucketName":       bucketName,
					"accessKeyID":      accessKeyID,
					"secretAccessKey":  secretAccessKey,
					"useSSL":           false,
					"endPoint":         minioEndpoint,
					"syncFrequency":    "30",
					"useRudderStorage": false,
				},
			},
			WorkspaceID: "test_workspace_id",
			Namespace:   namespace,
		}

		primaryWarehouse := th.Clone(t, warehouse)
		primaryWarehouse.Destination.Config["port"] = strconv.Itoa(primaryDBPort)
		standByWarehouse := th.Clone(t, warehouse)
		standByWarehouse.Destination.Config["port"] = strconv.Itoa(standbyDBPort)

		fm, err := filemanager.New(&filemanager.Settings{
			Provider: whutils.MINIO,
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
				"bucketProvider":   whutils.MINIO,
			},
		})
		require.NoError(t, err)

		primaryDSN := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
			user, password, host, strconv.Itoa(primaryDBPort), database,
		)
		primaryDB, err := sql.Open("postgres", primaryDSN)
		require.NoError(t, err)
		require.NoError(t, primaryDB.Ping())
		t.Cleanup(func() {
			_ = primaryDB.Close()
		})
		standByDSN := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
			user, password, host, strconv.Itoa(standbyDBPort), database,
		)
		standByDB, err := sql.Open("postgres", standByDSN)
		require.NoError(t, err)
		require.NoError(t, standByDB.Ping())
		t.Cleanup(func() {
			_ = standByDB.Close()
		})

		t.Run("Regular table", func(t *testing.T) {
			ctx := context.Background()
			tableName := "replication_table"
			expectedCount := 14

			replicationTableSchema := model.TableSchema{
				"test_bool":     "boolean",
				"test_datetime": "datetime",
				"test_float":    "float",
				"test_int":      "int",
				"test_string":   "string",
				"id":            "string",
				"received_at":   "datetime",
			}

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := mockUploader(t, loadFiles, tableName, replicationTableSchema, replicationTableSchema)

			primaryPG := postgres.New(config.New(), logger.NOP, stats.NOP)
			require.NoError(t, primaryPG.Setup(ctx, primaryWarehouse, mockUploader))
			require.NoError(t, primaryPG.CreateSchema(ctx))
			require.NoError(t, primaryPG.CreateTable(ctx, tableName, replicationTableSchema))
			standByPG := postgres.New(config.New(), logger.NOP, stats.NOP)
			require.NoError(t, standByPG.Setup(ctx, standByWarehouse, mockUploader))
			require.NoError(t, standByPG.CreateSchema(ctx))
			require.NoError(t, standByPG.CreateTable(ctx, tableName, replicationTableSchema))

			// Creating publication and subscription
			_, err = primaryDB.ExecContext(ctx, fmt.Sprintf("CREATE PUBLICATION regular_publication FOR TABLE %s.%s;", namespace, tableName))
			require.NoError(t, err)
			_, err = standByDB.ExecContext(ctx, fmt.Sprintf("CREATE SUBSCRIPTION regular_subscription CONNECTION 'host=primary port=5432 user=%s password=%s dbname=%s' PUBLICATION regular_publication;", user, password, database))
			require.NoError(t, err)

			// Loading data should fail because of the missing primary key
			_, err = primaryPG.LoadTable(ctx, tableName)
			require.Error(t, err)
			var pgErr *pq.Error
			require.ErrorAs(t, err, &pgErr)
			require.EqualValues(t, pq.ErrorCode("55000"), pgErr.Code)

			// Adding primary key
			_, err = primaryDB.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %s.%s ADD PRIMARY KEY ("id");`, namespace, tableName))
			require.NoError(t, err)

			// Loading data should work now
			_, err = primaryPG.LoadTable(ctx, tableName)
			require.NoError(t, err)

			// Checking the number of rows in both primary and standby databases
			var (
				countQuery = fmt.Sprintf("SELECT COUNT(*) FROM %s.%s;", namespace, tableName)
				count      int
			)
			require.Eventually(t, func() bool {
				err := primaryDB.QueryRowContext(ctx, countQuery).Scan(&count)
				if err != nil {
					t.Logf("Error while querying primary database: %v", err)
					return false
				}
				if count != expectedCount {
					t.Logf("Expected %d rows in primary database, got %d", expectedCount, count)
					return false
				}
				return true
			},
				10*time.Second,
				100*time.Millisecond,
			)
			require.Eventually(t, func() bool {
				err := standByDB.QueryRowContext(ctx, countQuery).Scan(&count)
				if err != nil {
					t.Logf("Error while querying standby database: %v", err)
					return false
				}
				if count != expectedCount {
					t.Logf("Expected %d rows in standby database, got %d", expectedCount, count)
					return false
				}
				return true
			},
				10*time.Second,
				100*time.Millisecond,
			)
		})
		t.Run("Users table", func(t *testing.T) {
			ctx := context.Background()
			expectedCount := 14

			IdentifiesTableSchema := model.TableSchema{
				"test_bool":     "boolean",
				"test_datetime": "datetime",
				"test_float":    "float",
				"test_int":      "int",
				"test_string":   "string",
				"id":            "string",
				"received_at":   "datetime",
				"user_id":       "string",
			}
			usersTableSchema := model.TableSchema{
				"test_bool":     "boolean",
				"test_datetime": "datetime",
				"test_float":    "float",
				"test_int":      "int",
				"test_string":   "string",
				"id":            "string",
				"received_at":   "datetime",
			}

			usersUploadOutput := whth.UploadLoadFile(t, fm, "testdata/users.csv.gz", whutils.UsersTable)
			identifiesUploadOutput := whth.UploadLoadFile(t, fm, "testdata/identifies.csv.gz", whutils.IdentifiesTable)

			ctrl := gomock.NewController(t)
			mockUploader := mockuploader.NewMockUploader(ctrl)
			mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()
			mockUploader.EXPECT().GetLoadFilesMetadata(gomock.Any(), whutils.GetLoadFilesOptions{Table: whutils.UsersTable}).Return([]whutils.LoadFile{{Location: usersUploadOutput.Location}}, nil).AnyTimes()
			mockUploader.EXPECT().GetLoadFilesMetadata(gomock.Any(), whutils.GetLoadFilesOptions{Table: whutils.IdentifiesTable}).Return([]whutils.LoadFile{{Location: identifiesUploadOutput.Location}}, nil).AnyTimes()
			mockUploader.EXPECT().GetTableSchemaInUpload(whutils.UsersTable).Return(usersTableSchema).AnyTimes()
			mockUploader.EXPECT().GetTableSchemaInUpload(whutils.IdentifiesTable).Return(IdentifiesTableSchema).AnyTimes()
			mockUploader.EXPECT().GetTableSchemaInWarehouse(whutils.UsersTable).Return(usersTableSchema).AnyTimes()
			mockUploader.EXPECT().GetTableSchemaInWarehouse(whutils.IdentifiesTable).Return(IdentifiesTableSchema).AnyTimes()
			mockUploader.EXPECT().CanAppend().Return(true).AnyTimes()

			primaryPG := postgres.New(config.New(), logger.NOP, stats.NOP)
			require.NoError(t, primaryPG.Setup(ctx, primaryWarehouse, mockUploader))
			require.NoError(t, primaryPG.CreateSchema(ctx))
			require.NoError(t, primaryPG.CreateTable(ctx, whutils.IdentifiesTable, IdentifiesTableSchema))
			require.NoError(t, primaryPG.CreateTable(ctx, whutils.UsersTable, usersTableSchema))
			standByPG := postgres.New(config.New(), logger.NOP, stats.NOP)
			require.NoError(t, standByPG.Setup(ctx, standByWarehouse, mockUploader))
			require.NoError(t, standByPG.CreateSchema(ctx))
			require.NoError(t, standByPG.CreateTable(ctx, whutils.IdentifiesTable, IdentifiesTableSchema))
			require.NoError(t, standByPG.CreateTable(ctx, whutils.UsersTable, usersTableSchema))

			// Creating publication and subscription
			_, err = primaryDB.ExecContext(ctx, fmt.Sprintf("CREATE PUBLICATION users_publication FOR TABLE %[1]s.%[2]s, %[1]s.%[3]s;", namespace, whutils.IdentifiesTable, whutils.UsersTable))
			require.NoError(t, err)
			_, err = standByDB.ExecContext(ctx, fmt.Sprintf("CREATE SUBSCRIPTION users_subscription CONNECTION 'host=primary port=5432 user=%s password=%s dbname=%s' PUBLICATION users_publication;", user, password, database))
			require.NoError(t, err)

			// Adding primary key to identifies table
			_, err = primaryDB.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %s.%s ADD PRIMARY KEY ("id");`, namespace, whutils.IdentifiesTable))
			require.NoError(t, err)

			// Loading data should fail for the users table because of the missing primary key
			errorsMap := primaryPG.LoadUserTables(ctx)
			require.NoError(t, errorsMap[whutils.IdentifiesTable])
			var pgErr *pq.Error
			require.ErrorAs(t, errorsMap[whutils.UsersTable], &pgErr)
			require.EqualValues(t, pq.ErrorCode("55000"), pgErr.Code)

			// Adding primary key to users table
			_, err = primaryDB.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %s.%s ADD PRIMARY KEY ("id");`, namespace, whutils.UsersTable))
			require.NoError(t, err)

			// Loading data should work now
			errorsMap = primaryPG.LoadUserTables(ctx)
			require.NoError(t, errorsMap[whutils.IdentifiesTable])
			require.NoError(t, errorsMap[whutils.UsersTable])

			// Checking the number of rows in both primary and standby databases
			for _, tableName := range []string{whutils.IdentifiesTable, whutils.UsersTable} {
				var (
					countQuery = fmt.Sprintf("SELECT COUNT(*) FROM %s.%s;", namespace, tableName)
					count      int
				)
				require.Eventually(t, func() bool {
					err := primaryDB.QueryRowContext(ctx, countQuery).Scan(&count)
					if err != nil {
						t.Logf("Error while querying primary database: %v", err)
						return false
					}
					if count != expectedCount {
						t.Logf("Expected %d rows in primary database, got %d", expectedCount, count)
						return false
					}
					return true
				},
					10*time.Second,
					100*time.Millisecond,
				)
				require.Eventually(t, func() bool {
					err := standByDB.QueryRowContext(ctx, countQuery).Scan(&count)
					if err != nil {
						t.Logf("Error while querying standby database: %v", err)
						return false
					}
					if count != expectedCount {
						t.Logf("Expected %d rows in standby database, got %d", expectedCount, count)
						return false
					}
					return true
				},
					10*time.Second,
					100*time.Millisecond,
				)
			}
		})
	})
}

func mockUploader(
	t testing.TB,
	loadFiles []whutils.LoadFile,
	tableName string,
	schemaInUpload model.TableSchema,
	schemaInWarehouse model.TableSchema,
) whutils.Uploader {
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
