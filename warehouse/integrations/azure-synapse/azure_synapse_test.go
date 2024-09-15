package azuresynapse_test

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

	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	azuresynapse "github.com/rudderlabs/rudder-server/warehouse/integrations/azure-synapse"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

	"github.com/rudderlabs/compose-test/compose"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/compose-test/testcompose"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	whth "github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}

	misc.Init()
	validations.Init()
	whutils.Init()

	destType := whutils.AzureSynapse

	host := "localhost"
	database := "master"
	user := "SA"
	password := "reallyStrongPwd123"
	bucketName := "testbucket"
	accessKeyID := "MYACCESSKEY"
	secretAccessKey := "MYSECRETKEY"
	region := "us-east-1"

	t.Run("Events flow", func(t *testing.T) {
		httpPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.yml", "../testdata/docker-compose.jobsdb.yml", "../testdata/docker-compose.minio.yml", "../testdata/docker-compose.transformer.yml"}))
		c.Start(context.Background())

		workspaceID := whutils.RandHex()
		jobsDBPort := c.Port("jobsDb", 5432)
		azureSynapsePort := c.Port("azure_synapse", 1433)
		minioEndpoint := fmt.Sprintf("localhost:%d", c.Port("minio", 9000))
		transformerURL := fmt.Sprintf("http://localhost:%d", c.Port("transformer", 9090))

		jobsDB := whth.JobsDB(t, jobsDBPort)

		expectedSchema := [][]string{
			{"screens", "context_source_id", "varchar"},
			{"screens", "user_id", "varchar"},
			{"screens", "sent_at", "datetimeoffset"},
			{"screens", "context_request_ip", "varchar"},
			{"screens", "original_timestamp", "datetimeoffset"},
			{"screens", "url", "varchar"},
			{"screens", "context_source_type", "varchar"},
			{"screens", "_between", "varchar"},
			{"screens", "timestamp", "datetimeoffset"},
			{"screens", "context_ip", "varchar"},
			{"screens", "context_destination_type", "varchar"},
			{"screens", "received_at", "datetimeoffset"},
			{"screens", "title", "varchar"},
			{"screens", "uuid_ts", "datetimeoffset"},
			{"screens", "context_destination_id", "varchar"},
			{"screens", "name", "varchar"},
			{"screens", "id", "varchar"},
			{"screens", "_as", "varchar"},
			{"identifies", "context_ip", "varchar"},
			{"identifies", "context_destination_id", "varchar"},
			{"identifies", "email", "varchar"},
			{"identifies", "context_request_ip", "varchar"},
			{"identifies", "sent_at", "datetimeoffset"},
			{"identifies", "uuid_ts", "datetimeoffset"},
			{"identifies", "_as", "varchar"},
			{"identifies", "logins", "bigint"},
			{"identifies", "context_source_type", "varchar"},
			{"identifies", "context_traits_logins", "bigint"},
			{"identifies", "name", "varchar"},
			{"identifies", "context_destination_type", "varchar"},
			{"identifies", "_between", "varchar"},
			{"identifies", "id", "varchar"},
			{"identifies", "timestamp", "datetimeoffset"},
			{"identifies", "received_at", "datetimeoffset"},
			{"identifies", "user_id", "varchar"},
			{"identifies", "context_traits_email", "varchar"},
			{"identifies", "context_traits_as", "varchar"},
			{"identifies", "context_traits_name", "varchar"},
			{"identifies", "original_timestamp", "datetimeoffset"},
			{"identifies", "context_traits_between", "varchar"},
			{"identifies", "context_source_id", "varchar"},
			{"users", "context_traits_name", "varchar"},
			{"users", "context_traits_between", "varchar"},
			{"users", "context_request_ip", "varchar"},
			{"users", "context_traits_logins", "bigint"},
			{"users", "context_destination_id", "varchar"},
			{"users", "email", "varchar"},
			{"users", "logins", "bigint"},
			{"users", "_as", "varchar"},
			{"users", "context_source_id", "varchar"},
			{"users", "uuid_ts", "datetimeoffset"},
			{"users", "context_source_type", "varchar"},
			{"users", "context_traits_email", "varchar"},
			{"users", "name", "varchar"},
			{"users", "id", "varchar"},
			{"users", "_between", "varchar"},
			{"users", "context_ip", "varchar"},
			{"users", "received_at", "datetimeoffset"},
			{"users", "sent_at", "datetimeoffset"},
			{"users", "context_traits_as", "varchar"},
			{"users", "context_destination_type", "varchar"},
			{"users", "timestamp", "datetimeoffset"},
			{"users", "original_timestamp", "datetimeoffset"},
			{"product_track", "review_id", "varchar"},
			{"product_track", "context_source_id", "varchar"},
			{"product_track", "user_id", "varchar"},
			{"product_track", "timestamp", "datetimeoffset"},
			{"product_track", "uuid_ts", "datetimeoffset"},
			{"product_track", "review_body", "varchar"},
			{"product_track", "context_source_type", "varchar"},
			{"product_track", "_as", "varchar"},
			{"product_track", "_between", "varchar"},
			{"product_track", "id", "varchar"},
			{"product_track", "rating", "bigint"},
			{"product_track", "event", "varchar"},
			{"product_track", "original_timestamp", "datetimeoffset"},
			{"product_track", "context_destination_type", "varchar"},
			{"product_track", "context_ip", "varchar"},
			{"product_track", "context_destination_id", "varchar"},
			{"product_track", "sent_at", "datetimeoffset"},
			{"product_track", "received_at", "datetimeoffset"},
			{"product_track", "event_text", "varchar"},
			{"product_track", "product_id", "varchar"},
			{"product_track", "context_request_ip", "varchar"},
			{"tracks", "original_timestamp", "datetimeoffset"},
			{"tracks", "context_destination_id", "varchar"},
			{"tracks", "event", "varchar"},
			{"tracks", "context_request_ip", "varchar"},
			{"tracks", "uuid_ts", "datetimeoffset"},
			{"tracks", "context_destination_type", "varchar"},
			{"tracks", "user_id", "varchar"},
			{"tracks", "sent_at", "datetimeoffset"},
			{"tracks", "context_source_type", "varchar"},
			{"tracks", "context_ip", "varchar"},
			{"tracks", "timestamp", "datetimeoffset"},
			{"tracks", "received_at", "datetimeoffset"},
			{"tracks", "context_source_id", "varchar"},
			{"tracks", "event_text", "varchar"},
			{"tracks", "id", "varchar"},
			{"aliases", "context_request_ip", "varchar"},
			{"aliases", "context_destination_type", "varchar"},
			{"aliases", "context_destination_id", "varchar"},
			{"aliases", "previous_id", "varchar"},
			{"aliases", "context_ip", "varchar"},
			{"aliases", "sent_at", "datetimeoffset"},
			{"aliases", "id", "varchar"},
			{"aliases", "uuid_ts", "datetimeoffset"},
			{"aliases", "timestamp", "datetimeoffset"},
			{"aliases", "original_timestamp", "datetimeoffset"},
			{"aliases", "context_source_id", "varchar"},
			{"aliases", "user_id", "varchar"},
			{"aliases", "context_source_type", "varchar"},
			{"aliases", "received_at", "datetimeoffset"},
			{"pages", "name", "varchar"},
			{"pages", "url", "varchar"},
			{"pages", "id", "varchar"},
			{"pages", "timestamp", "datetimeoffset"},
			{"pages", "title", "varchar"},
			{"pages", "user_id", "varchar"},
			{"pages", "context_source_id", "varchar"},
			{"pages", "context_source_type", "varchar"},
			{"pages", "original_timestamp", "datetimeoffset"},
			{"pages", "context_request_ip", "varchar"},
			{"pages", "received_at", "datetimeoffset"},
			{"pages", "_between", "varchar"},
			{"pages", "context_destination_type", "varchar"},
			{"pages", "uuid_ts", "datetimeoffset"},
			{"pages", "context_destination_id", "varchar"},
			{"pages", "sent_at", "datetimeoffset"},
			{"pages", "context_ip", "varchar"},
			{"pages", "_as", "varchar"},
			{"groups", "_as", "varchar"},
			{"groups", "user_id", "varchar"},
			{"groups", "context_destination_type", "varchar"},
			{"groups", "sent_at", "datetimeoffset"},
			{"groups", "context_source_type", "varchar"},
			{"groups", "received_at", "datetimeoffset"},
			{"groups", "context_ip", "varchar"},
			{"groups", "industry", "varchar"},
			{"groups", "timestamp", "datetimeoffset"},
			{"groups", "group_id", "varchar"},
			{"groups", "uuid_ts", "datetimeoffset"},
			{"groups", "context_source_id", "varchar"},
			{"groups", "context_request_ip", "varchar"},
			{"groups", "_between", "varchar"},
			{"groups", "original_timestamp", "datetimeoffset"},
			{"groups", "name", "varchar"},
			{"groups", "_plan", "varchar"},
			{"groups", "context_destination_id", "varchar"},
			{"groups", "employees", "bigint"},
			{"groups", "id", "varchar"},
		}

		testcase := []struct {
			name          string
			tables        []string
			verifySchema  func(t *testing.T, db *sql.DB, namespace string)
			verifyRecords func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace string)
		}{
			{
				name:   "Upload Job",
				tables: []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				verifySchema: func(t *testing.T, db *sql.DB, namespace string) {
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
					require.ElementsMatch(t, schema, expectedSchema)
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace string) {
					userIDFormat := "userId_azure_synapse"
					userIDSQL := "LEFT(user_id, CHARINDEX('_', user_id, CHARINDEX('_', user_id, CHARINDEX('_', user_id) + 1) + 1) - 1)"
					uuidTSSQL := "LEFT(uuid_ts, 10)"

					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, LEFT(id, CHARINDEX('_', id, CHARINDEX('_', id, CHARINDEX('_', id) + 1) + 1) - 1), %s, received_at, name, original_timestamp FROM %q.%q ORDER BY id;`, uuidTSSQL, namespace, "users"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersRecords(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "product_track"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "pages"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, _as FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "screens"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "aliases"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, _plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "groups"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
		}

		for _, tc := range testcase {
			t.Run(tc.name, func(t *testing.T) {
				var (
					sourceID      = whutils.RandHex()
					destinationID = whutils.RandHex()
					writeKey      = whutils.RandHex()
					namespace     = whth.RandSchema(destType)
				)

				destination := backendconfigtest.NewDestinationBuilder(destType).
					WithID(destinationID).
					WithRevisionID(destinationID).
					WithConfigOption("host", host).
					WithConfigOption("database", database).
					WithConfigOption("user", user).
					WithConfigOption("password", password).
					WithConfigOption("port", strconv.Itoa(azureSynapsePort)).
					WithConfigOption("sslMode", "disable").
					WithConfigOption("namespace", namespace).
					WithConfigOption("bucketProvider", whutils.MINIO).
					WithConfigOption("bucketName", bucketName).
					WithConfigOption("accessKeyID", accessKeyID).
					WithConfigOption("secretAccessKey", secretAccessKey).
					WithConfigOption("useSSL", false).
					WithConfigOption("endPoint", minioEndpoint).
					WithConfigOption("useRudderStorage", false).
					WithConfigOption("syncFrequency", "30").
					Build()

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

				t.Setenv("RSERVER_WAREHOUSE_AZURE_SYNAPSE_MAX_PARALLEL_LOADS", "8")
				t.Setenv("RSERVER_WAREHOUSE_AZURE_SYNAPSE_SLOW_QUERY_THRESHOLD", "0s")

				whth.BootstrapSvc(t, workspaceConfig, httpPort, jobsDBPort)

				dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%d?TrustServerCertificate=true&database=%s&encrypt=disable",
					user, password, host, azureSynapsePort, database,
				)
				db, err := sql.Open("sqlserver", dsn)
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
					WriteKey:        writeKey,
					Schema:          namespace,
					Tables:          tc.tables,
					SourceID:        sourceID,
					DestinationID:   destinationID,
					Config:          conf,
					WorkspaceID:     workspaceID,
					DestinationType: destType,
					JobsDB:          jobsDB,
					HTTPPort:        httpPort,
					Client:          sqlClient,
					EventsFilePath:  "../testdata/upload-job.events-1.json",
					UserID:          whth.GetUserId(destType),
					TransformerURL:  transformerURL,
					Destination:     destination,
				}
				ts1.VerifyEvents(t)

				t.Log("verifying test case 2")
				ts2 := whth.TestConfig{
					WriteKey:        writeKey,
					Schema:          namespace,
					Tables:          tc.tables,
					SourceID:        sourceID,
					DestinationID:   destinationID,
					Config:          conf,
					WorkspaceID:     workspaceID,
					DestinationType: destType,
					JobsDB:          jobsDB,
					HTTPPort:        httpPort,
					Client:          sqlClient,
					EventsFilePath:  "../testdata/upload-job.events-2.json",
					UserID:          whth.GetUserId(destType),
					TransformerURL:  transformerURL,
					Destination:     destination,
				}
				ts2.VerifyEvents(t)

				t.Log("verifying schema")
				tc.verifySchema(t, db, namespace)

				t.Log("verifying records")
				tc.verifyRecords(t, db, sourceID, destinationID, namespace)
			})
		}
	})

	t.Run("Validations", func(t *testing.T) {
		c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.yml", "../testdata/docker-compose.minio.yml"}))
		c.Start(context.Background())

		azureSynapsePort := c.Port("azure_synapse", 1433)
		minioEndpoint := fmt.Sprintf("localhost:%d", c.Port("minio", 9000))

		namespace := whth.RandSchema(destType)

		dest := backendconfig.DestinationT{
			ID: "test_destination_id",
			Config: map[string]any{
				"host":             host,
				"database":         database,
				"user":             user,
				"password":         password,
				"port":             strconv.Itoa(azureSynapsePort),
				"sslMode":          "disable",
				"namespace":        namespace,
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
				ID:          "1qvbUYC2xVQ7lvI9UUYkkM4IBt9",
				Name:        "AZURE_SYNAPSE",
				DisplayName: "Microsoft SQL Server",
			},
			Name:       "azure-synapse-demo",
			Enabled:    true,
			RevisionID: "test_destination_id",
		}

		whth.VerifyConfigurationTest(t, dest)
	})

	t.Run("Load Table", func(t *testing.T) {
		c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.yml", "../testdata/docker-compose.minio.yml"}))
		c.Start(context.Background())

		azureSynapsePort := c.Port("azure_synapse", 1433)
		minioEndpoint := fmt.Sprintf("localhost:%d", c.Port("minio", 9000))

		ctx := context.Background()
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
					"port":             strconv.Itoa(azureSynapsePort),
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
			tableName := "schema_not_exists_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			az := azuresynapse.New(config.New(), logger.NOP, stats.NOP)
			err := az.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			loadTableStat, err := az.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("table does not exists", func(t *testing.T) {
			tableName := "table_not_exists_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			az := azuresynapse.New(config.New(), logger.NOP, stats.NOP)
			err := az.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = az.CreateSchema(ctx)
			require.NoError(t, err)

			loadTableStat, err := az.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("merge", func(t *testing.T) {
			tableName := "merge_test_table"

			t.Run("without dedup", func(t *testing.T) {
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

				az := azuresynapse.New(config.New(), logger.NOP, stats.NOP)
				err := az.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				err = az.CreateSchema(ctx)
				require.NoError(t, err)

				err = az.CreateTable(ctx, tableName, schemaInWarehouse)
				require.NoError(t, err)

				loadTableStat, err := az.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(14))
				require.Equal(t, loadTableStat.RowsUpdated, int64(0))

				loadTableStat, err = az.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(0))
				require.Equal(t, loadTableStat.RowsUpdated, int64(14))

				records := whth.RetrieveRecordsFromWarehouse(t, az.DB.DB,
					fmt.Sprintf(`
						SELECT
						  id,
						  received_at,
						  test_bool,
						  test_datetime,
						  cast(test_float AS float) AS test_float,
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
				require.Equal(t, records, whth.SampleTestRecords())
			})
			t.Run("with dedup", func(t *testing.T) {
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/dedup.csv.gz", tableName)

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

				az := azuresynapse.New(config.New(), logger.NOP, stats.NOP)
				err := az.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				err = az.CreateSchema(ctx)
				require.NoError(t, err)

				err = az.CreateTable(ctx, tableName, schemaInWarehouse)
				require.NoError(t, err)

				loadTableStat, err := az.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(0))
				require.Equal(t, loadTableStat.RowsUpdated, int64(14))

				records := whth.RetrieveRecordsFromWarehouse(t, az.DB.DB,
					fmt.Sprintf(`
						SELECT
						  id,
						  received_at,
						  test_bool,
						  test_datetime,
						  cast(test_float AS float) AS test_float,
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
		t.Run("load file does not exists", func(t *testing.T) {
			tableName := "load_file_not_exists_test_table"

			loadFiles := []whutils.LoadFile{{
				Location: "http://localhost:1234/testbucket/rudder-warehouse-load-objects/load_file_not_exists_test_table/test_source_id/f31af97e-03e8-46d0-8a1a-1786cb85b22c-load_file_not_exists_test_table/load.csv.gz",
			}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			az := azuresynapse.New(config.New(), logger.NOP, stats.NOP)
			err := az.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = az.CreateSchema(ctx)
			require.NoError(t, err)

			err = az.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := az.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("mismatch in number of columns", func(t *testing.T) {
			tableName := "mismatch_columns_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/mismatch-columns.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			az := azuresynapse.New(config.New(), logger.NOP, stats.NOP)
			err := az.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = az.CreateSchema(ctx)
			require.NoError(t, err)

			err = az.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := az.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("mismatch in schema", func(t *testing.T) {
			tableName := "mismatch_schema_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/mismatch-schema.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			az := azuresynapse.New(config.New(), logger.NOP, stats.NOP)
			err := az.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = az.CreateSchema(ctx)
			require.NoError(t, err)

			err = az.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := az.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := whth.RetrieveRecordsFromWarehouse(t, az.DB.DB,
				fmt.Sprintf(`
					SELECT
					  id,
					  received_at,
					  test_bool,
					  test_datetime,
					  cast(test_float AS float) AS test_float,
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
			require.Equal(t, records, whth.MismatchSchemaTestRecords())
		})
		t.Run("discards", func(t *testing.T) {
			tableName := whutils.DiscardsTable

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/discards.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, whutils.DiscardsSchema, whutils.DiscardsSchema)

			az := azuresynapse.New(config.New(), logger.NOP, stats.NOP)
			err := az.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = az.CreateSchema(ctx)
			require.NoError(t, err)

			err = az.CreateTable(ctx, tableName, whutils.DiscardsSchema)
			require.NoError(t, err)

			loadTableStat, err := az.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(6))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := whth.RetrieveRecordsFromWarehouse(t, az.DB.DB,
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

	t.Run("CrashRecovery", func(t *testing.T) {
		c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.yml", "../testdata/docker-compose.minio.yml"}))
		c.Start(context.Background())

		azureSynapsePort := c.Port("azure_synapse", 1433)
		minioEndpoint := fmt.Sprintf("localhost:%d", c.Port("minio", 9000))

		ctx := context.Background()
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
					"port":             strconv.Itoa(azureSynapsePort),
					"sslMode":          "disable",
					"namespace":        namespace,
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

		tableName := "crash_recovery_test_table"

		mockUploader := newMockUploader(t, []whutils.LoadFile{}, tableName, whutils.DiscardsSchema, whutils.DiscardsSchema)

		t.Run("successful cleanup", func(t *testing.T) {
			az := azuresynapse.New(config.New(), logger.NOP, stats.NOP)
			err := az.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			stagingTable := whutils.StagingTablePrefix(destType) + tableName

			_, err = az.DB.ExecContext(ctx, fmt.Sprintf("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '%[1]s') BEGIN EXEC('CREATE SCHEMA %[1]s') END;", namespace))
			require.NoError(t, err)

			_, err = az.DB.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %q.%q (id int)", namespace, stagingTable))
			require.NoError(t, err)

			var count int
			err = az.DB.QueryRowContext(ctx, `
				SELECT count(*)
				FROM information_schema.tables
				WHERE table_schema = @schema AND table_name = @table`,
				sql.Named("schema", namespace),
				sql.Named("table", stagingTable),
			).Scan(&count)
			require.NoError(t, err)

			require.Equal(t, 1, count, "staging table should be created")

			err = az.CrashRecover(ctx)
			require.NoError(t, err)

			err = az.DB.QueryRowContext(ctx, `
				SELECT count(*)
				FROM information_schema.tables
				WHERE table_schema = @schema AND table_name = @table`,
				sql.Named("schema", namespace),
				sql.Named("table", stagingTable),
			).Scan(&count)
			require.NoError(t, err)
			require.Equal(t, 0, count, "staging table should be dropped")
		})

		t.Run("query error", func(t *testing.T) {
			az := azuresynapse.New(config.New(), logger.NOP, stats.NOP)
			err := az.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			db, dbMock, err := sqlmock.New()
			require.NoError(t, err)
			defer func() {
				_ = db.Close()
			}()

			dbMock.ExpectQuery("select table_name").WillReturnError(fmt.Errorf("query error"))

			// TODO: Add more test cases
			az.DB = sqlquerywrapper.New(db)
			err = az.CrashRecover(ctx)
			require.ErrorContains(t, err, "query error")
		})
	})
}

func TestAzureSynapse_ProcessColumnValue(t *testing.T) {
	testCases := []struct {
		name          string
		data          string
		dataType      string
		expectedValue interface{}
		wantError     bool
	}{
		{
			name:      "invalid integer",
			data:      "1.01",
			dataType:  model.IntDataType,
			wantError: true,
		},
		{
			name:          "valid integer",
			data:          "1",
			dataType:      model.IntDataType,
			expectedValue: int64(1),
		},
		{
			name:      "invalid float",
			data:      "test",
			dataType:  model.FloatDataType,
			wantError: true,
		},
		{
			name:          "valid float",
			data:          "1.01",
			dataType:      model.FloatDataType,
			expectedValue: 1.01,
		},
		{
			name:      "invalid boolean",
			data:      "test",
			dataType:  model.BooleanDataType,
			wantError: true,
		},
		{
			name:          "valid boolean",
			data:          "true",
			dataType:      model.BooleanDataType,
			expectedValue: true,
		},
		{
			name:      "invalid datetime",
			data:      "1",
			dataType:  model.DateTimeDataType,
			wantError: true,
		},
		{
			name:          "valid datetime",
			data:          "2020-01-01T00:00:00Z",
			dataType:      model.DateTimeDataType,
			expectedValue: time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "valid string",
			data:          "test",
			dataType:      model.StringDataType,
			expectedValue: "test",
		},
		{
			name:          "valid string exceeding max length",
			data:          strings.Repeat("test", 200),
			dataType:      model.StringDataType,
			expectedValue: strings.Repeat("test", 128),
		},
		{
			name:          "valid string with diacritics",
			data:          "tést",
			dataType:      model.StringDataType,
			expectedValue: []byte{0x74, 0x0, 0xe9, 0x0, 0x73, 0x0, 0x74, 0x0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			az := azuresynapse.New(config.New(), logger.NOP, stats.NOP)

			value, err := az.ProcessColumnValue(tc.data, tc.dataType)
			if tc.wantError {
				require.Error(t, err)
				return
			}
			require.EqualValues(t, tc.expectedValue, value)
			require.NoError(t, err)
		})
	}
}

func newMockUploader(
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
	mockUploader.EXPECT().GetLoadFilesMetadata(gomock.Any(), gomock.Any()).Return(loadFiles, nil).AnyTimes()
	mockUploader.EXPECT().GetTableSchemaInUpload(tableName).Return(schemaInUpload).AnyTimes()
	mockUploader.EXPECT().GetTableSchemaInWarehouse(tableName).Return(schemaInWarehouse).AnyTimes()

	return mockUploader
}
