package snowpipestreaming

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/iancoleman/strcase"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/testhelper"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/snowflake"
	whth "github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestSnowpipeStreaming(t *testing.T) {
	for _, key := range []string{
		testhelper.TestKeyPairEncrypted,
		testhelper.TestKeyPairUnencrypted,
	} {
		if _, exists := os.LookupEnv(key); !exists {
			if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
				t.Fatalf("%s environment variable not set", key)
			}
			t.Skipf("Skipping %s as %s is not set", t.Name(), key)
		}
	}

	c := testcompose.New(t, compose.FilePaths([]string{"../../router/batchrouter/asyncdestinationmanager/snowpipestreaming/testdata/docker-compose.rudder-snowpipe-clients.yml", "testdata/docker-compose.rudder-transformer.yml"}))
	c.Start(context.Background())

	transformerURL := fmt.Sprintf("http://localhost:%d", c.Port("transformer", 9090))
	snowpipeClientsURL := fmt.Sprintf("http://localhost:%d", c.Port("rudder-snowpipe-clients", 9078))

	credentials, err := testhelper.GetSnowpipeTestCredentials(testhelper.TestKeyPairUnencrypted)
	require.NoError(t, err)

	t.Run("namespace and table already exists", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := testhelper.RandSchema()
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, snowpipeClientsURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		t.Log("Creating schema and tables")
		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, sm.Setup(ctx, warehouse, whutils.NewNoOpUploader()))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { testhelper.DropSchema(t, sm.DB.DB, namespace) })
		require.NoError(t, sm.CreateTable(ctx, "PRODUCT_REVIEWED", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_IP": "string", "CONTEXT_REQUEST_IP": "string", "CONTEXT_PASSED_IP": "string", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "EVENT": "string", "EVENT_TEXT": "string", "ID": "string", "ORIGINAL_TIMESTAMP": "datetime", "PRODUCT_ID": "string", "RATING": "int", "RECEIVED_AT": "datetime", "REVIEW_BODY": "string", "REVIEW_ID": "string", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "USER_ID": "string", "UUID_TS": "datetime",
		}))
		require.NoError(t, sm.CreateTable(ctx, "TRACKS", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_IP": "string", "CONTEXT_REQUEST_IP": "string", "CONTEXT_PASSED_IP": "string", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "EVENT": "string", "EVENT_TEXT": "string", "ID": "string", "ORIGINAL_TIMESTAMP": "datetime", "RECEIVED_AT": "datetime", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "USER_ID": "string", "UUID_TS": "datetime",
		}))

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"type":"track","messageId":"%[1]s","userId":"%[1]s","event":"Product Reviewed","properties":{"review_id":"86ac1cd43","product_id":"9578257311","rating":3,"review_body":"OK for the price. It works but the material feels flimsy."}, "timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z", "context":{"ip":"14.5.67.21"}}]}`,
				strconv.Itoa(index+1),
			)
		}
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"PRODUCT_REVIEWED": {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "PRODUCT_ID": "TEXT", "RATING": "NUMBER", "RECEIVED_AT": "TIMESTAMP_TZ", "REVIEW_BODY": "TEXT", "REVIEW_ID": "TEXT", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"TRACKS":           {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"RUDDER_DISCARDS":  {"COLUMN_NAME": "TEXT", "COLUMN_VALUE": "TEXT", "REASON": "TEXT", "RECEIVED_AT": "TIMESTAMP_TZ", "ROW_ID": "TEXT", "TABLE_NAME": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		produceReviewedRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, PRODUCT_ID, RATING, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), REVIEW_BODY, REVIEW_ID, SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "PRODUCT_REVIEWED"))
		require.ElementsMatch(t, productReviewedRecords(source, destination), produceReviewedRecordsFromDB)
		tracksRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "TRACKS"))
		require.ElementsMatch(t, tracksRecords(source, destination), tracksRecordsFromDB)

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("namespace does not exists", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := testhelper.RandSchema()
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, snowpipeClientsURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, whutils.NewNoOpUploader()))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		t.Cleanup(func() { testhelper.DropSchema(t, sm.DB.DB, namespace) })

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"type":"track","messageId":"%[1]s","userId":"%[1]s","event":"Product Reviewed","properties":{"review_id":"86ac1cd43","product_id":"9578257311","rating":3,"review_body":"OK for the price. It works but the material feels flimsy."}, "timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z", "context":{"ip":"14.5.67.21"}}]}`,
				strconv.Itoa(index+1),
			)
		}
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"PRODUCT_REVIEWED": {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "PRODUCT_ID": "TEXT", "RATING": "NUMBER", "RECEIVED_AT": "TIMESTAMP_TZ", "REVIEW_BODY": "TEXT", "REVIEW_ID": "TEXT", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"TRACKS":           {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"RUDDER_DISCARDS":  {"COLUMN_NAME": "TEXT", "COLUMN_VALUE": "TEXT", "REASON": "TEXT", "RECEIVED_AT": "TIMESTAMP_TZ", "ROW_ID": "TEXT", "TABLE_NAME": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		produceReviewedRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, PRODUCT_ID, RATING, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), REVIEW_BODY, REVIEW_ID, SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "PRODUCT_REVIEWED"))
		require.ElementsMatch(t, productReviewedRecords(source, destination), produceReviewedRecordsFromDB)
		tracksRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "TRACKS"))
		require.ElementsMatch(t, tracksRecords(source, destination), tracksRecordsFromDB)

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("table does not exists", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := testhelper.RandSchema()
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, snowpipeClientsURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		t.Log("Creating schema")
		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, whutils.NewNoOpUploader()))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { testhelper.DropSchema(t, sm.DB.DB, namespace) })

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"type":"track","messageId":"%[1]s","userId":"%[1]s","event":"Product Reviewed","properties":{"review_id":"86ac1cd43","product_id":"9578257311","rating":3,"review_body":"OK for the price. It works but the material feels flimsy."}, "timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z", "context":{"ip":"14.5.67.21"}}]}`,
				strconv.Itoa(index+1),
			)
		}
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"PRODUCT_REVIEWED": {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "PRODUCT_ID": "TEXT", "RATING": "NUMBER", "RECEIVED_AT": "TIMESTAMP_TZ", "REVIEW_BODY": "TEXT", "REVIEW_ID": "TEXT", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"TRACKS":           {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"RUDDER_DISCARDS":  {"COLUMN_NAME": "TEXT", "COLUMN_VALUE": "TEXT", "REASON": "TEXT", "RECEIVED_AT": "TIMESTAMP_TZ", "ROW_ID": "TEXT", "TABLE_NAME": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		produceReviewedRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, PRODUCT_ID, RATING, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), REVIEW_BODY, REVIEW_ID, SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "PRODUCT_REVIEWED"))
		require.ElementsMatch(t, productReviewedRecords(source, destination), produceReviewedRecordsFromDB)
		tracksRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "TRACKS"))
		require.ElementsMatch(t, tracksRecords(source, destination), tracksRecordsFromDB)

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("events with different schema", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := testhelper.RandSchema()
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, snowpipeClientsURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		t.Log("Creating schema and tables")
		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, whutils.NewNoOpUploader()))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { testhelper.DropSchema(t, sm.DB.DB, namespace) })
		require.NoError(t, sm.CreateTable(ctx, "PRODUCT_REVIEWED", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_IP": "string", "CONTEXT_REQUEST_IP": "string", "CONTEXT_PASSED_IP": "string", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "EVENT": "string", "EVENT_TEXT": "string", "ID": "string", "ORIGINAL_TIMESTAMP": "datetime", "PRODUCT_ID": "string", "RATING": "int", "RECEIVED_AT": "datetime", "REVIEW_BODY": "string", "REVIEW_ID": "string", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "USER_ID": "string", "UUID_TS": "datetime",
		}))
		require.NoError(t, sm.CreateTable(ctx, "TRACKS", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_IP": "string", "CONTEXT_REQUEST_IP": "string", "CONTEXT_PASSED_IP": "string", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "EVENT": "string", "EVENT_TEXT": "string", "ID": "string", "ORIGINAL_TIMESTAMP": "datetime", "RECEIVED_AT": "datetime", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "USER_ID": "string", "UUID_TS": "datetime",
		}))

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"type":"track","messageId":"%[1]s","userId":"%[1]s","event":"Product Reviewed","properties":{"review_id":"86ac1cd43","product_id":"9578257311","rating":3,"review_body":"OK for the price. It works but the material feels flimsy.","additional_column_%[1]s": "%[1]s"}, "timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z", "context":{"ip":"14.5.67.21"}}]}`,
				strconv.Itoa(index+1),
			)
		}
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"PRODUCT_REVIEWED": {"ADDITIONAL_COLUMN_1": "TEXT", "ADDITIONAL_COLUMN_2": "TEXT", "ADDITIONAL_COLUMN_3": "TEXT", "ADDITIONAL_COLUMN_4": "TEXT", "ADDITIONAL_COLUMN_5": "TEXT", "CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "PRODUCT_ID": "TEXT", "RATING": "NUMBER", "RECEIVED_AT": "TIMESTAMP_TZ", "REVIEW_BODY": "TEXT", "REVIEW_ID": "TEXT", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"TRACKS":           {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"RUDDER_DISCARDS":  {"COLUMN_NAME": "TEXT", "COLUMN_VALUE": "TEXT", "REASON": "TEXT", "RECEIVED_AT": "TIMESTAMP_TZ", "ROW_ID": "TEXT", "TABLE_NAME": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		ts := timeutil.Now().Format("2006-01-02")
		eventName := "Product Reviewed"
		tableName := strcase.ToSnake(eventName)
		produceReviewedRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, PRODUCT_ID, RATING, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), REVIEW_BODY, REVIEW_ID, SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD'), ADDITIONAL_COLUMN_1, ADDITIONAL_COLUMN_2, ADDITIONAL_COLUMN_3, ADDITIONAL_COLUMN_4, ADDITIONAL_COLUMN_5 FROM %q.%q;`, namespace, "PRODUCT_REVIEWED"))
		require.ElementsMatch(t, [][]string{
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "1", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "1", ts, "1", "", "", "", ""},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "2", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "2", ts, "", "2", "", "", ""},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "3", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "3", ts, "", "", "3", "", ""},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "4", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "4", ts, "", "", "", "4", ""},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "5", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "5", ts, "", "", "", "", "5"},
		}, produceReviewedRecordsFromDB)
		tracksRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "TRACKS"))
		require.ElementsMatch(t, tracksRecords(source, destination), tracksRecordsFromDB)

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("discards", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := testhelper.RandSchema()
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, snowpipeClientsURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		t.Log("Creating schema and tables. Discards is not created")
		t.Log("CONTEXT_IP, CONTEXT_REQUEST_IP are of type int")
		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, whutils.NewNoOpUploader()))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { testhelper.DropSchema(t, sm.DB.DB, namespace) })
		require.NoError(t, sm.CreateTable(ctx, "PRODUCT_REVIEWED", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_IP": "int", "CONTEXT_REQUEST_IP": "string", "CONTEXT_PASSED_IP": "int", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "EVENT": "string", "EVENT_TEXT": "string", "ID": "string", "ORIGINAL_TIMESTAMP": "datetime", "PRODUCT_ID": "string", "RATING": "int", "RECEIVED_AT": "datetime", "REVIEW_BODY": "string", "REVIEW_ID": "string", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "USER_ID": "string", "UUID_TS": "datetime",
			"ARRAY_EXAMPLE": "datetime", "ARRAY_OF_BOOLEANS_EXAMPLE": "datetime", "ARRAY_OF_FLOATS_EXAMPLE": "datetime", "ARRAY_OF_INTEGERS_EXAMPLE": "datetime", "BINARY_EXAMPLE": "datetime", "BOOLEAN_EXAMPLE": "datetime", "DATE_EXAMPLE": "datetime", "DATE_TIME_EXAMPLE": "datetime", "DIMENSIONS_DEPTH": "datetime", "DIMENSIONS_HEIGHT": "datetime", "DIMENSIONS_UNIT": "datetime", "DIMENSIONS_WIDTH": "datetime", "DURATION_EXAMPLE": "datetime", "FLOAT_EXAMPLE": "datetime", "GEO_POINT_EXAMPLE_LATITUDE": "datetime", "GEO_POINT_EXAMPLE_LONGITUDE": "datetime", "NESTED_ARRAY_EXAMPLE": "datetime", "NESTED_ARRAY_OF_OBJECTS_EXAMPLE": "datetime", "OBJECT_EXAMPLE_NESTED_BOOLEAN": "datetime", "OBJECT_EXAMPLE_NESTED_INTEGER": "datetime", "OBJECT_EXAMPLE_NESTED_STRING": "datetime", "STRING_EXAMPLE": "datetime", "URL_EXAMPLE": "datetime",
		}))
		require.NoError(t, sm.CreateTable(ctx, "TRACKS", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_IP": "int", "CONTEXT_REQUEST_IP": "string", "CONTEXT_PASSED_IP": "int", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "EVENT": "string", "EVENT_TEXT": "string", "ID": "string", "ORIGINAL_TIMESTAMP": "datetime", "RECEIVED_AT": "datetime", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "USER_ID": "string", "UUID_TS": "datetime",
		}))

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"type":"track","messageId":"%[1]s","userId":"%[1]s","event":"Product Reviewed","properties":{"review_id":"86ac1cd43","product_id":"9578257311","rating":3,"review_body":"OK for the price. It works but the material feels flimsy.","stringExample":"Wireless Headphones","integerExample":12345,"floatExample":299.99,"booleanExample":true,"nullExample":null,"arrayExample":["electronics","audio","wireless"],"arrayOfIntegersExample":[1,2,3,4],"arrayOfFloatsExample":[10.5,20.3,30.7],"arrayOfBooleansExample":[true,false,true],"nestedArrayExample":[[1,2],[3,4]],"objectExample":{"nestedString":"Inner Text","nestedInteger":789,"nestedBoolean":false},"nestedArrayOfObjectsExample":[{"id":1,"name":"Accessory 1"},{"id":2,"name":"Accessory 2"}],"dateExample":"2024-11-12","dateTimeExample":"2024-11-12T15:30:00Z","durationExample":"PT1H30M","dimensions":{"height":10.5,"width":5,"depth":3,"unit":"cm"},"binaryExample":"SGVsbG8sIFdvcmxkIQ==","geoPointExample":{"latitude":37.7749,"longitude":-122.4194},"urlExample":"https://example.com/product/12345"},"timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z","context":{"ip":"14.5.67.21"}}]}`,
				strconv.Itoa(index+1),
			)
		}
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"PRODUCT_REVIEWED": {"ARRAY_EXAMPLE": "TIMESTAMP_TZ", "ARRAY_OF_BOOLEANS_EXAMPLE": "TIMESTAMP_TZ", "ARRAY_OF_FLOATS_EXAMPLE": "TIMESTAMP_TZ", "ARRAY_OF_INTEGERS_EXAMPLE": "TIMESTAMP_TZ", "BINARY_EXAMPLE": "TIMESTAMP_TZ", "BOOLEAN_EXAMPLE": "TIMESTAMP_TZ", "CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "NUMBER", "CONTEXT_PASSED_IP": "NUMBER", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "DATE_EXAMPLE": "TIMESTAMP_TZ", "DATE_TIME_EXAMPLE": "TIMESTAMP_TZ", "DIMENSIONS_DEPTH": "TIMESTAMP_TZ", "DIMENSIONS_HEIGHT": "TIMESTAMP_TZ", "DIMENSIONS_UNIT": "TIMESTAMP_TZ", "DIMENSIONS_WIDTH": "TIMESTAMP_TZ", "DURATION_EXAMPLE": "TIMESTAMP_TZ", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "FLOAT_EXAMPLE": "TIMESTAMP_TZ", "GEO_POINT_EXAMPLE_LATITUDE": "TIMESTAMP_TZ", "GEO_POINT_EXAMPLE_LONGITUDE": "TIMESTAMP_TZ", "ID": "TEXT", "INTEGER_EXAMPLE": "NUMBER", "NESTED_ARRAY_EXAMPLE": "TIMESTAMP_TZ", "NESTED_ARRAY_OF_OBJECTS_EXAMPLE": "TIMESTAMP_TZ", "OBJECT_EXAMPLE_NESTED_BOOLEAN": "TIMESTAMP_TZ", "OBJECT_EXAMPLE_NESTED_INTEGER": "TIMESTAMP_TZ", "OBJECT_EXAMPLE_NESTED_STRING": "TIMESTAMP_TZ", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "PRODUCT_ID": "TEXT", "RATING": "NUMBER", "RECEIVED_AT": "TIMESTAMP_TZ", "REVIEW_BODY": "TEXT", "REVIEW_ID": "TEXT", "SENT_AT": "TIMESTAMP_TZ", "STRING_EXAMPLE": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "URL_EXAMPLE": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"TRACKS":           {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "NUMBER", "CONTEXT_PASSED_IP": "NUMBER", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"RUDDER_DISCARDS":  {"COLUMN_NAME": "TEXT", "COLUMN_VALUE": "TEXT", "REASON": "TEXT", "RECEIVED_AT": "TIMESTAMP_TZ", "ROW_ID": "TEXT", "TABLE_NAME": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		ts := timeutil.Now().Format("2006-01-02")
		eventName := "Product Reviewed"
		tableName := strcase.ToSnake(eventName)
		produceReviewedRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT ARRAY_EXAMPLE, ARRAY_OF_BOOLEANS_EXAMPLE, ARRAY_OF_FLOATS_EXAMPLE, ARRAY_OF_INTEGERS_EXAMPLE, BINARY_EXAMPLE, BOOLEAN_EXAMPLE, DATE_EXAMPLE, DATE_TIME_EXAMPLE, DIMENSIONS_DEPTH, DIMENSIONS_HEIGHT, DIMENSIONS_UNIT, DIMENSIONS_WIDTH, DURATION_EXAMPLE, FLOAT_EXAMPLE, GEO_POINT_EXAMPLE_LATITUDE, GEO_POINT_EXAMPLE_LONGITUDE, INTEGER_EXAMPLE, NESTED_ARRAY_EXAMPLE, NESTED_ARRAY_OF_OBJECTS_EXAMPLE, OBJECT_EXAMPLE_NESTED_BOOLEAN, OBJECT_EXAMPLE_NESTED_INTEGER, OBJECT_EXAMPLE_NESTED_STRING, STRING_EXAMPLE, URL_EXAMPLE, CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, PRODUCT_ID, RATING, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), REVIEW_BODY, REVIEW_ID, SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "PRODUCT_REVIEWED"))
		require.ElementsMatch(t, [][]string{
			{"", "", "", "", "", "", "2024-11-12T00:00:00Z", "2024-11-12T15:30:00Z", "", "", "", "", "", "", "", "", "12345", "", "", "", "", "", "", "", destination.ID, "SNOWPIPE_STREAMING", "", "", source.ID, source.SourceDefinition.Name, tableName, eventName, "1", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "1", ts},
			{"", "", "", "", "", "", "2024-11-12T00:00:00Z", "2024-11-12T15:30:00Z", "", "", "", "", "", "", "", "", "12345", "", "", "", "", "", "", "", destination.ID, "SNOWPIPE_STREAMING", "", "", source.ID, source.SourceDefinition.Name, tableName, eventName, "2", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "2", ts},
			{"", "", "", "", "", "", "2024-11-12T00:00:00Z", "2024-11-12T15:30:00Z", "", "", "", "", "", "", "", "", "12345", "", "", "", "", "", "", "", destination.ID, "SNOWPIPE_STREAMING", "", "", source.ID, source.SourceDefinition.Name, tableName, eventName, "3", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "3", ts},
			{"", "", "", "", "", "", "2024-11-12T00:00:00Z", "2024-11-12T15:30:00Z", "", "", "", "", "", "", "", "", "12345", "", "", "", "", "", "", "", destination.ID, "SNOWPIPE_STREAMING", "", "", source.ID, source.SourceDefinition.Name, tableName, eventName, "4", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "4", ts},
			{"", "", "", "", "", "", "2024-11-12T00:00:00Z", "2024-11-12T15:30:00Z", "", "", "", "", "", "", "", "", "12345", "", "", "", "", "", "", "", destination.ID, "SNOWPIPE_STREAMING", "", "", source.ID, source.SourceDefinition.Name, tableName, eventName, "5", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "5", ts},
		}, produceReviewedRecordsFromDB)
		tracksRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "TRACKS"))
		require.ElementsMatch(t, tracksRecordsForDiscards(source, destination), tracksRecordsFromDB)
		discardsRecordsInDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT COLUMN_NAME, COLUMN_VALUE, REASON, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), ROW_ID, TABLE_NAME, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "RUDDER_DISCARDS"))
		require.ElementsMatch(t, [][]string{
			{"CONTEXT_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "1", "TRACKS", ts},
			{"CONTEXT_PASSED_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "1", "TRACKS", ts},
			{"CONTEXT_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "2", "TRACKS", ts},
			{"CONTEXT_PASSED_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "2", "TRACKS", ts},
			{"CONTEXT_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "3", "TRACKS", ts},
			{"CONTEXT_PASSED_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "3", "TRACKS", ts},
			{"CONTEXT_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "4", "TRACKS", ts},
			{"CONTEXT_PASSED_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "4", "TRACKS", ts},
			{"CONTEXT_PASSED_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "5", "TRACKS", ts},
			{"CONTEXT_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "5", "TRACKS", ts},
			{"CONTEXT_PASSED_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "1", "PRODUCT_REVIEWED", ts},
			{"NESTED_ARRAY_EXAMPLE", "[[1 2] [3 4]]", "incompatible schema conversion from datetime to string", ts, "1", "PRODUCT_REVIEWED", ts},
			{"NESTED_ARRAY_OF_OBJECTS_EXAMPLE", "[map[id:1 name:Accessory 1] map[id:2 name:Accessory 2]]", "incompatible schema conversion from datetime to string", ts, "1", "PRODUCT_REVIEWED", ts},
			{"BINARY_EXAMPLE", "SGVsbG8sIFdvcmxkIQ==", "incompatible schema conversion from datetime to string", ts, "1", "PRODUCT_REVIEWED", ts},
			{"DIMENSIONS_UNIT", "cm", "incompatible schema conversion from datetime to string", ts, "1", "PRODUCT_REVIEWED", ts},
			{"GEO_POINT_EXAMPLE_LATITUDE", "37.7749", "incompatible schema conversion from datetime to float", ts, "1", "PRODUCT_REVIEWED", ts},
			{"URL_EXAMPLE", "https://example.com/product/12345", "incompatible schema conversion from datetime to string", ts, "1", "PRODUCT_REVIEWED", ts},
			{"ARRAY_OF_FLOATS_EXAMPLE", "[10.5 20.3 30.7]", "incompatible schema conversion from datetime to string", ts, "1", "PRODUCT_REVIEWED", ts},
			{"DIMENSIONS_WIDTH", "5", "incompatible schema conversion from datetime to int", ts, "1", "PRODUCT_REVIEWED", ts},
			{"ARRAY_OF_BOOLEANS_EXAMPLE", "[true false true]", "incompatible schema conversion from datetime to string", ts, "1", "PRODUCT_REVIEWED", ts},
			{"BOOLEAN_EXAMPLE", "true", "incompatible schema conversion from datetime to boolean", ts, "1", "PRODUCT_REVIEWED", ts},
			{"DIMENSIONS_HEIGHT", "10.5", "incompatible schema conversion from datetime to float", ts, "1", "PRODUCT_REVIEWED", ts},
			{"OBJECT_EXAMPLE_NESTED_INTEGER", "789", "incompatible schema conversion from datetime to int", ts, "1", "PRODUCT_REVIEWED", ts},
			{"ARRAY_EXAMPLE", "[electronics audio wireless]", "incompatible schema conversion from datetime to string", ts, "1", "PRODUCT_REVIEWED", ts},
			{"CONTEXT_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "1", "PRODUCT_REVIEWED", ts},
			{"GEO_POINT_EXAMPLE_LONGITUDE", "-122.4194", "incompatible schema conversion from datetime to float", ts, "1", "PRODUCT_REVIEWED", ts},
			{"ARRAY_OF_INTEGERS_EXAMPLE", "[1 2 3 4]", "incompatible schema conversion from datetime to string", ts, "1", "PRODUCT_REVIEWED", ts},
			{"FLOAT_EXAMPLE", "299.99", "incompatible schema conversion from datetime to float", ts, "1", "PRODUCT_REVIEWED", ts},
			{"OBJECT_EXAMPLE_NESTED_STRING", "Inner Text", "incompatible schema conversion from datetime to string", ts, "1", "PRODUCT_REVIEWED", ts},
			{"STRING_EXAMPLE", "Wireless Headphones", "incompatible schema conversion from datetime to string", ts, "1", "PRODUCT_REVIEWED", ts},
			{"DIMENSIONS_DEPTH", "3", "incompatible schema conversion from datetime to int", ts, "1", "PRODUCT_REVIEWED", ts},
			{"DURATION_EXAMPLE", "PT1H30M", "incompatible schema conversion from datetime to string", ts, "1", "PRODUCT_REVIEWED", ts},
			{"OBJECT_EXAMPLE_NESTED_BOOLEAN", "false", "incompatible schema conversion from datetime to boolean", ts, "1", "PRODUCT_REVIEWED", ts},
			{"URL_EXAMPLE", "https://example.com/product/12345", "incompatible schema conversion from datetime to string", ts, "2", "PRODUCT_REVIEWED", ts},
			{"ARRAY_EXAMPLE", "[electronics audio wireless]", "incompatible schema conversion from datetime to string", ts, "2", "PRODUCT_REVIEWED", ts},
			{"DIMENSIONS_HEIGHT", "10.5", "incompatible schema conversion from datetime to float", ts, "2", "PRODUCT_REVIEWED", ts},
			{"NESTED_ARRAY_EXAMPLE", "[[1 2] [3 4]]", "incompatible schema conversion from datetime to string", ts, "2", "PRODUCT_REVIEWED", ts},
			{"OBJECT_EXAMPLE_NESTED_BOOLEAN", "false", "incompatible schema conversion from datetime to boolean", ts, "2", "PRODUCT_REVIEWED", ts},
			{"ARRAY_OF_INTEGERS_EXAMPLE", "[1 2 3 4]", "incompatible schema conversion from datetime to string", ts, "2", "PRODUCT_REVIEWED", ts},
			{"BINARY_EXAMPLE", "SGVsbG8sIFdvcmxkIQ==", "incompatible schema conversion from datetime to string", ts, "2", "PRODUCT_REVIEWED", ts},
			{"GEO_POINT_EXAMPLE_LONGITUDE", "-122.4194", "incompatible schema conversion from datetime to float", ts, "2", "PRODUCT_REVIEWED", ts},
			{"STRING_EXAMPLE", "Wireless Headphones", "incompatible schema conversion from datetime to string", ts, "2", "PRODUCT_REVIEWED", ts},
			{"ARRAY_OF_FLOATS_EXAMPLE", "[10.5 20.3 30.7]", "incompatible schema conversion from datetime to string", ts, "2", "PRODUCT_REVIEWED", ts},
			{"OBJECT_EXAMPLE_NESTED_INTEGER", "789", "incompatible schema conversion from datetime to int", ts, "2", "PRODUCT_REVIEWED", ts},
			{"DIMENSIONS_UNIT", "cm", "incompatible schema conversion from datetime to string", ts, "2", "PRODUCT_REVIEWED", ts},
			{"NESTED_ARRAY_OF_OBJECTS_EXAMPLE", "[map[id:1 name:Accessory 1] map[id:2 name:Accessory 2]]", "incompatible schema conversion from datetime to string", ts, "2", "PRODUCT_REVIEWED", ts},
			{"CONTEXT_PASSED_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "2", "PRODUCT_REVIEWED", ts},
			{"DIMENSIONS_WIDTH", "5", "incompatible schema conversion from datetime to int", ts, "2", "PRODUCT_REVIEWED", ts},
			{"BOOLEAN_EXAMPLE", "true", "incompatible schema conversion from datetime to boolean", ts, "2", "PRODUCT_REVIEWED", ts},
			{"GEO_POINT_EXAMPLE_LATITUDE", "37.7749", "incompatible schema conversion from datetime to float", ts, "2", "PRODUCT_REVIEWED", ts},
			{"OBJECT_EXAMPLE_NESTED_STRING", "Inner Text", "incompatible schema conversion from datetime to string", ts, "2", "PRODUCT_REVIEWED", ts},
			{"DURATION_EXAMPLE", "PT1H30M", "incompatible schema conversion from datetime to string", ts, "2", "PRODUCT_REVIEWED", ts},
			{"CONTEXT_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "2", "PRODUCT_REVIEWED", ts},
			{"DIMENSIONS_DEPTH", "3", "incompatible schema conversion from datetime to int", ts, "2", "PRODUCT_REVIEWED", ts},
			{"ARRAY_OF_BOOLEANS_EXAMPLE", "[true false true]", "incompatible schema conversion from datetime to string", ts, "2", "PRODUCT_REVIEWED", ts},
			{"FLOAT_EXAMPLE", "299.99", "incompatible schema conversion from datetime to float", ts, "2", "PRODUCT_REVIEWED", ts},
			{"OBJECT_EXAMPLE_NESTED_INTEGER", "789", "incompatible schema conversion from datetime to int", ts, "3", "PRODUCT_REVIEWED", ts},
			{"CONTEXT_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "3", "PRODUCT_REVIEWED", ts},
			{"DIMENSIONS_HEIGHT", "10.5", "incompatible schema conversion from datetime to float", ts, "3", "PRODUCT_REVIEWED", ts},
			{"DIMENSIONS_UNIT", "cm", "incompatible schema conversion from datetime to string", ts, "3", "PRODUCT_REVIEWED", ts},
			{"DIMENSIONS_WIDTH", "5", "incompatible schema conversion from datetime to int", ts, "3", "PRODUCT_REVIEWED", ts},
			{"FLOAT_EXAMPLE", "299.99", "incompatible schema conversion from datetime to float", ts, "3", "PRODUCT_REVIEWED", ts},
			{"ARRAY_OF_BOOLEANS_EXAMPLE", "[true false true]", "incompatible schema conversion from datetime to string", ts, "3", "PRODUCT_REVIEWED", ts},
			{"ARRAY_OF_FLOATS_EXAMPLE", "[10.5 20.3 30.7]", "incompatible schema conversion from datetime to string", ts, "3", "PRODUCT_REVIEWED", ts},
			{"NESTED_ARRAY_OF_OBJECTS_EXAMPLE", "[map[id:1 name:Accessory 1] map[id:2 name:Accessory 2]]", "incompatible schema conversion from datetime to string", ts, "3", "PRODUCT_REVIEWED", ts},
			{"OBJECT_EXAMPLE_NESTED_BOOLEAN", "false", "incompatible schema conversion from datetime to boolean", ts, "3", "PRODUCT_REVIEWED", ts},
			{"OBJECT_EXAMPLE_NESTED_STRING", "Inner Text", "incompatible schema conversion from datetime to string", ts, "3", "PRODUCT_REVIEWED", ts},
			{"STRING_EXAMPLE", "Wireless Headphones", "incompatible schema conversion from datetime to string", ts, "3", "PRODUCT_REVIEWED", ts},
			{"GEO_POINT_EXAMPLE_LONGITUDE", "-122.4194", "incompatible schema conversion from datetime to float", ts, "3", "PRODUCT_REVIEWED", ts},
			{"NESTED_ARRAY_EXAMPLE", "[[1 2] [3 4]]", "incompatible schema conversion from datetime to string", ts, "3", "PRODUCT_REVIEWED", ts},
			{"GEO_POINT_EXAMPLE_LATITUDE", "37.7749", "incompatible schema conversion from datetime to float", ts, "3", "PRODUCT_REVIEWED", ts},
			{"URL_EXAMPLE", "https://example.com/product/12345", "incompatible schema conversion from datetime to string", ts, "3", "PRODUCT_REVIEWED", ts},
			{"ARRAY_OF_INTEGERS_EXAMPLE", "[1 2 3 4]", "incompatible schema conversion from datetime to string", ts, "3", "PRODUCT_REVIEWED", ts},
			{"BINARY_EXAMPLE", "SGVsbG8sIFdvcmxkIQ==", "incompatible schema conversion from datetime to string", ts, "3", "PRODUCT_REVIEWED", ts},
			{"DURATION_EXAMPLE", "PT1H30M", "incompatible schema conversion from datetime to string", ts, "3", "PRODUCT_REVIEWED", ts},
			{"BOOLEAN_EXAMPLE", "true", "incompatible schema conversion from datetime to boolean", ts, "3", "PRODUCT_REVIEWED", ts},
			{"CONTEXT_PASSED_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "3", "PRODUCT_REVIEWED", ts},
			{"DIMENSIONS_DEPTH", "3", "incompatible schema conversion from datetime to int", ts, "3", "PRODUCT_REVIEWED", ts},
			{"ARRAY_EXAMPLE", "[electronics audio wireless]", "incompatible schema conversion from datetime to string", ts, "3", "PRODUCT_REVIEWED", ts},
			{"ARRAY_OF_INTEGERS_EXAMPLE", "[1 2 3 4]", "incompatible schema conversion from datetime to string", ts, "4", "PRODUCT_REVIEWED", ts},
			{"DIMENSIONS_UNIT", "cm", "incompatible schema conversion from datetime to string", ts, "4", "PRODUCT_REVIEWED", ts},
			{"DURATION_EXAMPLE", "PT1H30M", "incompatible schema conversion from datetime to string", ts, "4", "PRODUCT_REVIEWED", ts},
			{"OBJECT_EXAMPLE_NESTED_BOOLEAN", "false", "incompatible schema conversion from datetime to boolean", ts, "4", "PRODUCT_REVIEWED", ts},
			{"DIMENSIONS_WIDTH", "5", "incompatible schema conversion from datetime to int", ts, "4", "PRODUCT_REVIEWED", ts},
			{"OBJECT_EXAMPLE_NESTED_INTEGER", "789", "incompatible schema conversion from datetime to int", ts, "4", "PRODUCT_REVIEWED", ts},
			{"ARRAY_OF_BOOLEANS_EXAMPLE", "[true false true]", "incompatible schema conversion from datetime to string", ts, "4", "PRODUCT_REVIEWED", ts},
			{"BINARY_EXAMPLE", "SGVsbG8sIFdvcmxkIQ==", "incompatible schema conversion from datetime to string", ts, "4", "PRODUCT_REVIEWED", ts},
			{"CONTEXT_PASSED_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "4", "PRODUCT_REVIEWED", ts},
			{"NESTED_ARRAY_OF_OBJECTS_EXAMPLE", "[map[id:1 name:Accessory 1] map[id:2 name:Accessory 2]]", "incompatible schema conversion from datetime to string", ts, "4", "PRODUCT_REVIEWED", ts},
			{"ARRAY_OF_FLOATS_EXAMPLE", "[10.5 20.3 30.7]", "incompatible schema conversion from datetime to string", ts, "4", "PRODUCT_REVIEWED", ts},
			{"DIMENSIONS_DEPTH", "3", "incompatible schema conversion from datetime to int", ts, "4", "PRODUCT_REVIEWED", ts},
			{"FLOAT_EXAMPLE", "299.99", "incompatible schema conversion from datetime to float", ts, "4", "PRODUCT_REVIEWED", ts},
			{"GEO_POINT_EXAMPLE_LATITUDE", "37.7749", "incompatible schema conversion from datetime to float", ts, "4", "PRODUCT_REVIEWED", ts},
			{"CONTEXT_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "4", "PRODUCT_REVIEWED", ts},
			{"DIMENSIONS_HEIGHT", "10.5", "incompatible schema conversion from datetime to float", ts, "4", "PRODUCT_REVIEWED", ts},
			{"GEO_POINT_EXAMPLE_LONGITUDE", "-122.4194", "incompatible schema conversion from datetime to float", ts, "4", "PRODUCT_REVIEWED", ts},
			{"OBJECT_EXAMPLE_NESTED_STRING", "Inner Text", "incompatible schema conversion from datetime to string", ts, "4", "PRODUCT_REVIEWED", ts},
			{"STRING_EXAMPLE", "Wireless Headphones", "incompatible schema conversion from datetime to string", ts, "4", "PRODUCT_REVIEWED", ts},
			{"URL_EXAMPLE", "https://example.com/product/12345", "incompatible schema conversion from datetime to string", ts, "4", "PRODUCT_REVIEWED", ts},
			{"ARRAY_EXAMPLE", "[electronics audio wireless]", "incompatible schema conversion from datetime to string", ts, "4", "PRODUCT_REVIEWED", ts},
			{"BOOLEAN_EXAMPLE", "true", "incompatible schema conversion from datetime to boolean", ts, "4", "PRODUCT_REVIEWED", ts},
			{"NESTED_ARRAY_EXAMPLE", "[[1 2] [3 4]]", "incompatible schema conversion from datetime to string", ts, "4", "PRODUCT_REVIEWED", ts},
			{"ARRAY_OF_FLOATS_EXAMPLE", "[10.5 20.3 30.7]", "incompatible schema conversion from datetime to string", ts, "5", "PRODUCT_REVIEWED", ts},
			{"ARRAY_OF_INTEGERS_EXAMPLE", "[1 2 3 4]", "incompatible schema conversion from datetime to string", ts, "5", "PRODUCT_REVIEWED", ts},
			{"BINARY_EXAMPLE", "SGVsbG8sIFdvcmxkIQ==", "incompatible schema conversion from datetime to string", ts, "5", "PRODUCT_REVIEWED", ts},
			{"DURATION_EXAMPLE", "PT1H30M", "incompatible schema conversion from datetime to string", ts, "5", "PRODUCT_REVIEWED", ts},
			{"OBJECT_EXAMPLE_NESTED_STRING", "Inner Text", "incompatible schema conversion from datetime to string", ts, "5", "PRODUCT_REVIEWED", ts},
			{"STRING_EXAMPLE", "Wireless Headphones", "incompatible schema conversion from datetime to string", ts, "5", "PRODUCT_REVIEWED", ts},
			{"BOOLEAN_EXAMPLE", "true", "incompatible schema conversion from datetime to boolean", ts, "5", "PRODUCT_REVIEWED", ts},
			{"CONTEXT_PASSED_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "5", "PRODUCT_REVIEWED", ts},
			{"DIMENSIONS_UNIT", "cm", "incompatible schema conversion from datetime to string", ts, "5", "PRODUCT_REVIEWED", ts},
			{"ARRAY_OF_BOOLEANS_EXAMPLE", "[true false true]", "incompatible schema conversion from datetime to string", ts, "5", "PRODUCT_REVIEWED", ts},
			{"DIMENSIONS_HEIGHT", "10.5", "incompatible schema conversion from datetime to float", ts, "5", "PRODUCT_REVIEWED", ts},
			{"FLOAT_EXAMPLE", "299.99", "incompatible schema conversion from datetime to float", ts, "5", "PRODUCT_REVIEWED", ts},
			{"NESTED_ARRAY_EXAMPLE", "[[1 2] [3 4]]", "incompatible schema conversion from datetime to string", ts, "5", "PRODUCT_REVIEWED", ts},
			{"URL_EXAMPLE", "https://example.com/product/12345", "incompatible schema conversion from datetime to string", ts, "5", "PRODUCT_REVIEWED", ts},
			{"OBJECT_EXAMPLE_NESTED_BOOLEAN", "false", "incompatible schema conversion from datetime to boolean", ts, "5", "PRODUCT_REVIEWED", ts},
			{"DIMENSIONS_DEPTH", "3", "incompatible schema conversion from datetime to int", ts, "5", "PRODUCT_REVIEWED", ts},
			{"DIMENSIONS_WIDTH", "5", "incompatible schema conversion from datetime to int", ts, "5", "PRODUCT_REVIEWED", ts},
			{"GEO_POINT_EXAMPLE_LONGITUDE", "-122.4194", "incompatible schema conversion from datetime to float", ts, "5", "PRODUCT_REVIEWED", ts},
			{"NESTED_ARRAY_OF_OBJECTS_EXAMPLE", "[map[id:1 name:Accessory 1] map[id:2 name:Accessory 2]]", "incompatible schema conversion from datetime to string", ts, "5", "PRODUCT_REVIEWED", ts},
			{"ARRAY_EXAMPLE", "[electronics audio wireless]", "incompatible schema conversion from datetime to string", ts, "5", "PRODUCT_REVIEWED", ts},
			{"GEO_POINT_EXAMPLE_LATITUDE", "37.7749", "incompatible schema conversion from datetime to float", ts, "5", "PRODUCT_REVIEWED", ts},
			{"OBJECT_EXAMPLE_NESTED_INTEGER", "789", "incompatible schema conversion from datetime to int", ts, "5", "PRODUCT_REVIEWED", ts},
			{"CONTEXT_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "5", "PRODUCT_REVIEWED", ts},
		}, discardsRecordsInDB)

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("discards migration for reason", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := testhelper.RandSchema()
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, snowpipeClientsURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		t.Log("Creating schema and tables. Discards is created without reason")
		t.Log("CONTEXT_IP, CONTEXT_REQUEST_IP are of type int")
		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, whutils.NewNoOpUploader()))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { testhelper.DropSchema(t, sm.DB.DB, namespace) })
		require.NoError(t, sm.CreateTable(ctx, "PRODUCT_REVIEWED", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_IP": "int", "CONTEXT_REQUEST_IP": "string", "CONTEXT_PASSED_IP": "int", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "EVENT": "string", "EVENT_TEXT": "string", "ID": "string", "ORIGINAL_TIMESTAMP": "datetime", "PRODUCT_ID": "string", "RATING": "int", "RECEIVED_AT": "datetime", "REVIEW_BODY": "string", "REVIEW_ID": "string", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "USER_ID": "string", "UUID_TS": "datetime",
		}))
		require.NoError(t, sm.CreateTable(ctx, "TRACKS", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_IP": "int", "CONTEXT_REQUEST_IP": "string", "CONTEXT_PASSED_IP": "int", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "EVENT": "string", "EVENT_TEXT": "string", "ID": "string", "ORIGINAL_TIMESTAMP": "datetime", "RECEIVED_AT": "datetime", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "USER_ID": "string", "UUID_TS": "datetime",
		}))
		require.NoError(t, sm.CreateTable(ctx, "RUDDER_DISCARDS", whutils.ModelTableSchema{
			"COLUMN_NAME": "string", "COLUMN_VALUE": "string", "RECEIVED_AT": "datetime", "ROW_ID": "string", "TABLE_NAME": "string", "UUID_TS": "datetime",
		}))

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"type":"track","messageId":"%[1]s","userId":"%[1]s","event":"Product Reviewed","properties":{"review_id":"86ac1cd43","product_id":"9578257311","rating":3,"review_body":"OK for the price. It works but the material feels flimsy."}, "timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z", "context":{"ip":"14.5.67.21"}}]}`,
				strconv.Itoa(index+1),
			)
		}
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"PRODUCT_REVIEWED": {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "NUMBER", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "NUMBER", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "PRODUCT_ID": "TEXT", "RATING": "NUMBER", "RECEIVED_AT": "TIMESTAMP_TZ", "REVIEW_BODY": "TEXT", "REVIEW_ID": "TEXT", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"TRACKS":           {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "NUMBER", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "NUMBER", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"RUDDER_DISCARDS":  {"COLUMN_NAME": "TEXT", "COLUMN_VALUE": "TEXT", "REASON": "TEXT", "RECEIVED_AT": "TIMESTAMP_TZ", "ROW_ID": "TEXT", "TABLE_NAME": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		produceReviewedRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, PRODUCT_ID, RATING, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), REVIEW_BODY, REVIEW_ID, SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "PRODUCT_REVIEWED"))
		require.ElementsMatch(t, productReviewedRecordsForDiscards(source, destination), produceReviewedRecordsFromDB)
		tracksRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "TRACKS"))
		require.ElementsMatch(t, tracksRecordsForDiscards(source, destination), tracksRecordsFromDB)
		discardsRecordsInDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT COLUMN_NAME, COLUMN_VALUE, REASON, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), ROW_ID, TABLE_NAME, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "RUDDER_DISCARDS"))
		require.ElementsMatch(t, discardsRecords(), discardsRecordsInDB)

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("discards migrated", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := testhelper.RandSchema()
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, snowpipeClientsURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		t.Log("Creating schema and tables. Discards is created with reason")
		t.Log("CONTEXT_IP, CONTEXT_REQUEST_IP are of type int")
		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, whutils.NewNoOpUploader()))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { testhelper.DropSchema(t, sm.DB.DB, namespace) })
		require.NoError(t, sm.CreateTable(ctx, "PRODUCT_REVIEWED", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_IP": "int", "CONTEXT_REQUEST_IP": "string", "CONTEXT_PASSED_IP": "int", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "EVENT": "string", "EVENT_TEXT": "string", "ID": "string", "ORIGINAL_TIMESTAMP": "datetime", "PRODUCT_ID": "string", "RATING": "int", "RECEIVED_AT": "datetime", "REVIEW_BODY": "string", "REVIEW_ID": "string", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "USER_ID": "string", "UUID_TS": "datetime",
		}))
		require.NoError(t, sm.CreateTable(ctx, "TRACKS", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_IP": "int", "CONTEXT_REQUEST_IP": "string", "CONTEXT_PASSED_IP": "int", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "EVENT": "string", "EVENT_TEXT": "string", "ID": "string", "ORIGINAL_TIMESTAMP": "datetime", "RECEIVED_AT": "datetime", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "USER_ID": "string", "UUID_TS": "datetime",
		}))
		require.NoError(t, sm.CreateTable(ctx, "RUDDER_DISCARDS", whutils.ModelTableSchema{
			"COLUMN_NAME": "string", "COLUMN_VALUE": "string", "RECEIVED_AT": "datetime", "ROW_ID": "string", "TABLE_NAME": "string", "UUID_TS": "datetime", "REASON": "string",
		}))

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"type":"track","messageId":"%[1]s","userId":"%[1]s","event":"Product Reviewed","properties":{"review_id":"86ac1cd43","product_id":"9578257311","rating":3,"review_body":"OK for the price. It works but the material feels flimsy."}, "timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z", "context":{"ip":"14.5.67.21"}}]}`,
				strconv.Itoa(index+1),
			)
		}
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"PRODUCT_REVIEWED": {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "NUMBER", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "NUMBER", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "PRODUCT_ID": "TEXT", "RATING": "NUMBER", "RECEIVED_AT": "TIMESTAMP_TZ", "REVIEW_BODY": "TEXT", "REVIEW_ID": "TEXT", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"TRACKS":           {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "NUMBER", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "NUMBER", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"RUDDER_DISCARDS":  {"COLUMN_NAME": "TEXT", "COLUMN_VALUE": "TEXT", "REASON": "TEXT", "RECEIVED_AT": "TIMESTAMP_TZ", "ROW_ID": "TEXT", "TABLE_NAME": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		produceReviewedRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, PRODUCT_ID, RATING, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), REVIEW_BODY, REVIEW_ID, SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "PRODUCT_REVIEWED"))
		require.ElementsMatch(t, productReviewedRecordsForDiscards(source, destination), produceReviewedRecordsFromDB)
		tracksRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "TRACKS"))
		require.ElementsMatch(t, tracksRecordsForDiscards(source, destination), tracksRecordsFromDB)
		discardsRecordsInDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT COLUMN_NAME, COLUMN_VALUE, REASON, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), ROW_ID, TABLE_NAME, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "RUDDER_DISCARDS"))
		require.ElementsMatch(t, discardsRecords(), discardsRecordsInDB)

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("don't re-create channel on loading twice when successful", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := testhelper.RandSchema()
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, snowpipeClientsURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		t.Log("Creating schema and tables")
		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, whutils.NewNoOpUploader()))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { testhelper.DropSchema(t, sm.DB.DB, namespace) })
		require.NoError(t, sm.CreateTable(ctx, "PRODUCT_REVIEWED", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_IP": "string", "CONTEXT_REQUEST_IP": "string", "CONTEXT_PASSED_IP": "string", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "EVENT": "string", "EVENT_TEXT": "string", "ID": "string", "ORIGINAL_TIMESTAMP": "datetime", "PRODUCT_ID": "string", "RATING": "int", "RECEIVED_AT": "datetime", "REVIEW_BODY": "string", "REVIEW_ID": "string", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "USER_ID": "string", "UUID_TS": "datetime",
		}))
		require.NoError(t, sm.CreateTable(ctx, "TRACKS", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_IP": "string", "CONTEXT_REQUEST_IP": "string", "CONTEXT_PASSED_IP": "string", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "EVENT": "string", "EVENT_TEXT": "string", "ID": "string", "ORIGINAL_TIMESTAMP": "datetime", "RECEIVED_AT": "datetime", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "USER_ID": "string", "UUID_TS": "datetime",
		}))

		t.Log("Sending 5 events")
		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"type":"track","messageId":"%[1]s","userId":"%[1]s","event":"Product Reviewed","properties":{"review_id":"86ac1cd43","product_id":"9578257311","rating":3,"review_body":"OK for the price. It works but the material feels flimsy."}, "timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z", "context":{"ip":"14.5.67.21"}}]}`,
				strconv.Itoa(index+1),
			)
		}
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		t.Log("Sending 5 events again")
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 20)

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"PRODUCT_REVIEWED": {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "PRODUCT_ID": "TEXT", "RATING": "NUMBER", "RECEIVED_AT": "TIMESTAMP_TZ", "REVIEW_BODY": "TEXT", "REVIEW_ID": "TEXT", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"TRACKS":           {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"RUDDER_DISCARDS":  {"COLUMN_NAME": "TEXT", "COLUMN_VALUE": "TEXT", "REASON": "TEXT", "RECEIVED_AT": "TIMESTAMP_TZ", "ROW_ID": "TEXT", "TABLE_NAME": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		produceReviewedRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, PRODUCT_ID, RATING, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), REVIEW_BODY, REVIEW_ID, SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "PRODUCT_REVIEWED"))
		require.ElementsMatch(t, append(productReviewedRecords(source, destination), productReviewedRecords(source, destination)...), produceReviewedRecordsFromDB)
		tracksRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "TRACKS"))
		require.ElementsMatch(t, append(tracksRecords(source, destination), tracksRecords(source, destination)...), tracksRecordsFromDB)

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("many tables", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := testhelper.RandSchema()
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, snowpipeClientsURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		t.Log("Creating schema and tables")
		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, sm.Setup(ctx, warehouse, whutils.NewNoOpUploader()))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { testhelper.DropSchema(t, sm.DB.DB, namespace) })

		for i := 0; i < 10; i++ {
			eventFormat := func(int) string {
				return fmt.Sprintf(`{"batch":[{"type":"track","userId":"%[1]s","event":"Product Reviewed %[1]s","properties":{"review_id":"86ac1cd43","product_id":"9578257311","rating":3,"review_body":"OK for the price. It works but the material feels flimsy."}, "timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z", "context":{"ip":"14.5.67.21"}}]}`,
					strconv.Itoa(i+1),
				)
			}
			require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		}

		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5*10)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 2*5*10)

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		expectedSchema := lo.SliceToMap(
			lo.RepeatBy(10, func(index int) string {
				return "PRODUCT_REVIEWED_" + strconv.Itoa(index+1)
			}),
			func(tableName string) (string, map[string]string) {
				return tableName, map[string]string{"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "PRODUCT_ID": "TEXT", "RATING": "NUMBER", "RECEIVED_AT": "TIMESTAMP_TZ", "REVIEW_BODY": "TEXT", "REVIEW_ID": "TEXT", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"}
			},
		)
		expectedSchema = lo.Assign(expectedSchema, map[string]map[string]string{
			"TRACKS":          {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"RUDDER_DISCARDS": {"COLUMN_NAME": "TEXT", "COLUMN_VALUE": "TEXT", "REASON": "TEXT", "RECEIVED_AT": "TIMESTAMP_TZ", "ROW_ID": "TEXT", "TABLE_NAME": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
		})
		require.Equal(t, expectedSchema, convertRecordsToSchema(schema))

		for i := 0; i < 10; i++ {
			productIDIndex := i + 1
			userID := strconv.Itoa(productIDIndex)
			eventName := "Product Reviewed " + strconv.Itoa(productIDIndex)
			tableName := strcase.ToSnake(eventName)
			recordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ORIGINAL_TIMESTAMP, PRODUCT_ID, RATING, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), REVIEW_BODY, REVIEW_ID, SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "PRODUCT_REVIEWED_"+strconv.Itoa(productIDIndex)))
			ts := timeutil.Now().Format("2006-01-02")

			expectedProductReviewedRecords := [][]string{
				{destination.ID, "SNOWPIPE_STREAMING", source.ID, source.SourceDefinition.Name, tableName, eventName, "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", userID, ts},
				{destination.ID, "SNOWPIPE_STREAMING", source.ID, source.SourceDefinition.Name, tableName, eventName, "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", userID, ts},
				{destination.ID, "SNOWPIPE_STREAMING", source.ID, source.SourceDefinition.Name, tableName, eventName, "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", userID, ts},
				{destination.ID, "SNOWPIPE_STREAMING", source.ID, source.SourceDefinition.Name, tableName, eventName, "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", userID, ts},
				{destination.ID, "SNOWPIPE_STREAMING", source.ID, source.SourceDefinition.Name, tableName, eventName, "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", userID, ts},
			}
			require.ElementsMatch(t, expectedProductReviewedRecords, recordsFromDB)
		}

		trackRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "TRACKS"))
		expectedTrackRecords := lo.RepeatBy(50, func(index int) []string {
			productIDIndex := index/5 + 1
			userID := strconv.Itoa(productIDIndex)
			eventName := "Product Reviewed " + strconv.Itoa(productIDIndex)
			tableName := strcase.ToSnake(eventName)
			ts := timeutil.Now().Format("2006-01-02")

			return []string{destination.ID, "SNOWPIPE_STREAMING", source.ID, source.SourceDefinition.Name, tableName, eventName, "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", userID, ts}
		})
		require.ElementsMatch(t, expectedTrackRecords, trackRecordsFromDB)

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("schema modified after channel creation (schema deleted)", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := testhelper.RandSchema()
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, snowpipeClientsURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, whutils.NewNoOpUploader()))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { testhelper.DropSchema(t, sm.DB.DB, namespace) })
		require.NoError(t, sm.CreateTable(ctx, "PRODUCT_REVIEWED", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_IP": "string", "CONTEXT_REQUEST_IP": "string", "CONTEXT_PASSED_IP": "string", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "EVENT": "string", "EVENT_TEXT": "string", "ID": "string", "ORIGINAL_TIMESTAMP": "datetime", "PRODUCT_ID": "string", "RATING": "int", "RECEIVED_AT": "datetime", "REVIEW_BODY": "string", "REVIEW_ID": "string", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "USER_ID": "string", "UUID_TS": "datetime",
		}))
		require.NoError(t, sm.CreateTable(ctx, "TRACKS", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_IP": "string", "CONTEXT_REQUEST_IP": "string", "CONTEXT_PASSED_IP": "string", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "EVENT": "string", "EVENT_TEXT": "string", "ID": "string", "ORIGINAL_TIMESTAMP": "datetime", "RECEIVED_AT": "datetime", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "USER_ID": "string", "UUID_TS": "datetime",
		}))

		t.Log("Sending 5 events")
		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"type":"track","messageId":"%[1]s","userId":"%[1]s","event":"Product Reviewed","properties":{"review_id":"86ac1cd43","product_id":"9578257311","rating":3,"review_body":"OK for the price. It works but the material feels flimsy."}, "timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z", "context":{"ip":"14.5.67.21"}}]}`,
				strconv.Itoa(index+1),
			)
		}
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		t.Log("Schema modified, Dropping schema")
		testhelper.DropSchema(t, sm.DB.DB, namespace)

		t.Log("Sending 5 events again")
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 20)

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"PRODUCT_REVIEWED": {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "PRODUCT_ID": "TEXT", "RATING": "NUMBER", "RECEIVED_AT": "TIMESTAMP_TZ", "REVIEW_BODY": "TEXT", "REVIEW_ID": "TEXT", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"TRACKS":           {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		produceReviewedRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, PRODUCT_ID, RATING, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), REVIEW_BODY, REVIEW_ID, SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "PRODUCT_REVIEWED"))
		require.ElementsMatch(t, productReviewedRecords(source, destination), produceReviewedRecordsFromDB)
		tracksRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "TRACKS"))
		require.ElementsMatch(t, tracksRecords(source, destination), tracksRecordsFromDB)

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("schema modified after channel creation (table deleted)", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := testhelper.RandSchema()
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, snowpipeClientsURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, whutils.NewNoOpUploader()))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { testhelper.DropSchema(t, sm.DB.DB, namespace) })
		require.NoError(t, sm.CreateTable(ctx, "PRODUCT_REVIEWED", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_IP": "string", "CONTEXT_REQUEST_IP": "string", "CONTEXT_PASSED_IP": "string", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "EVENT": "string", "EVENT_TEXT": "string", "ID": "string", "ORIGINAL_TIMESTAMP": "datetime", "PRODUCT_ID": "string", "RATING": "int", "RECEIVED_AT": "datetime", "REVIEW_BODY": "string", "REVIEW_ID": "string", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "USER_ID": "string", "UUID_TS": "datetime",
		}))
		require.NoError(t, sm.CreateTable(ctx, "TRACKS", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_IP": "string", "CONTEXT_REQUEST_IP": "string", "CONTEXT_PASSED_IP": "string", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "EVENT": "string", "EVENT_TEXT": "string", "ID": "string", "ORIGINAL_TIMESTAMP": "datetime", "RECEIVED_AT": "datetime", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "USER_ID": "string", "UUID_TS": "datetime",
		}))

		t.Log("Sending 5 events")
		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"type":"track","messageId":"%[1]s","userId":"%[1]s","event":"Product Reviewed","properties":{"review_id":"86ac1cd43","product_id":"9578257311","rating":3,"review_body":"OK for the price. It works but the material feels flimsy."}, "timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z", "context":{"ip":"14.5.67.21"}}]}`,
				strconv.Itoa(index+1),
			)
		}
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		t.Log("Schema modified, Dropping table")
		_, err = sm.DB.DB.ExecContext(ctx, fmt.Sprintf("DROP TABLE %q.%q;", namespace, "TRACKS"))
		require.NoError(t, err)

		t.Log("Sending 5 events again")
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 20)

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"PRODUCT_REVIEWED": {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "PRODUCT_ID": "TEXT", "RATING": "NUMBER", "RECEIVED_AT": "TIMESTAMP_TZ", "REVIEW_BODY": "TEXT", "REVIEW_ID": "TEXT", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"TRACKS":           {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"RUDDER_DISCARDS":  {"COLUMN_NAME": "TEXT", "COLUMN_VALUE": "TEXT", "REASON": "TEXT", "RECEIVED_AT": "TIMESTAMP_TZ", "ROW_ID": "TEXT", "TABLE_NAME": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		produceReviewedRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, PRODUCT_ID, RATING, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), REVIEW_BODY, REVIEW_ID, SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "PRODUCT_REVIEWED"))
		require.ElementsMatch(t, append(productReviewedRecords(source, destination), productReviewedRecords(source, destination)...), produceReviewedRecordsFromDB)
		tracksRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "TRACKS"))
		require.ElementsMatch(t, tracksRecords(source, destination), tracksRecordsFromDB)

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("schema modified after channel creation (columns deleted)", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := testhelper.RandSchema()
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, snowpipeClientsURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, whutils.NewNoOpUploader()))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { testhelper.DropSchema(t, sm.DB.DB, namespace) })
		require.NoError(t, sm.CreateTable(ctx, "PRODUCT_REVIEWED", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_IP": "string", "CONTEXT_REQUEST_IP": "string", "CONTEXT_PASSED_IP": "string", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "EVENT": "string", "EVENT_TEXT": "string", "ID": "string", "ORIGINAL_TIMESTAMP": "datetime", "PRODUCT_ID": "string", "RATING": "int", "RECEIVED_AT": "datetime", "REVIEW_BODY": "string", "REVIEW_ID": "string", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "USER_ID": "string", "UUID_TS": "datetime",
		}))
		require.NoError(t, sm.CreateTable(ctx, "TRACKS", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_IP": "string", "CONTEXT_REQUEST_IP": "string", "CONTEXT_PASSED_IP": "string", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "EVENT": "string", "EVENT_TEXT": "string", "ID": "string", "ORIGINAL_TIMESTAMP": "datetime", "RECEIVED_AT": "datetime", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "USER_ID": "string", "UUID_TS": "datetime",
		}))

		t.Log("Sending 5 events")
		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"type":"track","messageId":"%[1]s","userId":"%[1]s","event":"Product Reviewed","properties":{"review_id":"86ac1cd43","product_id":"9578257311","rating":3,"review_body":"OK for the price. It works but the material feels flimsy."}, "timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z", "context":{"ip":"14.5.67.21"}}]}`,
				strconv.Itoa(index+1),
			)
		}
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		t.Log("Schema modified, Dropping columns for TRACKS table")
		_, err = sm.DB.DB.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s.TRACKS DROP COLUMN CONTEXT_IP, CONTEXT_PASSED_IP;", namespace))
		require.NoError(t, err)

		t.Log("Sending 5 events again")
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 20)

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"PRODUCT_REVIEWED": {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "PRODUCT_ID": "TEXT", "RATING": "NUMBER", "RECEIVED_AT": "TIMESTAMP_TZ", "REVIEW_BODY": "TEXT", "REVIEW_ID": "TEXT", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"TRACKS":           {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"RUDDER_DISCARDS":  {"COLUMN_NAME": "TEXT", "COLUMN_VALUE": "TEXT", "REASON": "TEXT", "RECEIVED_AT": "TIMESTAMP_TZ", "ROW_ID": "TEXT", "TABLE_NAME": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		ts := timeutil.Now().Format("2006-01-02")
		eventName := "Product Reviewed"
		tableName := strcase.ToSnake(eventName)
		recordsBeforeDeletion := [][]string{
			{destination.ID, "SNOWPIPE_STREAMING", "", "", source.ID, source.SourceDefinition.Name, tableName, eventName, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "1", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "", source.ID, source.SourceDefinition.Name, tableName, eventName, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "2", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "", source.ID, source.SourceDefinition.Name, tableName, eventName, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "3", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "", source.ID, source.SourceDefinition.Name, tableName, eventName, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "4", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "", source.ID, source.SourceDefinition.Name, tableName, eventName, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "5", ts},
		}
		tracksRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "TRACKS"))
		require.ElementsMatch(t, append(tracksRecords(source, destination), recordsBeforeDeletion...), tracksRecordsFromDB)
		produceReviewedRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, PRODUCT_ID, RATING, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), REVIEW_BODY, REVIEW_ID, SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "PRODUCT_REVIEWED"))
		require.ElementsMatch(t, append(productReviewedRecords(source, destination), productReviewedRecords(source, destination)...), produceReviewedRecordsFromDB)

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("schema modified after channel creation (datatype changed for all tables)", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := testhelper.RandSchema()
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, snowpipeClientsURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, whutils.NewNoOpUploader()))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { testhelper.DropSchema(t, sm.DB.DB, namespace) })
		require.NoError(t, sm.CreateTable(ctx, "PRODUCT_REVIEWED", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_IP": "string", "CONTEXT_REQUEST_IP": "string", "CONTEXT_PASSED_IP": "string", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "EVENT": "string", "EVENT_TEXT": "string", "ID": "string", "ORIGINAL_TIMESTAMP": "datetime", "PRODUCT_ID": "string", "RATING": "int", "RECEIVED_AT": "datetime", "REVIEW_BODY": "string", "REVIEW_ID": "string", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "USER_ID": "string", "UUID_TS": "datetime",
		}))
		require.NoError(t, sm.CreateTable(ctx, "TRACKS", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_IP": "string", "CONTEXT_REQUEST_IP": "string", "CONTEXT_PASSED_IP": "string", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "EVENT": "string", "EVENT_TEXT": "string", "ID": "string", "ORIGINAL_TIMESTAMP": "datetime", "RECEIVED_AT": "datetime", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "USER_ID": "string", "UUID_TS": "datetime",
		}))
		require.NoError(t, sm.CreateTable(ctx, "RUDDER_DISCARDS", whutils.ModelTableSchema{
			"COLUMN_NAME": "string", "COLUMN_VALUE": "string", "RECEIVED_AT": "datetime", "ROW_ID": "string", "TABLE_NAME": "string", "UUID_TS": "datetime", "REASON": "string",
		}))

		t.Log("Sending 5 events")
		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"type":"track","messageId":"%[1]s","userId":"%[1]s","event":"Product Reviewed","properties":{"review_id":"86ac1cd43","product_id":"9578257311","rating":3,"review_body":"OK for the price. It works but the material feels flimsy."}, "timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z", "context":{"ip":"14.5.67.21"}}]}`,
				strconv.Itoa(index+1),
			)
		}
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		t.Log("Schema modified, CONTEXT_IP, CONTEXT_PASSED_IP are of type int")
		_, err = sm.DB.DB.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s.TRACKS DROP COLUMN CONTEXT_IP, CONTEXT_PASSED_IP;", namespace))
		require.NoError(t, err)
		_, err = sm.DB.DB.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s.PRODUCT_REVIEWED DROP COLUMN CONTEXT_IP, CONTEXT_PASSED_IP;", namespace))
		require.NoError(t, err)
		_, err = sm.DB.DB.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s.TRACKS ADD COLUMN CONTEXT_IP NUMBER, CONTEXT_PASSED_IP NUMBER;", namespace))
		require.NoError(t, err)
		_, err = sm.DB.DB.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s.PRODUCT_REVIEWED ADD COLUMN CONTEXT_IP NUMBER, CONTEXT_PASSED_IP NUMBER;", namespace))
		require.NoError(t, err)

		t.Log("Sending 5 events again")
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 20)

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"PRODUCT_REVIEWED": {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "NUMBER", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "NUMBER", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "PRODUCT_ID": "TEXT", "RATING": "NUMBER", "RECEIVED_AT": "TIMESTAMP_TZ", "REVIEW_BODY": "TEXT", "REVIEW_ID": "TEXT", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"TRACKS":           {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "NUMBER", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "NUMBER", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"RUDDER_DISCARDS":  {"COLUMN_NAME": "TEXT", "COLUMN_VALUE": "TEXT", "REASON": "TEXT", "RECEIVED_AT": "TIMESTAMP_TZ", "ROW_ID": "TEXT", "TABLE_NAME": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		produceReviewedRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, PRODUCT_ID, RATING, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), REVIEW_BODY, REVIEW_ID, SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "PRODUCT_REVIEWED"))
		require.ElementsMatch(t, append(productReviewedRecordsForDiscards(source, destination), productReviewedRecordsForDiscards(source, destination)...), produceReviewedRecordsFromDB)
		tracksRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "TRACKS"))
		require.ElementsMatch(t, append(tracksRecordsForDiscards(source, destination), tracksRecordsForDiscards(source, destination)...), tracksRecordsFromDB)
		discardsRecordsInDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT COLUMN_NAME, COLUMN_VALUE, REASON, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), ROW_ID, TABLE_NAME, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "RUDDER_DISCARDS"))
		require.ElementsMatch(t, discardsRecords(), discardsRecordsInDB)

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("JSON columns", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := testhelper.RandSchema()
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)
		destination.Config["jsonPaths"] = "track.properties.objectInfo,track.properties.arrayInfo"

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, snowpipeClientsURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, sm.Setup(ctx, warehouse, whutils.NewNoOpUploader()))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		t.Cleanup(func() { testhelper.DropSchema(t, sm.DB.DB, namespace) })

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"type":"track","messageId":"%[1]s","userId":"%[1]s","event":"Product Reviewed","properties":{"review_id":"86ac1cd43","product_id":"9578257311","rating":3,"review_body":"OK for the price. It works but the material feels flimsy.","objectInfo":{"id":123,"name":"Test User","email":"testuser@example.com","isActive":true,"createdAt":"2023-10-01T12:34:56Z","profile":{"age":30,"address":{"street":"123 Test St","city":"Testville","zip":"12345"},"interests":["coding","reading","gaming"]}},"arrayInfo":[{"itemId":"item_001","itemName":"Wireless Mouse","quantity":2,"price":19.99},{"itemId":"item_002","itemName":"USB-C Adapter","quantity":1,"price":9.99},{"itemId":"item_003","itemName":"Bluetooth Speaker","quantity":1,"price":49.99}]},"timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z","context":{"ip":"14.5.67.21"}}]}`,
				strconv.Itoa(index+1),
			)
		}
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"PRODUCT_REVIEWED": {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "PRODUCT_ID": "TEXT", "RATING": "NUMBER", "RECEIVED_AT": "TIMESTAMP_TZ", "REVIEW_BODY": "TEXT", "REVIEW_ID": "TEXT", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ", "OBJECT_INFO": "VARIANT", "ARRAY_INFO": "VARIANT"},
			"TRACKS":           {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"RUDDER_DISCARDS":  {"COLUMN_NAME": "TEXT", "COLUMN_VALUE": "TEXT", "REASON": "TEXT", "RECEIVED_AT": "TIMESTAMP_TZ", "ROW_ID": "TEXT", "TABLE_NAME": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		ts := timeutil.Now().Format("2006-01-02")
		eventName := "Product Reviewed"
		tableName := strcase.ToSnake(eventName)
		produceReviewedRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, PRODUCT_ID, RATING, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), REVIEW_BODY, REVIEW_ID, SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD'), OBJECT_INFO, ARRAY_INFO FROM %q.%q;`, namespace, "PRODUCT_REVIEWED"))
		require.ElementsMatch(t, [][]string{
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "1", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "1", ts, "{\n  \"createdAt\": \"2023-10-01T12:34:56Z\",\n  \"email\": \"testuser@example.com\",\n  \"id\": 123,\n  \"isActive\": true,\n  \"name\": \"Test User\",\n  \"profile\": {\n    \"address\": {\n      \"city\": \"Testville\",\n      \"street\": \"123 Test St\",\n      \"zip\": \"12345\"\n    },\n    \"age\": 30,\n    \"interests\": [\n      \"coding\",\n      \"reading\",\n      \"gaming\"\n    ]\n  }\n}", "[\n  {\n    \"itemId\": \"item_001\",\n    \"itemName\": \"Wireless Mouse\",\n    \"price\": 19.99,\n    \"quantity\": 2\n  },\n  {\n    \"itemId\": \"item_002\",\n    \"itemName\": \"USB-C Adapter\",\n    \"price\": 9.99,\n    \"quantity\": 1\n  },\n  {\n    \"itemId\": \"item_003\",\n    \"itemName\": \"Bluetooth Speaker\",\n    \"price\": 49.99,\n    \"quantity\": 1\n  }\n]"},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "2", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "2", ts, "{\n  \"createdAt\": \"2023-10-01T12:34:56Z\",\n  \"email\": \"testuser@example.com\",\n  \"id\": 123,\n  \"isActive\": true,\n  \"name\": \"Test User\",\n  \"profile\": {\n    \"address\": {\n      \"city\": \"Testville\",\n      \"street\": \"123 Test St\",\n      \"zip\": \"12345\"\n    },\n    \"age\": 30,\n    \"interests\": [\n      \"coding\",\n      \"reading\",\n      \"gaming\"\n    ]\n  }\n}", "[\n  {\n    \"itemId\": \"item_001\",\n    \"itemName\": \"Wireless Mouse\",\n    \"price\": 19.99,\n    \"quantity\": 2\n  },\n  {\n    \"itemId\": \"item_002\",\n    \"itemName\": \"USB-C Adapter\",\n    \"price\": 9.99,\n    \"quantity\": 1\n  },\n  {\n    \"itemId\": \"item_003\",\n    \"itemName\": \"Bluetooth Speaker\",\n    \"price\": 49.99,\n    \"quantity\": 1\n  }\n]"},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "3", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "3", ts, "{\n  \"createdAt\": \"2023-10-01T12:34:56Z\",\n  \"email\": \"testuser@example.com\",\n  \"id\": 123,\n  \"isActive\": true,\n  \"name\": \"Test User\",\n  \"profile\": {\n    \"address\": {\n      \"city\": \"Testville\",\n      \"street\": \"123 Test St\",\n      \"zip\": \"12345\"\n    },\n    \"age\": 30,\n    \"interests\": [\n      \"coding\",\n      \"reading\",\n      \"gaming\"\n    ]\n  }\n}", "[\n  {\n    \"itemId\": \"item_001\",\n    \"itemName\": \"Wireless Mouse\",\n    \"price\": 19.99,\n    \"quantity\": 2\n  },\n  {\n    \"itemId\": \"item_002\",\n    \"itemName\": \"USB-C Adapter\",\n    \"price\": 9.99,\n    \"quantity\": 1\n  },\n  {\n    \"itemId\": \"item_003\",\n    \"itemName\": \"Bluetooth Speaker\",\n    \"price\": 49.99,\n    \"quantity\": 1\n  }\n]"},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "4", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "4", ts, "{\n  \"createdAt\": \"2023-10-01T12:34:56Z\",\n  \"email\": \"testuser@example.com\",\n  \"id\": 123,\n  \"isActive\": true,\n  \"name\": \"Test User\",\n  \"profile\": {\n    \"address\": {\n      \"city\": \"Testville\",\n      \"street\": \"123 Test St\",\n      \"zip\": \"12345\"\n    },\n    \"age\": 30,\n    \"interests\": [\n      \"coding\",\n      \"reading\",\n      \"gaming\"\n    ]\n  }\n}", "[\n  {\n    \"itemId\": \"item_001\",\n    \"itemName\": \"Wireless Mouse\",\n    \"price\": 19.99,\n    \"quantity\": 2\n  },\n  {\n    \"itemId\": \"item_002\",\n    \"itemName\": \"USB-C Adapter\",\n    \"price\": 9.99,\n    \"quantity\": 1\n  },\n  {\n    \"itemId\": \"item_003\",\n    \"itemName\": \"Bluetooth Speaker\",\n    \"price\": 49.99,\n    \"quantity\": 1\n  }\n]"},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "5", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "5", ts, "{\n  \"createdAt\": \"2023-10-01T12:34:56Z\",\n  \"email\": \"testuser@example.com\",\n  \"id\": 123,\n  \"isActive\": true,\n  \"name\": \"Test User\",\n  \"profile\": {\n    \"address\": {\n      \"city\": \"Testville\",\n      \"street\": \"123 Test St\",\n      \"zip\": \"12345\"\n    },\n    \"age\": 30,\n    \"interests\": [\n      \"coding\",\n      \"reading\",\n      \"gaming\"\n    ]\n  }\n}", "[\n  {\n    \"itemId\": \"item_001\",\n    \"itemName\": \"Wireless Mouse\",\n    \"price\": 19.99,\n    \"quantity\": 2\n  },\n  {\n    \"itemId\": \"item_002\",\n    \"itemName\": \"USB-C Adapter\",\n    \"price\": 9.99,\n    \"quantity\": 1\n  },\n  {\n    \"itemId\": \"item_003\",\n    \"itemName\": \"Bluetooth Speaker\",\n    \"price\": 49.99,\n    \"quantity\": 1\n  }\n]"},
		}, produceReviewedRecordsFromDB)
		tracksRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "TRACKS"))
		require.ElementsMatch(t, tracksRecords(source, destination), tracksRecordsFromDB)

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("All datatypes", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := testhelper.RandSchema()
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)
		_ = source

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, snowpipeClientsURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, sm.Setup(ctx, warehouse, whutils.NewNoOpUploader()))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		t.Cleanup(func() { testhelper.DropSchema(t, sm.DB.DB, namespace) })

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"type":"track","messageId":"%[1]s","userId":"%[1]s","event":"Product Reviewed","properties":{"stringExample":"Wireless Headphones","integerExample":12345,"floatExample":299.99,"booleanExample":true,"nullExample":null,"arrayExample":["electronics","audio","wireless"],"arrayOfIntegersExample":[1,2,3,4],"arrayOfFloatsExample":[10.5,20.3,30.7],"arrayOfBooleansExample":[true,false,true],"nestedArrayExample":[[1,2],[3,4]],"objectExample":{"nestedString":"Inner Text","nestedInteger":789,"nestedBoolean":false},"nestedArrayOfObjectsExample":[{"id":1,"name":"Accessory 1"},{"id":2,"name":"Accessory 2"}],"dateExample":"2024-11-12","dateTimeExample":"2024-11-12T15:30:00Z","durationExample":"PT1H30M","dimensions":{"height":10.5,"width":5,"depth":3,"unit":"cm"},"binaryExample":"SGVsbG8sIFdvcmxkIQ==","geoPointExample":{"latitude":37.7749,"longitude":-122.4194},"urlExample":"https://example.com/product/12345"},"timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z","context":{"ip":"14.5.67.21"}}]}`,
				strconv.Itoa(index+1),
			)
		}
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"PRODUCT_REVIEWED": {"ARRAY_EXAMPLE": "TEXT", "ARRAY_OF_BOOLEANS_EXAMPLE": "TEXT", "ARRAY_OF_FLOATS_EXAMPLE": "TEXT", "ARRAY_OF_INTEGERS_EXAMPLE": "TEXT", "BINARY_EXAMPLE": "TEXT", "BOOLEAN_EXAMPLE": "BOOLEAN", "CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "DATE_EXAMPLE": "TIMESTAMP_TZ", "DATE_TIME_EXAMPLE": "TIMESTAMP_TZ", "DIMENSIONS_DEPTH": "NUMBER", "DIMENSIONS_HEIGHT": "FLOAT", "DIMENSIONS_UNIT": "TEXT", "DIMENSIONS_WIDTH": "NUMBER", "DURATION_EXAMPLE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "FLOAT_EXAMPLE": "FLOAT", "GEO_POINT_EXAMPLE_LATITUDE": "FLOAT", "GEO_POINT_EXAMPLE_LONGITUDE": "FLOAT", "ID": "TEXT", "INTEGER_EXAMPLE": "NUMBER", "NESTED_ARRAY_EXAMPLE": "TEXT", "NESTED_ARRAY_OF_OBJECTS_EXAMPLE": "TEXT", "OBJECT_EXAMPLE_NESTED_BOOLEAN": "BOOLEAN", "OBJECT_EXAMPLE_NESTED_INTEGER": "NUMBER", "OBJECT_EXAMPLE_NESTED_STRING": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "STRING_EXAMPLE": "TEXT", "TIMESTAMP": "TIMESTAMP_TZ", "URL_EXAMPLE": "TEXT", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"RUDDER_DISCARDS":  {"COLUMN_NAME": "TEXT", "COLUMN_VALUE": "TEXT", "REASON": "TEXT", "RECEIVED_AT": "TIMESTAMP_TZ", "ROW_ID": "TEXT", "TABLE_NAME": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"TRACKS":           {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		ts := timeutil.Now().Format("2006-01-02")
		eventName := "Product Reviewed"
		tableName := strcase.ToSnake(eventName)
		produceReviewedRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT ARRAY_EXAMPLE, ARRAY_OF_BOOLEANS_EXAMPLE, ARRAY_OF_FLOATS_EXAMPLE, ARRAY_OF_INTEGERS_EXAMPLE, BINARY_EXAMPLE, BOOLEAN_EXAMPLE, CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, DATE_EXAMPLE, DATE_TIME_EXAMPLE, DIMENSIONS_DEPTH, DIMENSIONS_HEIGHT, DIMENSIONS_UNIT, DIMENSIONS_WIDTH, DURATION_EXAMPLE, EVENT, EVENT_TEXT, FLOAT_EXAMPLE, GEO_POINT_EXAMPLE_LATITUDE, GEO_POINT_EXAMPLE_LONGITUDE, ID, INTEGER_EXAMPLE, NESTED_ARRAY_EXAMPLE, NESTED_ARRAY_OF_OBJECTS_EXAMPLE, OBJECT_EXAMPLE_NESTED_BOOLEAN, OBJECT_EXAMPLE_NESTED_INTEGER, OBJECT_EXAMPLE_NESTED_STRING, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, STRING_EXAMPLE, TIMESTAMP, URL_EXAMPLE, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "PRODUCT_REVIEWED"))
		require.ElementsMatch(t, [][]string{
			{"[\"electronics\",\"audio\",\"wireless\"]", "[true,false,true]", "[10.5,20.3,30.7]", "[1,2,3,4]", "SGVsbG8sIFdvcmxkIQ==", "true", destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, "2024-11-12T00:00:00Z", "2024-11-12T15:30:00Z", "3", "10.5", "cm", "5", "PT1H30M", tableName, eventName, "299.99", "37.7749", "-122.4194", "1", "12345", "[[1,2],[3,4]]", "[{\"id\":1,\"name\":\"Accessory 1\"},{\"id\":2,\"name\":\"Accessory 2\"}]", "false", "789", "Inner Text", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "Wireless Headphones", "2020-02-02T00:23:09Z", "https://example.com/product/12345", "1", ts},
			{"[\"electronics\",\"audio\",\"wireless\"]", "[true,false,true]", "[10.5,20.3,30.7]", "[1,2,3,4]", "SGVsbG8sIFdvcmxkIQ==", "true", destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, "2024-11-12T00:00:00Z", "2024-11-12T15:30:00Z", "3", "10.5", "cm", "5", "PT1H30M", tableName, eventName, "299.99", "37.7749", "-122.4194", "2", "12345", "[[1,2],[3,4]]", "[{\"id\":1,\"name\":\"Accessory 1\"},{\"id\":2,\"name\":\"Accessory 2\"}]", "false", "789", "Inner Text", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "Wireless Headphones", "2020-02-02T00:23:09Z", "https://example.com/product/12345", "2", ts},
			{"[\"electronics\",\"audio\",\"wireless\"]", "[true,false,true]", "[10.5,20.3,30.7]", "[1,2,3,4]", "SGVsbG8sIFdvcmxkIQ==", "true", destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, "2024-11-12T00:00:00Z", "2024-11-12T15:30:00Z", "3", "10.5", "cm", "5", "PT1H30M", tableName, eventName, "299.99", "37.7749", "-122.4194", "3", "12345", "[[1,2],[3,4]]", "[{\"id\":1,\"name\":\"Accessory 1\"},{\"id\":2,\"name\":\"Accessory 2\"}]", "false", "789", "Inner Text", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "Wireless Headphones", "2020-02-02T00:23:09Z", "https://example.com/product/12345", "3", ts},
			{"[\"electronics\",\"audio\",\"wireless\"]", "[true,false,true]", "[10.5,20.3,30.7]", "[1,2,3,4]", "SGVsbG8sIFdvcmxkIQ==", "true", destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, "2024-11-12T00:00:00Z", "2024-11-12T15:30:00Z", "3", "10.5", "cm", "5", "PT1H30M", tableName, eventName, "299.99", "37.7749", "-122.4194", "4", "12345", "[[1,2],[3,4]]", "[{\"id\":1,\"name\":\"Accessory 1\"},{\"id\":2,\"name\":\"Accessory 2\"}]", "false", "789", "Inner Text", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "Wireless Headphones", "2020-02-02T00:23:09Z", "https://example.com/product/12345", "4", ts},
			{"[\"electronics\",\"audio\",\"wireless\"]", "[true,false,true]", "[10.5,20.3,30.7]", "[1,2,3,4]", "SGVsbG8sIFdvcmxkIQ==", "true", destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, "2024-11-12T00:00:00Z", "2024-11-12T15:30:00Z", "3", "10.5", "cm", "5", "PT1H30M", tableName, eventName, "299.99", "37.7749", "-122.4194", "5", "12345", "[[1,2],[3,4]]", "[{\"id\":1,\"name\":\"Accessory 1\"},{\"id\":2,\"name\":\"Accessory 2\"}]", "false", "789", "Inner Text", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "Wireless Headphones", "2020-02-02T00:23:09Z", "https://example.com/product/12345", "5", ts},
		}, produceReviewedRecordsFromDB)
		tracksRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "TRACKS"))
		require.Equal(t, tracksRecords(source, destination), tracksRecordsFromDB)

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("identify event should not contain users", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := testhelper.RandSchema()
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, snowpipeClientsURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		t.Log("Creating schema and tables")
		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, sm.Setup(ctx, warehouse, whutils.NewNoOpUploader()))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { testhelper.DropSchema(t, sm.DB.DB, namespace) })
		require.NoError(t, sm.CreateTable(ctx, "IDENTIFIES", whutils.ModelTableSchema{
			"CONTEXT_IP": "string", "CONTEXT_LIBRARY_NAME": "string", "CONTEXT_SOURCE_TYPE": "string", "ORIGINAL_TIMESTAMP": "datetime", "UUID_TS": "datetime", "CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_PASSED_IP": "string", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "CONTEXT_SOURCE_ID": "string", "CONTEXT_REQUEST_IP": "string", "ID": "string", "RECEIVED_AT": "datetime", "USER_ID": "string",
		}))

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"messageId": "%[1]s", "userId":"%[1]s","type":"%[2]s","context":{"ip":"14.5.67.21","library":{"name":"http"}},"timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z"}]}`,
				strconv.Itoa(index+1),
				"identify",
			)
		}
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"IDENTIFIES":      {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_LIBRARY_NAME": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"RUDDER_DISCARDS": {"COLUMN_NAME": "TEXT", "COLUMN_VALUE": "TEXT", "REASON": "TEXT", "RECEIVED_AT": "TIMESTAMP_TZ", "ROW_ID": "TEXT", "TABLE_NAME": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		identifiesRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_LIBRARY_NAME, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "IDENTIFIES"))
		require.ElementsMatch(t, identifiesRecords(source, destination), identifiesRecordsFromDB)

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("encrypted credentials", func(t *testing.T) {
		encryptedCredentials, err := testhelper.GetSnowpipeTestCredentials(testhelper.TestKeyPairEncrypted)
		require.NoError(t, err)

		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := testhelper.RandSchema()
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, encryptedCredentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, snowpipeClientsURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, whutils.NewNoOpUploader()))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		t.Cleanup(func() { testhelper.DropSchema(t, sm.DB.DB, namespace) })

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"type":"track","messageId":"%[1]s","userId":"%[1]s","event":"Product Reviewed","properties":{"review_id":"86ac1cd43","product_id":"9578257311","rating":3,"review_body":"OK for the price. It works but the material feels flimsy."}, "timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z", "context":{"ip":"14.5.67.21"}}]}`,
				strconv.Itoa(index+1),
			)
		}
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"PRODUCT_REVIEWED": {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "PRODUCT_ID": "TEXT", "RATING": "NUMBER", "RECEIVED_AT": "TIMESTAMP_TZ", "REVIEW_BODY": "TEXT", "REVIEW_ID": "TEXT", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"TRACKS":           {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"RUDDER_DISCARDS":  {"COLUMN_NAME": "TEXT", "COLUMN_VALUE": "TEXT", "REASON": "TEXT", "RECEIVED_AT": "TIMESTAMP_TZ", "ROW_ID": "TEXT", "TABLE_NAME": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		produceReviewedRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, PRODUCT_ID, RATING, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), REVIEW_BODY, REVIEW_ID, SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "PRODUCT_REVIEWED"))
		require.ElementsMatch(t, productReviewedRecords(source, destination), produceReviewedRecordsFromDB)
		tracksRecordsFromDB := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, EVENT, EVENT_TEXT, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "TRACKS"))
		require.ElementsMatch(t, tracksRecords(source, destination), tracksRecordsFromDB)

		cancel()
		require.NoError(t, <-done)
	})
}

func initializeTestEnvironment(t testing.TB) (*postgres.Resource, int) {
	t.Helper()

	config.Reset()
	t.Cleanup(config.Reset)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	postgresContainer, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	gatewayPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	return postgresContainer, gatewayPort
}

func setupBackendConfigTestServer(
	t testing.TB,
	credentials *testhelper.TestCredentials,
	namespace string,
) (
	*httptest.Server,
	backendconfig.SourceT,
	backendconfig.DestinationT,
) {
	t.Helper()

	destination := backendconfigtest.
		NewDestinationBuilder("SNOWPIPE_STREAMING").
		WithID("destination1").
		WithConfigOption("account", credentials.Account).
		WithConfigOption("warehouse", credentials.Warehouse).
		WithConfigOption("database", credentials.Database).
		WithConfigOption("role", credentials.Role).
		WithConfigOption("user", credentials.User).
		WithConfigOption("useKeyPairAuth", true).
		WithConfigOption("privateKey", credentials.PrivateKey).
		WithConfigOption("privateKeyPassphrase", credentials.PrivateKeyPassphrase).
		WithConfigOption("namespace", namespace).
		WithRevisionID("destination1").
		Build()

	source := backendconfigtest.NewSourceBuilder().
		WithID("source1").
		WithWriteKey("writekey1").
		WithConnection(destination).
		Build()

	backendConfigServer := backendconfigtest.NewBuilder().
		WithWorkspaceConfig(
			backendconfigtest.NewConfigBuilder().
				WithSource(source).
				Build()).
		Build()
	t.Cleanup(backendConfigServer.Close)
	return backendConfigServer, source, destination
}

func runRudderServer(
	ctx context.Context,
	port int,
	postgresContainer *postgres.Resource,
	cbURL, transformerURL, snowpipeClientsURL,
	tmpDir string,
) (err error) {
	config.Set("INSTANCE_ID", "1")
	config.Set("CONFIG_BACKEND_URL", cbURL)
	config.Set("WORKSPACE_TOKEN", "token")
	config.Set("DEST_TRANSFORM_URL", transformerURL)
	config.Set("DB.host", postgresContainer.Host)
	config.Set("DB.port", postgresContainer.Port)
	config.Set("DB.user", postgresContainer.User)
	config.Set("DB.name", postgresContainer.Database)
	config.Set("DB.password", postgresContainer.Password)
	config.Set("SnowpipeStreaming.Client.URL", snowpipeClientsURL)
	config.Set("BatchRouter.SNOWPIPE_STREAMING.mainLoopFreq", "1s")                     // default 30s
	config.Set("BatchRouter.SNOWPIPE_STREAMING.uploadFreq", "1s")                       // default 30s
	config.Set("BatchRouter.SNOWPIPE_STREAMING.minIdleSleep", "1s")                     // default 2s
	config.Set("BatchRouter.SNOWPIPE_STREAMING.maxEventsInABatch", 10000)               // default 10000
	config.Set("BatchRouter.SNOWPIPE_STREAMING.maxPayloadSizeInBytes", 512*bytesize.KB) // default 10kb
	config.Set("BatchRouter.SNOWPIPE_STREAMING.asyncUploadWorkerTimeout", "1s")         // default 10s
	config.Set("BatchRouter.SNOWPIPE_STREAMING.asyncUploadTimeout", "1s")               // default 30m
	config.Set("BatchRouter.SNOWPIPE_STREAMING.pollStatusLoopSleep", "1s")              // default 10s
	config.Set("BatchRouter.isolationMode", "none")
	config.Set("Warehouse.mode", "off")
	config.Set("DestinationDebugger.disableEventDeliveryStatusUploads", true)
	config.Set("SourceDebugger.disableEventUploads", true)
	config.Set("TransformationDebugger.disableTransformationStatusUploads", true)
	config.Set("JobsDB.backup.enabled", false)
	config.Set("JobsDB.migrateDSLoopSleepDuration", "60m")
	config.Set("archival.Enabled", false)
	config.Set("Reporting.syncer.enabled", false)
	config.Set("Gateway.webPort", strconv.Itoa(port))
	config.Set("RUDDER_TMPDIR", os.TempDir())
	config.Set("recovery.storagePath", path.Join(tmpDir, "/recovery_data.json"))
	config.Set("recovery.enabled", false)
	config.Set("Profiler.Enabled", false)
	config.Set("Gateway.enableSuppressUserFeature", false)

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panicked: %v", r)
		}
	}()
	r := runner.New(runner.ReleaseInfo{EnterpriseToken: "TOKEN", Version: uuid.NewString()})
	c := r.Run(ctx, []string{"snowpipe-streaming-rudder-server"})
	if c != 0 {
		err = fmt.Errorf("rudder-server exited with a non-0 exit code: %d", c)
	}
	return
}

// nolint:unparam
func sendEvents(
	num int,
	eventFormat func(index int) string,
	writeKey, url string,
) error {
	for i := 0; i < num; i++ {
		payload := []byte(eventFormat(i))
		req, err := http.NewRequest(http.MethodPost, url+"/v1/batch", bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.SetBasicAuth(writeKey, "password")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to send event to rudder server, status code: %d: %s", resp.StatusCode, string(b))
		}
		func() { kithttputil.CloseResponse(resp) }()
	}
	return nil
}

// nolint:unparam
func requireGatewayJobsCount(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	status string,
	expectedCount int,
) {
	t.Helper()
	t.Log("Verifying gateway jobs count")

	query := fmt.Sprintf("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = '%s'", status)
	count := 0
	require.Eventually(t,
		func() bool {
			err := db.QueryRowContext(ctx, query).Scan(&count)
			if err != nil {
				t.Log("Error while querying for jobs count: ", err)
				return false
			}
			t.Logf("require gateway count: %d, expected: %d", count, expectedCount)
			return count == expectedCount
		},
		20*time.Second,
		1*time.Second,
	)
}

// nolint:unparam
func requireBatchRouterJobsCount(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	status string,
	expectedCount int,
) {
	t.Helper()
	t.Log("Verifying batch router jobs count")

	query := fmt.Sprintf("SELECT count(*) FROM unionjobsdbmetadata('batch_rt',1) WHERE job_state = '%s'", status)
	count := 0
	require.Eventually(t,
		func() bool {
			err := db.QueryRowContext(ctx, query).Scan(&count)
			if err != nil {
				t.Log("Error while querying for jobs count: ", err)
				return false
			}
			t.Logf("require batch router count: %d, expected: %d", count, expectedCount)
			return count == expectedCount
		},
		200*time.Second,
		1*time.Second,
	)
}

func convertRecordsToSchema(input [][]string) map[string]map[string]string {
	return lo.MapValues(lo.GroupBy(input, func(row []string) string {
		return row[0]
	}), func(columns [][]string, _ string) map[string]string {
		return lo.SliceToMap(columns, func(col []string) (string, string) {
			return col[1], col[2]
		})
	})
}

func tracksRecords(
	source backendconfig.SourceT,
	destination backendconfig.DestinationT,
) [][]string {
	ts := timeutil.Now().Format("2006-01-02")
	eventName := "Product Reviewed"
	tableName := strcase.ToSnake(eventName)
	return [][]string{
		{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "1", ts},
		{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "2", ts},
		{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "3", ts},
		{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "4", ts},
		{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "5", ts},
	}
}

func tracksRecordsForDiscards(
	source backendconfig.SourceT,
	destination backendconfig.DestinationT,
) [][]string {
	ts := timeutil.Now().Format("2006-01-02")
	eventName := "Product Reviewed"
	tableName := strcase.ToSnake(eventName)
	return [][]string{
		{destination.ID, "SNOWPIPE_STREAMING", "", "", source.ID, source.SourceDefinition.Name, tableName, eventName, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "1", ts},
		{destination.ID, "SNOWPIPE_STREAMING", "", "", source.ID, source.SourceDefinition.Name, tableName, eventName, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "2", ts},
		{destination.ID, "SNOWPIPE_STREAMING", "", "", source.ID, source.SourceDefinition.Name, tableName, eventName, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "3", ts},
		{destination.ID, "SNOWPIPE_STREAMING", "", "", source.ID, source.SourceDefinition.Name, tableName, eventName, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "4", ts},
		{destination.ID, "SNOWPIPE_STREAMING", "", "", source.ID, source.SourceDefinition.Name, tableName, eventName, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "5", ts},
	}
}

func productReviewedRecords(
	source backendconfig.SourceT,
	destination backendconfig.DestinationT,
) [][]string {
	ts := timeutil.Now().Format("2006-01-02")
	eventName := "Product Reviewed"
	tableName := strcase.ToSnake(eventName)
	return [][]string{
		{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "1", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "1", ts},
		{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "2", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "2", ts},
		{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "3", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "3", ts},
		{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "4", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "4", ts},
		{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "5", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "5", ts},
	}
}

func productReviewedRecordsForDiscards(
	source backendconfig.SourceT,
	destination backendconfig.DestinationT,
) [][]string {
	ts := timeutil.Now().Format("2006-01-02")
	eventName := "Product Reviewed"
	tableName := strcase.ToSnake(eventName)
	return [][]string{
		{destination.ID, "SNOWPIPE_STREAMING", "", "", source.ID, source.SourceDefinition.Name, tableName, eventName, "1", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "1", ts},
		{destination.ID, "SNOWPIPE_STREAMING", "", "", source.ID, source.SourceDefinition.Name, tableName, eventName, "2", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "2", ts},
		{destination.ID, "SNOWPIPE_STREAMING", "", "", source.ID, source.SourceDefinition.Name, tableName, eventName, "3", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "3", ts},
		{destination.ID, "SNOWPIPE_STREAMING", "", "", source.ID, source.SourceDefinition.Name, tableName, eventName, "4", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "4", ts},
		{destination.ID, "SNOWPIPE_STREAMING", "", "", source.ID, source.SourceDefinition.Name, tableName, eventName, "5", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "5", ts},
	}
}

func identifiesRecords(
	source backendconfig.SourceT,
	destination backendconfig.DestinationT,
) [][]string {
	ts := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "1", ts},
		{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "2", ts},
		{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "3", ts},
		{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "4", ts},
		{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "5", ts},
	}
}

func discardsRecords() [][]string {
	return append(discardProductReviewedRecords(), discardTracksRecords()...)
}

func discardProductReviewedRecords() [][]string {
	ts := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{"CONTEXT_PASSED_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "1", "PRODUCT_REVIEWED", ts},
		{"CONTEXT_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "1", "PRODUCT_REVIEWED", ts},
		{"CONTEXT_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "2", "PRODUCT_REVIEWED", ts},
		{"CONTEXT_PASSED_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "2", "PRODUCT_REVIEWED", ts},
		{"CONTEXT_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "3", "PRODUCT_REVIEWED", ts},
		{"CONTEXT_PASSED_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "3", "PRODUCT_REVIEWED", ts},
		{"CONTEXT_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "4", "PRODUCT_REVIEWED", ts},
		{"CONTEXT_PASSED_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "4", "PRODUCT_REVIEWED", ts},
		{"CONTEXT_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "5", "PRODUCT_REVIEWED", ts},
		{"CONTEXT_PASSED_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "5", "PRODUCT_REVIEWED", ts},
	}
}

func discardTracksRecords() [][]string {
	ts := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{"CONTEXT_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "3", "TRACKS", ts},
		{"CONTEXT_PASSED_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "3", "TRACKS", ts},
		{"CONTEXT_PASSED_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "4", "TRACKS", ts},
		{"CONTEXT_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "4", "TRACKS", ts},
		{"CONTEXT_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "5", "TRACKS", ts},
		{"CONTEXT_PASSED_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "5", "TRACKS", ts},
		{"CONTEXT_PASSED_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "1", "TRACKS", ts},
		{"CONTEXT_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "1", "TRACKS", ts},
		{"CONTEXT_PASSED_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "2", "TRACKS", ts},
		{"CONTEXT_IP", "14.5.67.21", "incompatible schema conversion from int to string", ts, "2", "TRACKS", ts},
	}
}
