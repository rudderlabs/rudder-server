package BQStreamAllEvents

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/google/uuid"
	"github.com/iancoleman/strcase"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-go-kit/testhelper/httptest"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	whbigquery "github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery"
	bqhelper "github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery/testhelper"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const destinationType = "BQSTREAM_ALL_EVENTS"

func TestBQStreamAllEvents(t *testing.T) {
	if _, exists := os.LookupEnv(bqhelper.TestKey); !exists {
		if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
			t.Fatalf("%s environment variable not set", bqhelper.TestKey)
		}
		t.Skipf("Skipping %s as %s is not set", t.Name(), bqhelper.TestKey)
	}

	c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.rudder-transformer.yml"}))
	c.Start(context.Background())

	transformerURL := fmt.Sprintf("http://localhost:%d", c.Port("transformer", 9090))

	credentials, err := bqhelper.GetBQTestCredentials()
	require.NoError(t, err)

	t.Run("namespace and table already exists", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := randSchema()
		safeNamespace := whutils.ToSafeNamespace(whutils.BQStreamAllEvents, namespace)
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, cancel, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		bm := setupBQManager(t, ctx, safeNamespace, destination)

		db := newBQClient(t, ctx, credentials)
		t.Cleanup(func() { _ = db.Close() })

		t.Log("Creating schema and tables")
		require.NoError(t, bm.CreateSchema(ctx))
		t.Cleanup(func() { dropSchema(t, db, safeNamespace) })
		require.NoError(t, bm.CreateTable(ctx, "product_reviewed", productReviewedSchema()))
		require.NoError(t, bm.CreateTable(ctx, "tracks", tracksSchema()))

		require.NoError(t, sendEvents(5, productReviewedEventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		require.Equal(t, map[string]map[string]string{
			"product_reviewed": productReviewedExpectedSchema(),
			"tracks":           tracksExpectedSchema(),
			"rudder_discards":  discardsExpectedSchema(),
		}, retrieveSchema(t, db, safeNamespace))

		require.ElementsMatch(t, productReviewedRecords(source, destination), retrieveProductReviewed(t, db, safeNamespace))
		require.ElementsMatch(t, tracksRecords(source, destination), retrieveTracks(t, db, safeNamespace))

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("namespace does not exists", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := randSchema()
		safeNamespace := whutils.ToSafeNamespace(whutils.BQStreamAllEvents, namespace)
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, cancel, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		db := newBQClient(t, ctx, credentials)
		t.Cleanup(func() { _ = db.Close() })
		t.Cleanup(func() { dropSchema(t, db, safeNamespace) })

		require.NoError(t, sendEvents(5, productReviewedEventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		require.Equal(t, map[string]map[string]string{
			"product_reviewed": productReviewedExpectedSchema(),
			"tracks":           tracksExpectedSchema(),
			"rudder_discards":  discardsExpectedSchema(),
		}, retrieveSchema(t, db, safeNamespace))

		require.ElementsMatch(t, productReviewedRecords(source, destination), retrieveProductReviewed(t, db, safeNamespace))
		require.ElementsMatch(t, tracksRecords(source, destination), retrieveTracks(t, db, safeNamespace))

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("table does not exists", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := randSchema()
		safeNamespace := whutils.ToSafeNamespace(whutils.BQStreamAllEvents, namespace)
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, cancel, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		bm := setupBQManager(t, ctx, safeNamespace, destination)
		db := newBQClient(t, ctx, credentials)
		t.Cleanup(func() { _ = db.Close() })

		t.Log("Creating schema")
		require.NoError(t, bm.CreateSchema(ctx))
		t.Cleanup(func() { dropSchema(t, db, safeNamespace) })

		require.NoError(t, sendEvents(5, productReviewedEventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		require.Equal(t, map[string]map[string]string{
			"product_reviewed": productReviewedExpectedSchema(),
			"tracks":           tracksExpectedSchema(),
			"rudder_discards":  discardsExpectedSchema(),
		}, retrieveSchema(t, db, safeNamespace))

		require.ElementsMatch(t, productReviewedRecords(source, destination), retrieveProductReviewed(t, db, safeNamespace))
		require.ElementsMatch(t, tracksRecords(source, destination), retrieveTracks(t, db, safeNamespace))

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("events with different schema", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := randSchema()
		safeNamespace := whutils.ToSafeNamespace(whutils.BQStreamAllEvents, namespace)
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, cancel, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		bm := setupBQManager(t, ctx, safeNamespace, destination)
		db := newBQClient(t, ctx, credentials)
		t.Cleanup(func() { _ = db.Close() })

		t.Log("Creating schema and tables")
		require.NoError(t, bm.CreateSchema(ctx))
		t.Cleanup(func() { dropSchema(t, db, safeNamespace) })
		require.NoError(t, bm.CreateTable(ctx, "product_reviewed", productReviewedSchema()))
		require.NoError(t, bm.CreateTable(ctx, "tracks", tracksSchema()))

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"type":"track","messageId":"%[1]s","userId":"%[1]s","event":"Product Reviewed","properties":{"review_id":"86ac1cd43","product_id":"9578257311","rating":3,"review_body":"OK for the price. It works but the material feels flimsy.","additional_column_%[1]s": "%[1]s"}, "timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z", "context":{"ip":"14.5.67.21"}}]}`,
				strconv.Itoa(index+1),
			)
		}
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		expectedProductReviewed := productReviewedExpectedSchema()
		for i := 1; i <= 5; i++ {
			expectedProductReviewed["additional_column_"+strconv.Itoa(i)] = "STRING"
		}
		require.Equal(t, map[string]map[string]string{
			"product_reviewed": expectedProductReviewed,
			"tracks":           tracksExpectedSchema(),
			"rudder_discards":  discardsExpectedSchema(),
		}, retrieveSchema(t, db, safeNamespace))

		ts := timeutil.Now().Format("2006-01-02")
		eventName := "Product Reviewed"
		tableName := strcase.ToSnake(eventName)
		records := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_id, context_destination_type, context_ip, context_passed_ip, context_source_id, context_source_type, event, event_text, id, original_timestamp, product_id, rating, FORMAT_TIMESTAMP('%%Y-%%m-%%d', received_at), review_body, review_id, sent_at, timestamp, user_id, FORMAT_TIMESTAMP('%%Y-%%m-%%d', uuid_ts), FORMAT_TIMESTAMP('%%Y-%%m-%%d', loaded_at), additional_column_1, additional_column_2, additional_column_3, additional_column_4, additional_column_5 FROM %s.%s ORDER BY id;`, safeNamespace, "product_reviewed"))
		require.ElementsMatch(t, [][]string{
			{destination.ID, destinationType, "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "1", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "1", ts, ts, "1", "", "", "", ""},
			{destination.ID, destinationType, "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "2", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "2", ts, ts, "", "2", "", "", ""},
			{destination.ID, destinationType, "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "3", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "3", ts, ts, "", "", "3", "", ""},
			{destination.ID, destinationType, "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "4", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "4", ts, ts, "", "", "", "4", ""},
			{destination.ID, destinationType, "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, tableName, eventName, "5", "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "5", ts, ts, "", "", "", "", "5"},
		}, records)
		require.ElementsMatch(t, tracksRecords(source, destination), retrieveTracks(t, db, safeNamespace))

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("discards", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := randSchema()
		safeNamespace := whutils.ToSafeNamespace(whutils.BQStreamAllEvents, namespace)
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, cancel, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		bm := setupBQManager(t, ctx, safeNamespace, destination)
		db := newBQClient(t, ctx, credentials)
		t.Cleanup(func() { _ = db.Close() })

		t.Log("Creating schema and tables. Discards is not created")
		t.Log("context_ip, context_passed_ip are of type int, datatype example columns are of type datetime")
		require.NoError(t, bm.CreateSchema(ctx))
		t.Cleanup(func() { dropSchema(t, db, safeNamespace) })

		productReviewedDiscardsTableSchema := productReviewedSchema()
		productReviewedDiscardsTableSchema["context_ip"] = "int"
		productReviewedDiscardsTableSchema["context_passed_ip"] = "int"
		for _, columnName := range datatypeExampleColumns() {
			productReviewedDiscardsTableSchema[columnName] = "datetime"
		}
		require.NoError(t, bm.CreateTable(ctx, "product_reviewed", productReviewedDiscardsTableSchema))

		tracksDiscardsTableSchema := tracksSchema()
		tracksDiscardsTableSchema["context_ip"] = "int"
		tracksDiscardsTableSchema["context_passed_ip"] = "int"
		require.NoError(t, bm.CreateTable(ctx, "tracks", tracksDiscardsTableSchema))

		require.NoError(t, sendEvents(5, allDatatypesEventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		expectedProductReviewed := productReviewedExpectedSchema()
		expectedProductReviewed["context_ip"] = "INT64"
		expectedProductReviewed["context_passed_ip"] = "INT64"
		for _, columnName := range datatypeExampleColumns() {
			expectedProductReviewed[columnName] = "TIMESTAMP"
		}
		expectedProductReviewed["integer_example"] = "INT64"
		expectedTracks := tracksExpectedSchema()
		expectedTracks["context_ip"] = "INT64"
		expectedTracks["context_passed_ip"] = "INT64"
		require.Equal(t, map[string]map[string]string{
			"product_reviewed": expectedProductReviewed,
			"tracks":           expectedTracks,
			"rudder_discards":  discardsExpectedSchema(),
		}, retrieveSchema(t, db, safeNamespace))

		produceReviewedRecordsFromDB := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT array_example, array_of_booleans_example, array_of_floats_example, array_of_integers_example, binary_example, boolean_example, date_example, date_time_example, dimensions_depth, dimensions_height, dimensions_unit, dimensions_width, duration_example, float_example, geo_point_example_latitude, geo_point_example_longitude, integer_example, nested_array_example, nested_array_of_objects_example, object_example_nested_boolean, object_example_nested_integer, object_example_nested_string, string_example, url_example, context_destination_id, context_destination_type, context_ip, context_passed_ip, context_source_id, context_source_type, event, event_text, id, original_timestamp, product_id, rating, FORMAT_TIMESTAMP('%%Y-%%m-%%d', received_at), review_body, review_id, sent_at, timestamp, user_id, FORMAT_TIMESTAMP('%%Y-%%m-%%d', uuid_ts), FORMAT_TIMESTAMP('%%Y-%%m-%%d', loaded_at) FROM %s.%s ORDER BY id;`, safeNamespace, "product_reviewed"))
		require.ElementsMatch(t, allDatatypesProductReviewedRecordsForDiscards(source, destination), produceReviewedRecordsFromDB)
		require.ElementsMatch(t, tracksRecordsForDiscards(source, destination), retrieveTracks(t, db, safeNamespace))
		require.ElementsMatch(t, allDatatypesDiscardsRecords(), retrieveDiscards(t, db, safeNamespace))

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("discards migration for reason", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := randSchema()
		safeNamespace := whutils.ToSafeNamespace(whutils.BQStreamAllEvents, namespace)
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, cancel, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		bm := setupBQManager(t, ctx, safeNamespace, destination)
		db := newBQClient(t, ctx, credentials)
		t.Cleanup(func() { _ = db.Close() })

		t.Log("Creating schema and tables. Discards is created without reason")
		t.Log("context_ip, context_passed_ip are of type int")
		require.NoError(t, bm.CreateSchema(ctx))
		t.Cleanup(func() { dropSchema(t, db, safeNamespace) })

		productReviewedDiscardsTableSchema := productReviewedSchema()
		productReviewedDiscardsTableSchema["context_ip"] = "int"
		productReviewedDiscardsTableSchema["context_passed_ip"] = "int"
		require.NoError(t, bm.CreateTable(ctx, "product_reviewed", productReviewedDiscardsTableSchema))

		tracksDiscardsTableSchema := tracksSchema()
		tracksDiscardsTableSchema["context_ip"] = "int"
		tracksDiscardsTableSchema["context_passed_ip"] = "int"
		require.NoError(t, bm.CreateTable(ctx, "tracks", tracksDiscardsTableSchema))

		require.NoError(t, bm.CreateTable(ctx, "rudder_discards", whutils.ModelTableSchema{
			"column_name": "string", "column_value": "string", "received_at": "datetime", "row_id": "string", "table_name": "string", "uuid_ts": "datetime",
		}))

		require.NoError(t, sendEvents(5, productReviewedEventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		expectedProductReviewed := productReviewedExpectedSchema()
		expectedProductReviewed["context_ip"] = "INT64"
		expectedProductReviewed["context_passed_ip"] = "INT64"
		expectedTracks := tracksExpectedSchema()
		expectedTracks["context_ip"] = "INT64"
		expectedTracks["context_passed_ip"] = "INT64"
		require.Equal(t, map[string]map[string]string{
			"product_reviewed": expectedProductReviewed,
			"tracks":           expectedTracks,
			"rudder_discards":  discardsExpectedSchema(),
		}, retrieveSchema(t, db, safeNamespace))

		require.ElementsMatch(t, productReviewedRecordsForDiscards(source, destination), retrieveProductReviewed(t, db, safeNamespace))
		require.ElementsMatch(t, tracksRecordsForDiscards(source, destination), retrieveTracks(t, db, safeNamespace))
		require.ElementsMatch(t, discardsRecords(), retrieveDiscards(t, db, safeNamespace))

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("discards migrated", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := randSchema()
		safeNamespace := whutils.ToSafeNamespace(whutils.BQStreamAllEvents, namespace)
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, cancel, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		bm := setupBQManager(t, ctx, safeNamespace, destination)
		db := newBQClient(t, ctx, credentials)
		t.Cleanup(func() { _ = db.Close() })

		t.Log("Creating schema and tables. Discards is created with reason")
		t.Log("context_ip, context_passed_ip are of type int")
		require.NoError(t, bm.CreateSchema(ctx))
		t.Cleanup(func() { dropSchema(t, db, safeNamespace) })

		productReviewedDiscardsTableSchema := productReviewedSchema()
		productReviewedDiscardsTableSchema["context_ip"] = "int"
		productReviewedDiscardsTableSchema["context_passed_ip"] = "int"
		require.NoError(t, bm.CreateTable(ctx, "product_reviewed", productReviewedDiscardsTableSchema))

		tracksDiscardsTableSchema := tracksSchema()
		tracksDiscardsTableSchema["context_ip"] = "int"
		tracksDiscardsTableSchema["context_passed_ip"] = "int"
		require.NoError(t, bm.CreateTable(ctx, "tracks", tracksDiscardsTableSchema))

		require.NoError(t, bm.CreateTable(ctx, "rudder_discards", whutils.ModelTableSchema{
			"column_name": "string", "column_value": "string", "received_at": "datetime", "row_id": "string", "table_name": "string", "uuid_ts": "datetime", "reason": "string",
		}))

		require.NoError(t, sendEvents(5, productReviewedEventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		expectedProductReviewed := productReviewedExpectedSchema()
		expectedProductReviewed["context_ip"] = "INT64"
		expectedProductReviewed["context_passed_ip"] = "INT64"
		expectedTracks := tracksExpectedSchema()
		expectedTracks["context_ip"] = "INT64"
		expectedTracks["context_passed_ip"] = "INT64"
		require.Equal(t, map[string]map[string]string{
			"product_reviewed": expectedProductReviewed,
			"tracks":           expectedTracks,
			"rudder_discards":  discardsExpectedSchema(),
		}, retrieveSchema(t, db, safeNamespace))

		require.ElementsMatch(t, productReviewedRecordsForDiscards(source, destination), retrieveProductReviewed(t, db, safeNamespace))
		require.ElementsMatch(t, tracksRecordsForDiscards(source, destination), retrieveTracks(t, db, safeNamespace))
		require.ElementsMatch(t, discardsRecords(), retrieveDiscards(t, db, safeNamespace))

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("many tables", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := randSchema()
		safeNamespace := whutils.ToSafeNamespace(whutils.BQStreamAllEvents, namespace)
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, cancel, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		bm := setupBQManager(t, ctx, safeNamespace, destination)
		db := newBQClient(t, ctx, credentials)
		t.Cleanup(func() { _ = db.Close() })

		t.Log("Creating schema")
		require.NoError(t, bm.CreateSchema(ctx))
		t.Cleanup(func() { dropSchema(t, db, safeNamespace) })

		for i := range 10 {
			eventFormat := func(int) string {
				return fmt.Sprintf(`{"batch":[{"type":"track","userId":"%[1]s","event":"Product Reviewed %[1]s","properties":{"review_id":"86ac1cd43","product_id":"9578257311","rating":3,"review_body":"OK for the price. It works but the material feels flimsy."}, "timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z", "context":{"ip":"14.5.67.21"}}]}`,
					strconv.Itoa(i+1),
				)
			}
			require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		}

		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5*10)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 2*5*10)

		expectedSchema := lo.SliceToMap(
			lo.RepeatBy(10, func(index int) string {
				return "product_reviewed_" + strconv.Itoa(index+1)
			}),
			func(tableName string) (string, map[string]string) {
				return tableName, productReviewedExpectedSchema()
			},
		)
		expectedSchema = lo.Assign(expectedSchema, map[string]map[string]string{
			"tracks":          tracksExpectedSchema(),
			"rudder_discards": discardsExpectedSchema(),
		})
		require.Equal(t, expectedSchema, retrieveSchema(t, db, safeNamespace))

		for i := range 10 {
			productIDIndex := i + 1
			userID := strconv.Itoa(productIDIndex)
			eventName := "Product Reviewed " + strconv.Itoa(productIDIndex)
			tableName := strcase.ToSnake(eventName)
			recordsFromDB := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_id, context_destination_type, context_source_id, context_source_type, event, event_text, original_timestamp, product_id, rating, FORMAT_TIMESTAMP('%%Y-%%m-%%d', received_at), review_body, review_id, sent_at, timestamp, user_id, FORMAT_TIMESTAMP('%%Y-%%m-%%d', uuid_ts), FORMAT_TIMESTAMP('%%Y-%%m-%%d', loaded_at) FROM %s.%s;`, safeNamespace, "product_reviewed_"+strconv.Itoa(productIDIndex)))
			ts := timeutil.Now().Format("2006-01-02")

			expectedProductReviewedRecords := lo.RepeatBy(5, func(int) []string {
				return []string{destination.ID, destinationType, source.ID, source.SourceDefinition.Name, tableName, eventName, "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", userID, ts, ts}
			})
			require.ElementsMatch(t, expectedProductReviewedRecords, recordsFromDB)
		}

		trackRecordsFromDB := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_id, context_destination_type, context_source_id, context_source_type, event, event_text, original_timestamp, FORMAT_TIMESTAMP('%%Y-%%m-%%d', received_at), sent_at, timestamp, user_id, FORMAT_TIMESTAMP('%%Y-%%m-%%d', uuid_ts), FORMAT_TIMESTAMP('%%Y-%%m-%%d', loaded_at) FROM %s.%s;`, safeNamespace, "tracks"))
		expectedTrackRecords := lo.RepeatBy(50, func(index int) []string {
			productIDIndex := index/5 + 1
			userID := strconv.Itoa(productIDIndex)
			eventName := "Product Reviewed " + strconv.Itoa(productIDIndex)
			tableName := strcase.ToSnake(eventName)
			ts := timeutil.Now().Format("2006-01-02")

			return []string{destination.ID, destinationType, source.ID, source.SourceDefinition.Name, tableName, eventName, "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", userID, ts, ts}
		})
		require.ElementsMatch(t, expectedTrackRecords, trackRecordsFromDB)

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("schema modified after stream writer creation (schema deleted)", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := randSchema()
		safeNamespace := whutils.ToSafeNamespace(whutils.BQStreamAllEvents, namespace)
		backendConfigServer, _, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, cancel, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		bm := setupBQManager(t, ctx, safeNamespace, destination)
		db := newBQClient(t, ctx, credentials)
		t.Cleanup(func() { _ = db.Close() })

		require.NoError(t, bm.CreateSchema(ctx))
		t.Cleanup(func() { dropSchema(t, db, safeNamespace) })
		require.NoError(t, bm.CreateTable(ctx, "product_reviewed", productReviewedSchema()))
		require.NoError(t, bm.CreateTable(ctx, "tracks", tracksSchema()))

		t.Log("Sending 5 events")
		require.NoError(t, sendEvents(5, productReviewedEventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		t.Log("Schema modified, Dropping schema")
		dropSchema(t, db, safeNamespace)

		t.Log("Sending 5 events again")
		require.NoError(t, sendEvents(5, productReviewedEventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 20)

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("schema modified after stream writer creation (table deleted)", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := randSchema()
		safeNamespace := whutils.ToSafeNamespace(whutils.BQStreamAllEvents, namespace)
		backendConfigServer, _, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, cancel, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		bm := setupBQManager(t, ctx, safeNamespace, destination)
		db := newBQClient(t, ctx, credentials)
		t.Cleanup(func() { _ = db.Close() })
		t.Cleanup(func() { dropSchema(t, db, safeNamespace) })

		require.NoError(t, bm.CreateSchema(ctx))
		require.NoError(t, bm.CreateTable(ctx, "product_reviewed", productReviewedSchema()))
		require.NoError(t, bm.CreateTable(ctx, "tracks", tracksSchema()))

		t.Log("Sending 5 events")
		require.NoError(t, sendEvents(5, productReviewedEventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		t.Log("Schema modified, Dropping table")
		require.NoError(t, db.Dataset(safeNamespace).Table("tracks").Delete(ctx))

		t.Log("Sending 5 events again")
		require.NoError(t, sendEvents(5, productReviewedEventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 15)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "failed", 5)

		productReviewedRecordsFromDB := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT COUNT(*) FROM %s.product_reviewed;`, safeNamespace))
		require.Equal(t, [][]string{{"10"}}, productReviewedRecordsFromDB)

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("schema modified after stream writer creation (columns deleted)", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := randSchema()
		safeNamespace := whutils.ToSafeNamespace(whutils.BQStreamAllEvents, namespace)
		backendConfigServer, _, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, cancel, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		bm := setupBQManager(t, ctx, safeNamespace, destination)
		db := newBQClient(t, ctx, credentials)
		t.Cleanup(func() { _ = db.Close() })

		require.NoError(t, bm.CreateSchema(ctx))
		t.Cleanup(func() { dropSchema(t, db, safeNamespace) })
		require.NoError(t, bm.CreateTable(ctx, "product_reviewed", productReviewedSchema()))
		require.NoError(t, bm.CreateTable(ctx, "tracks", tracksSchema()))

		t.Log("Sending 5 events")
		require.NoError(t, sendEvents(5, productReviewedEventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		t.Log("Schema modified, Dropping columns for tracks table")
		executeQuery(t, ctx, db, fmt.Sprintf("ALTER TABLE %s.tracks DROP COLUMN context_ip, DROP COLUMN context_passed_ip;", safeNamespace))

		t.Log("Sending 5 events again")
		require.NoError(t, sendEvents(5, productReviewedEventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 20)

		retrievedSchema := retrieveSchema(t, db, safeNamespace)
		areColumnsPresent := retrievedSchema["tracks"]["context_ip"] != "" && retrievedSchema["tracks"]["context_passed_ip"] != ""
		expectedSchema := map[string]map[string]string{
			"product_reviewed": productReviewedExpectedSchema(),
			"tracks":           tracksExpectedSchema(),
			"rudder_discards":  discardsExpectedSchema(),
		}
		if !areColumnsPresent {
			delete(expectedSchema["tracks"], "context_ip")
			delete(expectedSchema["tracks"], "context_passed_ip")
		}
		require.Equal(t, expectedSchema, retrievedSchema)

		tracksRecordsFromDB := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT COUNT(*) FROM %s.tracks;`, safeNamespace))
		require.Equal(t, [][]string{{"10"}}, tracksRecordsFromDB)
		productReviewedRecordsFromDB := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT COUNT(*) FROM %s.product_reviewed;`, safeNamespace))
		require.Equal(t, [][]string{{"10"}}, productReviewedRecordsFromDB)

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("schema modified after stream writer creation (datatype changed for all tables)", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := randSchema()
		safeNamespace := whutils.ToSafeNamespace(whutils.BQStreamAllEvents, namespace)
		backendConfigServer, _, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, cancel, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		bm := setupBQManager(t, ctx, safeNamespace, destination)
		db := newBQClient(t, ctx, credentials)
		t.Cleanup(func() { _ = db.Close() })
		t.Cleanup(func() { dropSchema(t, db, safeNamespace) })

		require.NoError(t, bm.CreateSchema(ctx))
		require.NoError(t, bm.CreateTable(ctx, "product_reviewed", productReviewedSchema()))
		require.NoError(t, bm.CreateTable(ctx, "tracks", tracksSchema()))
		require.NoError(t, bm.CreateTable(ctx, "rudder_discards", whutils.ModelTableSchema{
			"column_name": "string", "column_value": "string", "received_at": "datetime", "row_id": "string", "table_name": "string", "uuid_ts": "datetime", "reason": "string",
		}))

		t.Log("Sending 5 events")
		require.NoError(t, sendEvents(5, productReviewedEventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		t.Log("Schema modified, context_ip, context_passed_ip are of type int")
		executeQuery(t, ctx, db, fmt.Sprintf("ALTER TABLE %s.tracks DROP COLUMN context_ip, DROP COLUMN context_passed_ip;", safeNamespace))
		executeQuery(t, ctx, db, fmt.Sprintf("ALTER TABLE %s.product_reviewed DROP COLUMN context_ip, DROP COLUMN context_passed_ip;", safeNamespace))
		executeQuery(t, ctx, db, fmt.Sprintf("ALTER TABLE %s.tracks ADD COLUMN context_ip INT64, ADD COLUMN context_passed_ip INT64;", safeNamespace))
		executeQuery(t, ctx, db, fmt.Sprintf("ALTER TABLE %s.product_reviewed ADD COLUMN context_ip INT64, ADD COLUMN context_passed_ip INT64;", safeNamespace))

		t.Log("Sending 5 events again")
		require.NoError(t, sendEvents(5, productReviewedEventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 20)

		expectedProductReviewed := productReviewedExpectedSchema()
		expectedProductReviewed["context_ip"] = "INT64"
		expectedProductReviewed["context_passed_ip"] = "INT64"
		expectedTracks := tracksExpectedSchema()
		expectedTracks["context_ip"] = "INT64"
		expectedTracks["context_passed_ip"] = "INT64"
		require.Equal(t, map[string]map[string]string{
			"product_reviewed": expectedProductReviewed,
			"tracks":           expectedTracks,
			"rudder_discards":  discardsExpectedSchema(),
		}, retrieveSchema(t, db, safeNamespace))

		productReviewedRecordsFromDB := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT COUNT(*) FROM %s.product_reviewed;`, safeNamespace))
		require.Equal(t, [][]string{{"10"}}, productReviewedRecordsFromDB)
		tracksRecordsFromDB := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT COUNT(*) FROM %s.tracks;`, safeNamespace))
		require.Equal(t, [][]string{{"10"}}, tracksRecordsFromDB)

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("JSON columns", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := randSchema()
		safeNamespace := whutils.ToSafeNamespace(whutils.BQStreamAllEvents, namespace)
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)
		destination.Config["jsonPaths"] = "track.properties.objectInfo,track.properties.arrayInfo"

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, cancel, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		db := newBQClient(t, ctx, credentials)
		t.Cleanup(func() { _ = db.Close() })
		t.Cleanup(func() { dropSchema(t, db, safeNamespace) })

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"type":"track","messageId":"%[1]s","userId":"%[1]s","event":"Product Reviewed","properties":{"review_id":"86ac1cd43","product_id":"9578257311","rating":3,"review_body":"OK for the price. It works but the material feels flimsy.","objectInfo":{"id":123,"name":"Test User","email":"testuser@example.com","isActive":true,"createdAt":"2023-10-01T12:34:56Z","profile":{"age":30,"address":{"street":"123 Test St","city":"Testville","zip":"12345"},"interests":["coding","reading","gaming"]}},"arrayInfo":[{"itemId":"item_001","itemName":"Wireless Mouse","quantity":2,"price":19.99},{"itemId":"item_002","itemName":"USB-C Adapter","quantity":1,"price":9.99},{"itemId":"item_003","itemName":"Bluetooth Speaker","quantity":1,"price":49.99}]},"timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z","context":{"ip":"14.5.67.21"}}]}`,
				strconv.Itoa(index+1),
			)
		}
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		expectedProductReviewed := productReviewedExpectedSchema()
		expectedProductReviewed["object_info"] = "STRING"
		expectedProductReviewed["array_info"] = "STRING"
		require.Equal(t, map[string]map[string]string{
			"product_reviewed": expectedProductReviewed,
			"tracks":           tracksExpectedSchema(),
			"rudder_discards":  discardsExpectedSchema(),
		}, retrieveSchema(t, db, safeNamespace))

		require.ElementsMatch(t, tracksRecords(source, destination), retrieveTracks(t, db, safeNamespace))

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("All datatypes", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := randSchema()
		safeNamespace := whutils.ToSafeNamespace(whutils.BQStreamAllEvents, namespace)
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, cancel, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		db := newBQClient(t, ctx, credentials)
		t.Cleanup(func() { _ = db.Close() })
		t.Cleanup(func() { dropSchema(t, db, safeNamespace) })

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"type":"track","messageId":"%[1]s","userId":"%[1]s","event":"Product Reviewed","properties":{"stringExample":"Wireless Headphones","integerExample":12345,"floatExample":299.99,"booleanExample":true,"nullExample":null,"arrayExample":["electronics","audio","wireless"],"arrayOfIntegersExample":[1,2,3,4],"arrayOfFloatsExample":[10.5,20.3,30.7],"arrayOfBooleansExample":[true,false,true],"nestedArrayExample":[[1,2],[3,4]],"objectExample":{"nestedString":"Inner Text","nestedInteger":789,"nestedBoolean":false},"nestedArrayOfObjectsExample":[{"id":1,"name":"Accessory 1"},{"id":2,"name":"Accessory 2"}],"dateExample":"2024-11-12","dateTimeExample":"2024-11-12T15:30:00Z","durationExample":"PT1H30M","dimensions":{"height":10.5,"width":5,"depth":3,"unit":"cm"},"binaryExample":"SGVsbG8sIFdvcmxkIQ==","geoPointExample":{"latitude":37.7749,"longitude":-122.4194},"urlExample":"https://example.com/product/12345"},"timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z","context":{"ip":"14.5.67.21"}}]}`,
				strconv.Itoa(index+1),
			)
		}
		require.NoError(t, sendEvents(5, eventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		require.Equal(t, map[string]map[string]string{
			"product_reviewed": {"array_example": "STRING", "array_of_booleans_example": "STRING", "array_of_floats_example": "STRING", "array_of_integers_example": "STRING", "binary_example": "STRING", "boolean_example": "BOOL", "context_destination_id": "STRING", "context_destination_type": "STRING", "context_ip": "STRING", "context_passed_ip": "STRING", "context_request_ip": "STRING", "context_source_id": "STRING", "context_source_type": "STRING", "date_example": "TIMESTAMP", "date_time_example": "TIMESTAMP", "dimensions_depth": "INT64", "dimensions_height": "FLOAT64", "dimensions_unit": "STRING", "dimensions_width": "INT64", "duration_example": "STRING", "event": "STRING", "event_text": "STRING", "float_example": "FLOAT64", "geo_point_example_latitude": "FLOAT64", "geo_point_example_longitude": "FLOAT64", "id": "STRING", "integer_example": "INT64", "nested_array_example": "STRING", "nested_array_of_objects_example": "STRING", "object_example_nested_boolean": "BOOL", "object_example_nested_integer": "INT64", "object_example_nested_string": "STRING", "original_timestamp": "TIMESTAMP", "received_at": "TIMESTAMP", "sent_at": "TIMESTAMP", "string_example": "STRING", "timestamp": "TIMESTAMP", "url_example": "STRING", "user_id": "STRING", "uuid_ts": "TIMESTAMP", "loaded_at": "TIMESTAMP"},
			"tracks":           tracksExpectedSchema(),
			"rudder_discards":  discardsExpectedSchema(),
		}, retrieveSchema(t, db, safeNamespace))

		ts := timeutil.Now().Format("2006-01-02")
		eventName := "Product Reviewed"
		tableName := strcase.ToSnake(eventName)
		produceReviewedRecordsFromDB := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT array_example, array_of_booleans_example, array_of_floats_example, array_of_integers_example, binary_example, boolean_example, context_destination_id, context_destination_type, context_ip, context_passed_ip, context_source_id, context_source_type, date_example, date_time_example, dimensions_depth, dimensions_height, dimensions_unit, dimensions_width, duration_example, event, event_text, float_example, geo_point_example_latitude, geo_point_example_longitude, id, integer_example, nested_array_example, nested_array_of_objects_example, object_example_nested_boolean, object_example_nested_integer, object_example_nested_string, original_timestamp, FORMAT_TIMESTAMP('%%Y-%%m-%%d', received_at), sent_at, string_example, timestamp, url_example, user_id, FORMAT_TIMESTAMP('%%Y-%%m-%%d', uuid_ts), FORMAT_TIMESTAMP('%%Y-%%m-%%d', loaded_at) FROM %s.%s ORDER BY id;`, safeNamespace, "product_reviewed"))
		require.ElementsMatch(t, lo.RepeatBy(5, func(index int) []string {
			id := strconv.Itoa(index + 1)
			return []string{"[\"electronics\",\"audio\",\"wireless\"]", "[true,false,true]", "[10.5,20.3,30.7]", "[1,2,3,4]", "SGVsbG8sIFdvcmxkIQ==", "true", destination.ID, destinationType, "14.5.67.21", "14.5.67.21", source.ID, source.SourceDefinition.Name, "2024-11-12T00:00:00Z", "2024-11-12T15:30:00Z", "3", "10.5", "cm", "5", "PT1H30M", tableName, eventName, "299.99", "37.7749", "-122.4194", id, "12345", "[[1,2],[3,4]]", "[{\"id\":1,\"name\":\"Accessory 1\"},{\"id\":2,\"name\":\"Accessory 2\"}]", "false", "789", "Inner Text", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "Wireless Headphones", "2020-02-02T00:23:09Z", "https://example.com/product/12345", id, ts, ts}
		}), produceReviewedRecordsFromDB)
		require.ElementsMatch(t, tracksRecords(source, destination), retrieveTracks(t, db, safeNamespace))

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("partitioning (partitionColumn: received_at, partitionType: day)", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := randSchema()
		safeNamespace := whutils.ToSafeNamespace(whutils.BQStreamAllEvents, namespace)
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)
		destination.Config["partitionColumn"] = "received_at"
		destination.Config["partitionType"] = "day"

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, cancel, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		db := newBQClient(t, ctx, credentials)
		t.Cleanup(func() { _ = db.Close() })
		t.Cleanup(func() { dropSchema(t, db, safeNamespace) })

		require.NoError(t, sendEvents(5, productReviewedEventFormat, "writekey1", url))
		requireGatewayJobsCount(t, ctx, postgresContainer.DB, "succeeded", 5)
		requireBatchRouterJobsCount(t, ctx, postgresContainer.DB, "succeeded", 10)

		require.Equal(t, map[string]map[string]string{
			"product_reviewed": productReviewedExpectedSchema(),
			"tracks":           tracksExpectedSchema(),
			"rudder_discards":  discardsExpectedSchema(),
		}, retrieveSchema(t, db, safeNamespace))

		require.ElementsMatch(t, productReviewedRecords(source, destination), retrieveProductReviewed(t, db, safeNamespace))
		require.ElementsMatch(t, tracksRecords(source, destination), retrieveTracks(t, db, safeNamespace))

		t.Log("Verifying time partitioning on tables")
		checkTables := []string{"product_reviewed", "tracks", "rudder_discards"}
		tables := listTables(t, ctx, db, safeNamespace)
		filteredTables := lo.Filter(tables, func(table *bigquery.TableMetadata, _ int) bool {
			return lo.Contains(checkTables, table.Name)
		})
		require.Len(t, filteredTables, len(checkTables))
		for _, table := range filteredTables {
			require.NotNil(t, table.TimePartitioning)
			require.Equal(t, "received_at", table.TimePartitioning.Field)
			require.Equal(t, bigquery.DayPartitioningType, table.TimePartitioning.Type)
		}

		cancel()
		require.NoError(t, <-done)
	})
	t.Run("identify event should not contain users", func(t *testing.T) {
		postgresContainer, gatewayPort := initializeTestEnvironment(t)
		namespace := randSchema()
		safeNamespace := whutils.ToSafeNamespace(whutils.BQStreamAllEvents, namespace)
		backendConfigServer, source, destination := setupBackendConfigTestServer(t, credentials, namespace)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error)
		go func() {
			defer close(done)
			done <- runRudderServer(ctx, cancel, gatewayPort, postgresContainer, backendConfigServer.URL, transformerURL, t.TempDir())
		}()

		url := fmt.Sprintf("http://localhost:%d", gatewayPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		bm := setupBQManager(t, ctx, safeNamespace, destination)
		db := newBQClient(t, ctx, credentials)
		t.Cleanup(func() { _ = db.Close() })

		t.Log("Creating schema and tables")
		require.NoError(t, bm.CreateSchema(ctx))
		t.Cleanup(func() { dropSchema(t, db, safeNamespace) })
		require.NoError(t, bm.CreateTable(ctx, "identifies", whutils.ModelTableSchema{
			"context_ip": "string", "context_library_name": "string", "context_source_type": "string", "original_timestamp": "datetime", "uuid_ts": "datetime", "loaded_at": "datetime", "context_destination_id": "string", "context_destination_type": "string", "context_passed_ip": "string", "sent_at": "datetime", "timestamp": "datetime", "context_source_id": "string", "context_request_ip": "string", "id": "string", "received_at": "datetime", "user_id": "string",
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

		require.Equal(t, map[string]map[string]string{
			"identifies":      identifiesExpectedSchema(),
			"rudder_discards": discardsExpectedSchema(),
		}, retrieveSchema(t, db, safeNamespace))

		identifiesRecordsFromDB := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_id, context_destination_type, context_ip, context_library_name, context_passed_ip, context_source_id, context_source_type, id, original_timestamp, FORMAT_TIMESTAMP('%%Y-%%m-%%d', received_at), sent_at, timestamp, user_id, FORMAT_TIMESTAMP('%%Y-%%m-%%d', uuid_ts), FORMAT_TIMESTAMP('%%Y-%%m-%%d', loaded_at) FROM %s.%s ORDER BY id;`, safeNamespace, "identifies"))
		require.ElementsMatch(t, identifiesRecords(source, destination), identifiesRecordsFromDB)

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
	credentials *bqhelper.TestCredentials,
	namespace string,
) (
	*httptest.Server,
	backendconfig.SourceT,
	backendconfig.DestinationT,
) {
	t.Helper()

	destination := backendconfigtest.
		NewDestinationBuilder(destinationType).
		WithID("destination1").
		WithConfigOption("project", credentials.ProjectID).
		WithConfigOption("location", credentials.Location).
		WithConfigOption("bucketName", credentials.BucketName).
		WithConfigOption("credentials", credentials.Credentials).
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
	cancel context.CancelFunc,
	port int,
	postgresContainer *postgres.Resource,
	cbURL, transformerURL,
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
	config.Set("BatchRouter.BQSTREAM_ALL_EVENTS.pingFrequency", "1s")                    // default 30s
	config.Set("BatchRouter.BQSTREAM_ALL_EVENTS.uploadFreq", "1s")                       // default 30s
	config.Set("BatchRouter.BQSTREAM_ALL_EVENTS.minIdleSleep", "1s")                     // default 2s
	config.Set("BatchRouter.BQSTREAM_ALL_EVENTS.maxEventsInABatch", 10000)               // default 10000
	config.Set("BatchRouter.BQSTREAM_ALL_EVENTS.maxPayloadSizeInBytes", 512*bytesize.KB) // default 10kb
	config.Set("BatchRouter.BQSTREAM_ALL_EVENTS.asyncUploadWorkerTimeout", "1s")         // default 10s
	config.Set("BatchRouter.BQSTREAM_ALL_EVENTS.asyncUploadTimeout", "1s")               // default 30m
	config.Set("BatchRouter.BQSTREAM_ALL_EVENTS.pollStatusLoopSleep", "1s")              // default 10s
	config.Set("Processor.enableWarehouseTransformations", true)
	config.Set("Processor.verifyWarehouseTransformations", false)
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
	c := r.Run(ctx, cancel, []string{"bqstream-all-events-rudder-server"})
	if c != 0 {
		err = fmt.Errorf("rudder-server exited with a non-0 exit code: %d", c)
	}
	return err
}

// nolint:unparam
func sendEvents(
	num int,
	eventFormat func(index int) string,
	writeKey, url string,
) error {
	for i := range num {
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

// setupBQManager creates a BigQuery warehouse manager used to create the
// namespace/tables ahead of time, mirroring snowflake.New(...) in the snowpipe test.
func setupBQManager(
	t testing.TB,
	ctx context.Context,
	namespace string,
	destination backendconfig.DestinationT,
) *whbigquery.BigQuery {
	t.Helper()

	warehouse := whutils.ModelWarehouse{
		Namespace:   namespace,
		Destination: destination,
	}
	bm := whbigquery.New(config.New(), logger.NOP)
	require.NoError(t, bm.Setup(ctx, warehouse, whutils.NewNoOpUploader()))
	t.Cleanup(func() { bm.Cleanup(ctx) })
	return bm
}

func newBQClient(t testing.TB, ctx context.Context, credentials *bqhelper.TestCredentials) *bigquery.Client {
	t.Helper()

	db, err := bigquery.NewClient(
		ctx,
		credentials.ProjectID,
		option.WithAuthCredentialsJSON(option.ServiceAccount, []byte(credentials.Credentials)),
	)
	require.NoError(t, err)
	return db
}

func dropSchema(t testing.TB, db *bigquery.Client, namespace string) {
	t.Helper()
	t.Log("Dropping schema", namespace)

	require.Eventually(t, func() bool {
		err := db.Dataset(namespace).DeleteWithContents(context.Background())
		if status.Code(err) == codes.NotFound {
			return true
		}
		t.Logf("error deleting dataset: %v", err)
		return false
	}, 2*time.Minute, 5*time.Second)
}

func randSchema() string {
	return "BQStreamAllEvents_test_" + whutils.RandHex()
}

// executeQuery runs a statement against BigQuery, retrying for a while since
// DDL on tables with recently streamed data can fail transiently.
func executeQuery(t testing.TB, ctx context.Context, db *bigquery.Client, query string) {
	t.Helper()
	t.Log("Executing query", query)

	require.Eventually(t, func() bool {
		job, err := db.Query(query).Run(ctx)
		if err != nil {
			t.Logf("error running query: %v", err)
			return false
		}
		status, err := job.Wait(ctx)
		if err != nil {
			t.Logf("error waiting for query: %v", err)
			return false
		}
		if err := status.Err(); err != nil {
			t.Logf("query failed: %v", err)
			return false
		}
		return true
	},
		2*time.Minute,
		5*time.Second,
	)
}

func listTables(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string) []*bigquery.TableMetadata {
	t.Helper()
	t.Log("Listing tables in namespace", namespace)

	it := db.Dataset(namespace).Tables(ctx)

	var tables []*bigquery.TableMetadata
	for table, err := it.Next(); !errors.Is(err, iterator.Done); table, err = it.Next() {
		require.NoError(t, err)

		metadata, err := db.Dataset(namespace).Table(table.TableID).Metadata(ctx)
		require.NoError(t, err)

		metadata.Name = table.TableID
		tables = append(tables, metadata)
	}

	return lo.Filter(tables, func(item *bigquery.TableMetadata, _ int) bool {
		return item.Type == "TABLE"
	})
}

func retrieveSchema(t testing.TB, db *bigquery.Client, namespace string) map[string]map[string]string {
	t.Helper()
	records := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT t.table_name, c.column_name, c.data_type FROM %[1]s.INFORMATION_SCHEMA.TABLES as t LEFT JOIN %[1]s.INFORMATION_SCHEMA.COLUMNS as c ON (t.table_name = c.table_name) WHERE (t.table_type != 'VIEW') AND (t.table_name NOT LIKE 'rudder_staging_%%') AND ( c.column_name != '_PARTITIONTIME' OR c.column_name IS NULL );`, namespace))
	return convertRecordsToSchema(records)
}

func retrieveProductReviewed(t testing.TB, db *bigquery.Client, namespace string) [][]string {
	t.Helper()
	return bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_id, context_destination_type, context_ip, context_passed_ip, context_source_id, context_source_type, event, event_text, id, original_timestamp, product_id, rating, FORMAT_TIMESTAMP('%%Y-%%m-%%d', received_at), review_body, review_id, sent_at, timestamp, user_id, FORMAT_TIMESTAMP('%%Y-%%m-%%d', uuid_ts), FORMAT_TIMESTAMP('%%Y-%%m-%%d', loaded_at) FROM %s.%s ORDER BY id;`, namespace, "product_reviewed"))
}

func retrieveTracks(t testing.TB, db *bigquery.Client, namespace string) [][]string {
	t.Helper()
	return bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_id, context_destination_type, context_ip, context_passed_ip, context_source_id, context_source_type, event, event_text, id, original_timestamp, FORMAT_TIMESTAMP('%%Y-%%m-%%d', received_at), sent_at, timestamp, user_id, FORMAT_TIMESTAMP('%%Y-%%m-%%d', uuid_ts), FORMAT_TIMESTAMP('%%Y-%%m-%%d', loaded_at) FROM %s.%s ORDER BY id;`, namespace, "tracks"))
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

func productReviewedSchema() whutils.ModelTableSchema {
	return whutils.ModelTableSchema{
		"context_destination_id": "string", "context_destination_type": "string", "context_ip": "string", "context_request_ip": "string", "context_passed_ip": "string", "context_source_id": "string", "context_source_type": "string", "event": "string", "event_text": "string", "id": "string", "original_timestamp": "datetime", "product_id": "string", "rating": "int", "received_at": "datetime", "review_body": "string", "review_id": "string", "sent_at": "datetime", "timestamp": "datetime", "user_id": "string", "uuid_ts": "datetime", "loaded_at": "datetime",
	}
}

func tracksSchema() whutils.ModelTableSchema {
	return whutils.ModelTableSchema{
		"context_destination_id": "string", "context_destination_type": "string", "context_ip": "string", "context_request_ip": "string", "context_passed_ip": "string", "context_source_id": "string", "context_source_type": "string", "event": "string", "event_text": "string", "id": "string", "original_timestamp": "datetime", "received_at": "datetime", "sent_at": "datetime", "timestamp": "datetime", "user_id": "string", "uuid_ts": "datetime", "loaded_at": "datetime",
	}
}

func productReviewedExpectedSchema() map[string]string {
	return map[string]string{"context_destination_id": "STRING", "context_destination_type": "STRING", "context_ip": "STRING", "context_request_ip": "STRING", "context_passed_ip": "STRING", "context_source_id": "STRING", "context_source_type": "STRING", "event": "STRING", "event_text": "STRING", "id": "STRING", "original_timestamp": "TIMESTAMP", "product_id": "STRING", "rating": "INT64", "received_at": "TIMESTAMP", "review_body": "STRING", "review_id": "STRING", "sent_at": "TIMESTAMP", "timestamp": "TIMESTAMP", "user_id": "STRING", "uuid_ts": "TIMESTAMP", "loaded_at": "TIMESTAMP"}
}

func tracksExpectedSchema() map[string]string {
	return map[string]string{"context_destination_id": "STRING", "context_destination_type": "STRING", "context_ip": "STRING", "context_request_ip": "STRING", "context_passed_ip": "STRING", "context_source_id": "STRING", "context_source_type": "STRING", "event": "STRING", "event_text": "STRING", "id": "STRING", "original_timestamp": "TIMESTAMP", "received_at": "TIMESTAMP", "sent_at": "TIMESTAMP", "timestamp": "TIMESTAMP", "user_id": "STRING", "uuid_ts": "TIMESTAMP", "loaded_at": "TIMESTAMP"}
}

func identifiesExpectedSchema() map[string]string {
	return map[string]string{"context_destination_id": "STRING", "context_destination_type": "STRING", "context_ip": "STRING", "context_library_name": "STRING", "context_passed_ip": "STRING", "context_request_ip": "STRING", "context_source_id": "STRING", "context_source_type": "STRING", "id": "STRING", "original_timestamp": "TIMESTAMP", "received_at": "TIMESTAMP", "sent_at": "TIMESTAMP", "timestamp": "TIMESTAMP", "user_id": "STRING", "uuid_ts": "TIMESTAMP", "loaded_at": "TIMESTAMP"}
}

func discardsExpectedSchema() map[string]string {
	return map[string]string{"column_name": "STRING", "column_value": "STRING", "reason": "STRING", "received_at": "TIMESTAMP", "row_id": "STRING", "table_name": "STRING", "uuid_ts": "TIMESTAMP"}
}

func productReviewedEventFormat(index int) string {
	return fmt.Sprintf(`{"batch":[{"type":"track","messageId":"%[1]s","userId":"%[1]s","event":"Product Reviewed","properties":{"review_id":"86ac1cd43","product_id":"9578257311","rating":3,"review_body":"OK for the price. It works but the material feels flimsy."}, "timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z", "context":{"ip":"14.5.67.21"}}]}`,
		strconv.Itoa(index+1),
	)
}

func tracksRecords(
	source backendconfig.SourceT,
	destination backendconfig.DestinationT,
) [][]string {
	return tracksRecordsWithIP(source, destination, "14.5.67.21")
}

// tracksRecordsForDiscards returns the expected tracks records when
// context_ip/context_passed_ip have been discarded (table columns are of type int).
func tracksRecordsForDiscards(
	source backendconfig.SourceT,
	destination backendconfig.DestinationT,
) [][]string {
	return tracksRecordsWithIP(source, destination, "")
}

func tracksRecordsWithIP(
	source backendconfig.SourceT,
	destination backendconfig.DestinationT,
	ip string,
) [][]string {
	ts := timeutil.Now().Format("2006-01-02")
	eventName := "Product Reviewed"
	tableName := strcase.ToSnake(eventName)
	return lo.RepeatBy(5, func(index int) []string {
		id := strconv.Itoa(index + 1)
		return []string{destination.ID, destinationType, ip, ip, source.ID, source.SourceDefinition.Name, tableName, eventName, id, "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", id, ts, ts}
	})
}

func productReviewedRecords(
	source backendconfig.SourceT,
	destination backendconfig.DestinationT,
) [][]string {
	return productReviewedRecordsWithIP(source, destination, "14.5.67.21")
}

// productReviewedRecordsForDiscards returns the expected product_reviewed records
// when context_ip/context_passed_ip have been discarded (table columns are of type int).
func productReviewedRecordsForDiscards(
	source backendconfig.SourceT,
	destination backendconfig.DestinationT,
) [][]string {
	return productReviewedRecordsWithIP(source, destination, "")
}

func productReviewedRecordsWithIP(
	source backendconfig.SourceT,
	destination backendconfig.DestinationT,
	ip string,
) [][]string {
	ts := timeutil.Now().Format("2006-01-02")
	eventName := "Product Reviewed"
	tableName := strcase.ToSnake(eventName)
	return lo.RepeatBy(5, func(index int) []string {
		id := strconv.Itoa(index + 1)
		return []string{destination.ID, destinationType, ip, ip, source.ID, source.SourceDefinition.Name, tableName, eventName, id, "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", id, ts, ts}
	})
}

func identifiesRecords(
	source backendconfig.SourceT,
	destination backendconfig.DestinationT,
) [][]string {
	ts := timeutil.Now().Format("2006-01-02")
	return lo.RepeatBy(5, func(index int) []string {
		id := strconv.Itoa(index + 1)
		return []string{destination.ID, destinationType, "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, id, "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", id, ts, ts}
	})
}

// allDatatypesEventFormat is the event payload used by the discards scenario:
// review properties plus one property per supported/unsupported datatype.
func allDatatypesEventFormat(index int) string {
	return fmt.Sprintf(`{"batch":[{"type":"track","messageId":"%[1]s","userId":"%[1]s","event":"Product Reviewed","properties":{"review_id":"86ac1cd43","product_id":"9578257311","rating":3,"review_body":"OK for the price. It works but the material feels flimsy.","stringExample":"Wireless Headphones","integerExample":12345,"floatExample":299.99,"booleanExample":true,"nullExample":null,"arrayExample":["electronics","audio","wireless"],"arrayOfIntegersExample":[1,2,3,4],"arrayOfFloatsExample":[10.5,20.3,30.7],"arrayOfBooleansExample":[true,false,true],"nestedArrayExample":[[1,2],[3,4]],"objectExample":{"nestedString":"Inner Text","nestedInteger":789,"nestedBoolean":false},"nestedArrayOfObjectsExample":[{"id":1,"name":"Accessory 1"},{"id":2,"name":"Accessory 2"}],"dateExample":"2024-11-12","dateTimeExample":"2024-11-12T15:30:00Z","durationExample":"PT1H30M","dimensions":{"height":10.5,"width":5,"depth":3,"unit":"cm"},"binaryExample":"SGVsbG8sIFdvcmxkIQ==","geoPointExample":{"latitude":37.7749,"longitude":-122.4194},"urlExample":"https://example.com/product/12345"},"timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z","context":{"ip":"14.5.67.21"}}]}`,
		strconv.Itoa(index+1),
	)
}

// datatypeExampleColumns are the per-datatype example columns derived from
// allDatatypesEventFormat (excluding integer_example, which is not pre-created
// in the discards scenario and is therefore added by the destination itself).
func datatypeExampleColumns() []string {
	return []string{
		"array_example", "array_of_booleans_example", "array_of_floats_example", "array_of_integers_example",
		"binary_example", "boolean_example", "date_example", "date_time_example",
		"dimensions_depth", "dimensions_height", "dimensions_unit", "dimensions_width",
		"duration_example", "float_example", "geo_point_example_latitude", "geo_point_example_longitude",
		"nested_array_example", "nested_array_of_objects_example",
		"object_example_nested_boolean", "object_example_nested_integer", "object_example_nested_string",
		"string_example", "url_example",
	}
}

func retrieveDiscards(t testing.TB, db *bigquery.Client, namespace string) [][]string {
	t.Helper()
	return bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT column_name, column_value, reason, FORMAT_TIMESTAMP('%%Y-%%m-%%d', received_at), row_id, table_name, FORMAT_TIMESTAMP('%%Y-%%m-%%d', uuid_ts) FROM %s.%s;`, namespace, "rudder_discards"))
}

// discardsRecords returns the expected rudder_discards rows when only
// context_ip/context_passed_ip (created as int) are discarded for both tables.
func discardsRecords() [][]string {
	ts := timeutil.Now().Format("2006-01-02")
	var records [][]string
	for _, tableName := range []string{"product_reviewed", "tracks"} {
		for i := 1; i <= 5; i++ {
			for _, columnName := range []string{"context_ip", "context_passed_ip"} {
				records = append(records, []string{columnName, "14.5.67.21", "incompatible schema conversion from int to string", ts, strconv.Itoa(i), tableName, ts})
			}
		}
	}
	return records
}

// allDatatypesDiscardsRecords returns the expected rudder_discards rows for the
// discards scenario, where the datatype example columns were pre-created as
// datetime (and context_ip/context_passed_ip as int), so every incompatible
// value is discarded.
func allDatatypesDiscardsRecords() [][]string {
	ts := timeutil.Now().Format("2006-01-02")
	discards := [][2]string{
		{"array_example", "[electronics audio wireless]"},
		{"array_of_booleans_example", "[true false true]"},
		{"array_of_floats_example", "[10.5 20.3 30.7]"},
		{"array_of_integers_example", "[1 2 3 4]"},
		{"binary_example", "SGVsbG8sIFdvcmxkIQ=="},
		{"dimensions_unit", "cm"},
		{"duration_example", "PT1H30M"},
		{"nested_array_example", "[[1 2] [3 4]]"},
		{"nested_array_of_objects_example", "[map[id:1 name:Accessory 1] map[id:2 name:Accessory 2]]"},
		{"object_example_nested_string", "Inner Text"},
		{"string_example", "Wireless Headphones"},
		{"url_example", "https://example.com/product/12345"},
	}
	intDiscards := [][2]string{
		{"dimensions_depth", "3"},
		{"dimensions_width", "5"},
		{"object_example_nested_integer", "789"},
	}
	floatDiscards := [][2]string{
		{"dimensions_height", "10.5"},
		{"float_example", "299.99"},
		{"geo_point_example_latitude", "37.7749"},
		{"geo_point_example_longitude", "-122.4194"},
	}
	booleanDiscards := [][2]string{
		{"boolean_example", "true"},
		{"object_example_nested_boolean", "false"},
	}

	var records [][]string
	for i := 1; i <= 5; i++ {
		rowID := strconv.Itoa(i)
		for _, d := range discards {
			records = append(records, []string{d[0], d[1], "incompatible schema conversion from datetime to string", ts, rowID, "product_reviewed", ts})
		}
		for _, d := range intDiscards {
			records = append(records, []string{d[0], d[1], "incompatible schema conversion from datetime to int", ts, rowID, "product_reviewed", ts})
		}
		for _, d := range floatDiscards {
			records = append(records, []string{d[0], d[1], "incompatible schema conversion from datetime to float", ts, rowID, "product_reviewed", ts})
		}
		for _, d := range booleanDiscards {
			records = append(records, []string{d[0], d[1], "incompatible schema conversion from datetime to boolean", ts, rowID, "product_reviewed", ts})
		}
		for _, columnName := range []string{"context_ip", "context_passed_ip"} {
			records = append(records, []string{columnName, "14.5.67.21", "incompatible schema conversion from int to string", ts, rowID, "product_reviewed", ts})
			records = append(records, []string{columnName, "14.5.67.21", "incompatible schema conversion from int to string", ts, rowID, "tracks", ts})
		}
	}
	return records
}

// allDatatypesProductReviewedRecordsForDiscards returns the expected
// product_reviewed records for the discards scenario: only date_example,
// date_time_example (valid datetime conversions) and integer_example (column
// added by the destination with the event's own type) retain values; every
// other example column and the int-typed IP columns are discarded.
//
// Columns: array_example, array_of_booleans_example, array_of_floats_example,
// array_of_integers_example, binary_example, boolean_example, date_example,
// date_time_example, dimensions_depth, dimensions_height, dimensions_unit,
// dimensions_width, duration_example, float_example,
// geo_point_example_latitude, geo_point_example_longitude, integer_example,
// nested_array_example, nested_array_of_objects_example,
// object_example_nested_boolean, object_example_nested_integer,
// object_example_nested_string, string_example, url_example, followed by the
// standard columns.
func allDatatypesProductReviewedRecordsForDiscards(
	source backendconfig.SourceT,
	destination backendconfig.DestinationT,
) [][]string {
	ts := timeutil.Now().Format("2006-01-02")
	eventName := "Product Reviewed"
	tableName := strcase.ToSnake(eventName)
	return lo.RepeatBy(5, func(index int) []string {
		id := strconv.Itoa(index + 1)
		return []string{
			"", "", "", "", "", "", "2024-11-12T00:00:00Z", "2024-11-12T15:30:00Z", "", "", "", "", "", "", "", "", "12345", "", "", "", "", "", "", "",
			destination.ID, destinationType, "", "", source.ID, source.SourceDefinition.Name, tableName, eventName, id, "2020-02-02T00:23:09Z", "9578257311", "3", ts, "OK for the price. It works but the material feels flimsy.", "86ac1cd43", "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", id, ts, ts,
		}
	})
}
