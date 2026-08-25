package clickhouse_test

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousestd "github.com/ClickHouse/clickhouse-go"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	clickhousev2 "github.com/rudderlabs/clickhouse-go/v2"
	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	whth "github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

// TestIntegration runs the suite against one ClickHouse implementation, chosen
// by CLICKHOUSE_USE_V2. Every subtest is shared, so coverage cannot drift
// between the two, and CI runs the package once per value so each gets its own
// job. Unset means v1, which is what a bare `go test` gets.
func TestIntegration(t *testing.T) {
	var useV2 bool
	if v := os.Getenv("CLICKHOUSE_USE_V2"); v != "" {
		parsed, err := strconv.ParseBool(v)
		require.NoError(t, err, "CLICKHOUSE_USE_V2 must be a boolean")
		useV2 = parsed
	}

	implementation := "v1"
	if useV2 {
		implementation = "v2"
	}
	t.Logf("running against the %s implementation", implementation)

	testIntegration(t, useV2)
}

func testIntegration(t *testing.T, useV2 bool) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}

	misc.Init()
	validations.Init()
	whutils.Init()

	// The single node server is newer for v2 because native JSON needs 25.3+.
	// The driver itself does not require it: MinSupportedVersion only makes it
	// log "unsupported clickhouse version" and it connects regardless. So the
	// cluster keeps the 21.x server both implementations have always used,
	// along with the node configs written for it.
	clickhouseCompose := "testdata/docker-compose.clickhouse.yml"
	clusterCompose := "testdata/docker-compose.clickhouse-cluster.yml"
	if useV2 {
		clickhouseCompose = "testdata/docker-compose.clickhouse-v2.yml"
	}

	// Both implementations satisfy the same interface, so the subtests below only
	// differ in which one they construct.
	newClickhouse := func(conf *config.Config) manager.WarehouseOperations {
		if useV2 {
			return clickhouse.NewV2(conf, logger.NOP, stats.NOP)
		}
		return clickhouse.New(conf, logger.NOP, stats.NOP)
	}

	// v2 reads its own Warehouse.clickhouse.v2.* namespace, so every key the
	// suite sets has to follow the implementation under test.
	configKey := func(key string) string {
		if useV2 {
			return "Warehouse.clickhouse.v2." + key
		}
		return "Warehouse.clickhouse." + key
	}
	destType := whutils.CLICKHOUSE

	host := "localhost"
	database := "rudderdb"
	user := "rudder"
	password := "rudder-password"
	cluster := "rudder_cluster"
	bucketName := "testbucket"
	accessKeyID := "MYACCESSKEY"
	secretAccessKey := "MYSECRETKEY"

	// Verification connects with the driver under test. v1 has no JSON type, so
	// a v1 handle cannot represent everything v2 can create: reads pass because
	// the queries coerce to String, but a write fails inside the driver rather
	// than at the server, which leaves v2-only types unverifiable.
	connectDB := func(t testing.TB, ctx context.Context, port int) *sql.DB {
		t.Helper()

		if useV2 {
			return connectClickhouseDBV2(t, host, port, database, user, password)
		}
		return connectClickhouseDB(t, ctx, fmt.Sprintf(
			"tcp://%s:%d?compress=false&database=%s&password=%s&secure=false&skip_verify=true&username=%s",
			host, port, database, password, user,
		))
	}

	expectedSchema := model.Schema{
		"screens":       {"context_source_id": "Nullable(String)", "user_id": "Nullable(String)", "sent_at": "Nullable(DateTime)", "context_request_ip": "Nullable(String)", "original_timestamp": "Nullable(DateTime)", "url": "Nullable(String)", "context_source_type": "Nullable(String)", "between": "Nullable(String)", "timestamp": "Nullable(DateTime)", "context_ip": "Nullable(String)", "context_destination_type": "Nullable(String)", "received_at": "DateTime", "title": "Nullable(String)", "uuid_ts": "Nullable(DateTime)", "context_destination_id": "Nullable(String)", "name": "Nullable(String)", "id": "String", "as": "Nullable(String)"},
		"identifies":    {"context_ip": "Nullable(String)", "context_destination_id": "Nullable(String)", "email": "Nullable(String)", "context_request_ip": "Nullable(String)", "sent_at": "Nullable(DateTime)", "uuid_ts": "Nullable(DateTime)", "as": "Nullable(String)", "logins": "Nullable(Int64)", "context_source_type": "Nullable(String)", "context_traits_logins": "Nullable(Int64)", "name": "Nullable(String)", "context_destination_type": "Nullable(String)", "between": "Nullable(String)", "id": "String", "timestamp": "Nullable(DateTime)", "received_at": "DateTime", "user_id": "Nullable(String)", "context_traits_email": "Nullable(String)", "context_traits_as": "Nullable(String)", "context_traits_name": "Nullable(String)", "original_timestamp": "Nullable(DateTime)", "context_traits_between": "Nullable(String)", "context_source_id": "Nullable(String)"},
		"users":         {"context_traits_name": "SimpleAggregateFunction(anyLast, Nullable(String))", "context_traits_between": "SimpleAggregateFunction(anyLast, Nullable(String))", "context_request_ip": "SimpleAggregateFunction(anyLast, Nullable(String))", "context_traits_logins": "SimpleAggregateFunction(anyLast, Nullable(Int64))", "context_destination_id": "SimpleAggregateFunction(anyLast, Nullable(String))", "email": "SimpleAggregateFunction(anyLast, Nullable(String))", "logins": "SimpleAggregateFunction(anyLast, Nullable(Int64))", "as": "SimpleAggregateFunction(anyLast, Nullable(String))", "context_source_id": "SimpleAggregateFunction(anyLast, Nullable(String))", "uuid_ts": "SimpleAggregateFunction(anyLast, Nullable(DateTime))", "context_source_type": "SimpleAggregateFunction(anyLast, Nullable(String))", "context_traits_email": "SimpleAggregateFunction(anyLast, Nullable(String))", "name": "SimpleAggregateFunction(anyLast, Nullable(String))", "id": "String", "between": "SimpleAggregateFunction(anyLast, Nullable(String))", "context_ip": "SimpleAggregateFunction(anyLast, Nullable(String))", "received_at": "DateTime", "sent_at": "SimpleAggregateFunction(anyLast, Nullable(DateTime))", "context_traits_as": "SimpleAggregateFunction(anyLast, Nullable(String))", "context_destination_type": "SimpleAggregateFunction(anyLast, Nullable(String))", "timestamp": "SimpleAggregateFunction(anyLast, Nullable(DateTime))", "original_timestamp": "SimpleAggregateFunction(anyLast, Nullable(DateTime))"},
		"product_track": {"review_id": "Nullable(String)", "context_source_id": "Nullable(String)", "user_id": "Nullable(String)", "timestamp": "Nullable(DateTime)", "uuid_ts": "Nullable(DateTime)", "review_body": "Nullable(String)", "context_source_type": "Nullable(String)", "as": "Nullable(String)", "between": "Nullable(String)", "id": "String", "rating": "Nullable(Int64)", "event": "LowCardinality(String)", "original_timestamp": "Nullable(DateTime)", "context_destination_type": "Nullable(String)", "context_ip": "Nullable(String)", "context_destination_id": "Nullable(String)", "sent_at": "Nullable(DateTime)", "received_at": "DateTime", "event_text": "LowCardinality(String)", "product_id": "Nullable(String)", "context_request_ip": "Nullable(String)"},
		"tracks":        {"original_timestamp": "Nullable(DateTime)", "context_destination_id": "Nullable(String)", "event": "LowCardinality(String)", "context_request_ip": "Nullable(String)", "uuid_ts": "Nullable(DateTime)", "context_destination_type": "Nullable(String)", "user_id": "Nullable(String)", "sent_at": "Nullable(DateTime)", "context_source_type": "Nullable(String)", "context_ip": "Nullable(String)", "timestamp": "Nullable(DateTime)", "received_at": "DateTime", "context_source_id": "Nullable(String)", "event_text": "LowCardinality(String)", "id": "String"},
		"aliases":       {"context_request_ip": "Nullable(String)", "context_destination_type": "Nullable(String)", "context_destination_id": "Nullable(String)", "previous_id": "Nullable(String)", "context_ip": "Nullable(String)", "sent_at": "Nullable(DateTime)", "id": "String", "uuid_ts": "Nullable(DateTime)", "timestamp": "Nullable(DateTime)", "original_timestamp": "Nullable(DateTime)", "context_source_id": "Nullable(String)", "user_id": "Nullable(String)", "context_source_type": "Nullable(String)", "received_at": "DateTime"},
		"pages":         {"name": "Nullable(String)", "url": "Nullable(String)", "id": "String", "timestamp": "Nullable(DateTime)", "title": "Nullable(String)", "user_id": "Nullable(String)", "context_source_id": "Nullable(String)", "context_source_type": "Nullable(String)", "original_timestamp": "Nullable(DateTime)", "context_request_ip": "Nullable(String)", "received_at": "DateTime", "between": "Nullable(String)", "context_destination_type": "Nullable(String)", "uuid_ts": "Nullable(DateTime)", "context_destination_id": "Nullable(String)", "sent_at": "Nullable(DateTime)", "context_ip": "Nullable(String)", "as": "Nullable(String)"},
		"groups":        {"as": "Nullable(String)", "user_id": "Nullable(String)", "context_destination_type": "Nullable(String)", "sent_at": "Nullable(DateTime)", "context_source_type": "Nullable(String)", "received_at": "DateTime", "context_ip": "Nullable(String)", "industry": "Nullable(String)", "timestamp": "Nullable(DateTime)", "group_id": "Nullable(String)", "uuid_ts": "Nullable(DateTime)", "context_source_id": "Nullable(String)", "context_request_ip": "Nullable(String)", "between": "Nullable(String)", "original_timestamp": "Nullable(DateTime)", "name": "Nullable(String)", "plan": "Nullable(String)", "context_destination_id": "Nullable(String)", "employees": "Nullable(Int64)", "id": "String"},
	}
	userIDFormat := "userId_clickhouse"
	userIDSQL := "SUBSTRING(user_id, 1, 17)"
	uuidTSSQL := "formatDateTime(uuid_ts, '%Y-%m-%d')"

	t.Run("Events flow (single mode)", func(t *testing.T) {
		testCases := []struct {
			name            string
			configOverride  map[string]any
			verifyPartition func(t *testing.T, db *sql.DB)
		}{
			{
				name:           "No Partition Type specified",
				configOverride: map[string]any{},
				verifyPartition: func(t *testing.T, db *sql.DB) {
					t.Helper()
					rows := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(
						`SELECT partition_key FROM system.tables WHERE database = '%s' AND name = 'identifies';`, database,
					))
					require.Len(t, rows, 1)
					require.Len(t, rows[0], 1)
					require.Equal(t, "toDate(received_at)", rows[0][0])
				},
			},
			{
				name: "Partition Type: day",
				configOverride: map[string]any{
					"partitionType": "day",
				},
				verifyPartition: func(t *testing.T, db *sql.DB) {
					t.Helper()
					rows := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(
						`SELECT partition_key FROM system.tables WHERE database = '%s' AND name = 'identifies';`, database,
					))
					require.Len(t, rows, 1)
					require.Len(t, rows[0], 1)
					require.Equal(t, "toDate(received_at)", rows[0][0])
				},
			},
			{
				name: "Partition Type: week",
				configOverride: map[string]any{
					"partitionType": "week",
				},
				verifyPartition: func(t *testing.T, db *sql.DB) {
					t.Helper()
					rows := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(
						`SELECT partition_key FROM system.tables WHERE database = '%s' AND name = 'identifies';`, database,
					))
					require.Len(t, rows, 1)
					require.Len(t, rows[0], 1)
					require.Equal(t, "toStartOfWeek(received_at)", rows[0][0])
				},
			},
			{
				name: "Partition Type: month",
				configOverride: map[string]any{
					"partitionType": "month",
				},
				verifyPartition: func(t *testing.T, db *sql.DB) {
					t.Helper()
					rows := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(
						`SELECT partition_key FROM system.tables WHERE database = '%s' AND name = 'identifies';`, database,
					))
					require.Len(t, rows, 1)
					require.Len(t, rows[0], 1)
					require.Equal(t, "toStartOfMonth(received_at)", rows[0][0])
				},
			},
			{
				name: "Partition Type: quarter",
				configOverride: map[string]any{
					"partitionType": "quarter",
				},
				verifyPartition: func(t *testing.T, db *sql.DB) {
					t.Helper()
					rows := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(
						`SELECT partition_key FROM system.tables WHERE database = '%s' AND name = 'identifies';`, database,
					))
					require.Len(t, rows, 1)
					require.Len(t, rows[0], 1)
					require.Equal(t, "toStartOfQuarter(received_at)", rows[0][0])
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				httpPort, err := kithelper.GetFreePort()
				require.NoError(t, err)

				c := testcompose.New(t, compose.FilePaths([]string{clickhouseCompose, "../testdata/docker-compose.jobsdb.yml", "../testdata/docker-compose.minio.yml", "../testdata/docker-compose.transformer.yml"}))
				c.Start(context.Background())

				workspaceID := whutils.RandHex()
				sourceID := whutils.RandHex()
				destinationID := whutils.RandHex()
				writeKey := whutils.RandHex()
				jobsDBPort := c.Port("jobsDb", 5432)
				clickhousePort := c.Port("clickhouse", 9000)
				minioEndpoint := fmt.Sprintf("localhost:%d", c.Port("minio", 9000))
				transformerURL := fmt.Sprintf("http://localhost:%d", c.Port("transformer", 9090))
				eventFilePrefix := "../testdata/upload-job"

				setupDB := func(t testing.TB, ctx context.Context) *sql.DB {
					t.Helper()
					return connectDB(t, ctx, clickhousePort)
				}

				verifySchema := func(t *testing.T, db *sql.DB, namespace string) {
					t.Helper()
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
					require.Equal(t, expectedSchema, whth.ConvertRecordsToSchema(schema))
				}

				verifyRecords := func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace string) {
					t.Helper()
					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, between, context_traits_email, context_destination_id, id FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, as, logins, sent_at, context_traits_logins, context_ip, between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, SUBSTRING(id, 1, 17), %s, received_at, name, original_timestamp FROM %q.%q ORDER BY id;`, uuidTSSQL, namespace, "users"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersRecordsUsingUsersLoadFilesForClickhouse(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, as, review_body, between, review_id, event_text, id, event, rating FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "product_track"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, between, context_request_ip, sent_at, url, %s FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "pages"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, as FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "screens"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "aliases"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, as, context_destination_id, received_at, name, context_ip FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "groups"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsRecords(userIDFormat, sourceID, destinationID, destType))
				}

				jobsDB := whth.JobsDB(t, jobsDBPort)

				destinationBuilder := backendconfigtest.NewDestinationBuilder(destType).
					WithID(destinationID).
					WithRevisionID(destinationID).
					WithConfigOption("host", host).
					WithConfigOption("database", database).
					WithConfigOption("user", user).
					WithConfigOption("password", password).
					WithConfigOption("port", strconv.Itoa(clickhousePort)).
					WithConfigOption("bucketProvider", whutils.MINIO).
					WithConfigOption("bucketName", bucketName).
					WithConfigOption("accessKeyID", accessKeyID).
					WithConfigOption("secretAccessKey", secretAccessKey).
					WithConfigOption("useSSL", false).
					WithConfigOption("secure", false).
					WithConfigOption("endPoint", minioEndpoint).
					WithConfigOption("useRudderStorage", false).
					WithConfigOption("syncFrequency", "30").
					WithConfigOption("allowUsersContextTraits", true).
					WithConfigOption("underscoreDivideNumbers", true).
					WithConfigOption("cleanupObjectStorageFiles", true)
				for k, v := range tc.configOverride {
					destinationBuilder = destinationBuilder.WithConfigOption(k, v)
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

				t.Setenv("RSERVER_WAREHOUSE_CLICKHOUSE_MAX_PARALLEL_LOADS", "8")
				t.Setenv("RSERVER_WAREHOUSE_CLICKHOUSE_SLOW_QUERY_THRESHOLD", "0s")
				if useV2 {
					t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Warehouse.clickhouse.useV2Driver"), "true")
				}

				whth.BootstrapSvc(t, workspaceConfig, httpPort, jobsDBPort)

				db := setupDB(t, context.Background())
				t.Cleanup(func() { _ = db.Close() })
				tables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}

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
					Schema:          database,
					Tables:          tables,
					SourceID:        sourceID,
					DestinationID:   destinationID,
					Config:          conf,
					WorkspaceID:     workspaceID,
					DestinationType: destType,
					JobsDB:          jobsDB,
					HTTPPort:        httpPort,
					Client:          sqlClient,
					UserID:          whth.GetUserId(destType),
					EventsFilePath:  eventFilePrefix + ".events-1.json",
					TransformerURL:  transformerURL,
					Destination:     destination,
				}
				ts1.VerifyEvents(t)

				t.Log("verifying test case 2")
				ts2 := whth.TestConfig{
					WriteKey:        writeKey,
					Schema:          database,
					Tables:          tables,
					SourceID:        sourceID,
					DestinationID:   destinationID,
					Config:          conf,
					WorkspaceID:     workspaceID,
					DestinationType: destType,
					JobsDB:          jobsDB,
					HTTPPort:        httpPort,
					Client:          sqlClient,
					UserID:          whth.GetUserId(destType),
					EventsFilePath:  eventFilePrefix + ".events-2.json",
					TransformerURL:  transformerURL,
					Destination:     destination,
				}
				ts2.VerifyEvents(t)

				t.Log("verifying schema")
				verifySchema(t, db, database)

				t.Log("verifying records")
				verifyRecords(t, db, sourceID, destinationID, database)

				t.Log("verifying partition")
				tc.verifyPartition(t, db)
			})
		}
	})

	t.Run("Events flow (cluster mode)", func(t *testing.T) {
		httpPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		c := testcompose.New(t, compose.FilePaths([]string{clusterCompose, "../testdata/docker-compose.jobsdb.yml", "../testdata/docker-compose.minio.yml", "../testdata/docker-compose.transformer.yml"}))
		c.Start(context.Background())

		workspaceID := whutils.RandHex()
		jobsDBPort := c.Port("jobsDb", 5432)
		clickhouseClusterPort1 := c.Port("clickhouse01", 9000)
		clickhouseClusterPort2 := c.Port("clickhouse02", 9000)
		clickhouseClusterPort3 := c.Port("clickhouse03", 9000)
		clickhouseClusterPort4 := c.Port("clickhouse04", 9000)
		minioEndpoint := fmt.Sprintf("localhost:%d", c.Port("minio", 9000))
		transformerURL := fmt.Sprintf("http://localhost:%d", c.Port("transformer", 9090))

		jobsDB := whth.JobsDB(t, jobsDBPort)

		testCases := []struct {
			name             string
			warehouseEvents2 whth.EventsCountMap
			clusterSetup     func(*testing.T, context.Context)
			setupDB          func(testing.TB, context.Context) *sql.DB
			eventFilePrefix  string
			configOverride   map[string]any
			verifySchema     func(t *testing.T, db *sql.DB, namespace string)
			verifyRecords    func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace string)
		}{
			{
				name: "Cluster Mode Setup",
				setupDB: func(t testing.TB, ctx context.Context) *sql.DB {
					t.Helper()
					return connectDB(t, ctx, clickhouseClusterPort1)
				},
				warehouseEvents2: whth.EventsCountMap{
					"identifies": 8, "users": 2, "tracks": 8, "product_track": 8, "pages": 8, "screens": 8, "aliases": 8, "groups": 8,
				},
				clusterSetup: func(t *testing.T, ctx context.Context) {
					t.Helper()

					clusterPorts := []int{clickhouseClusterPort2, clickhouseClusterPort3, clickhouseClusterPort4}
					dbs := lo.Map(clusterPorts, func(port, _ int) *sql.DB {
						return connectDB(t, ctx, port)
					})
					tables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}
					initializeClickhouseClusterMode(t, dbs, tables, clickhouseClusterPort1, connectDB)
				},
				eventFilePrefix: "../testdata/upload-job",
				configOverride: map[string]any{
					"cluster": cluster,
					"port":    strconv.Itoa(clickhouseClusterPort1),
				},
				verifySchema: func(t *testing.T, db *sql.DB, namespace string) {
					t.Helper()
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s' AND table_name IN ('identifies', 'users', 'tracks', 'product_track', 'pages', 'screens', 'aliases', 'groups');`, namespace))
					require.Equal(t, expectedSchema, whth.ConvertRecordsToSchema(schema))
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace string) {
					t.Helper()
					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, between, context_traits_email, context_destination_id, id FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies_shard"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, as, logins, sent_at, context_traits_logins, context_ip, between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, SUBSTRING(id, 1, 17), %s, received_at, name, original_timestamp FROM %q.%q ORDER BY id;`, uuidTSSQL, namespace, "users_shard"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersRecordsUsingUsersLoadFilesForClickhouse(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks_shard"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, as, review_body, between, review_id, event_text, id, event, rating FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "product_track_shard"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, between, context_request_ip, sent_at, url, %s FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "pages_shard"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, as FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "screens_shard"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "aliases_shard"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, as, context_destination_id, received_at, name, context_ip FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "groups_shard"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				var (
					sourceID      = whutils.RandHex()
					destinationID = whutils.RandHex()
					writeKey      = whutils.RandHex()
				)

				destinationBuilder := backendconfigtest.NewDestinationBuilder(destType).
					WithID(destinationID).
					WithRevisionID(destinationID).
					WithConfigOption("host", host).
					WithConfigOption("database", database).
					WithConfigOption("user", user).
					WithConfigOption("password", password).
					WithConfigOption("bucketProvider", whutils.MINIO).
					WithConfigOption("bucketName", bucketName).
					WithConfigOption("accessKeyID", accessKeyID).
					WithConfigOption("secretAccessKey", secretAccessKey).
					WithConfigOption("useSSL", false).
					WithConfigOption("secure", false).
					WithConfigOption("endPoint", minioEndpoint).
					WithConfigOption("useRudderStorage", false).
					WithConfigOption("syncFrequency", "30").
					WithConfigOption("allowUsersContextTraits", true).
					WithConfigOption("underscoreDivideNumbers", true).
					WithConfigOption("cleanupObjectStorageFiles", true)
				for k, v := range tc.configOverride {
					destinationBuilder = destinationBuilder.WithConfigOption(k, v)
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

				t.Setenv("RSERVER_WAREHOUSE_CLICKHOUSE_MAX_PARALLEL_LOADS", "8")
				t.Setenv("RSERVER_WAREHOUSE_CLICKHOUSE_SLOW_QUERY_THRESHOLD", "0s")
				if useV2 {
					t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Warehouse.clickhouse.useV2Driver"), "true")
				}

				whth.BootstrapSvc(t, workspaceConfig, httpPort, jobsDBPort)

				db := tc.setupDB(t, context.Background())
				t.Cleanup(func() { _ = db.Close() })
				tables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}

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
					Schema:          database,
					Tables:          tables,
					SourceID:        sourceID,
					DestinationID:   destinationID,
					Config:          conf,
					WorkspaceID:     workspaceID,
					DestinationType: destType,
					JobsDB:          jobsDB,
					HTTPPort:        httpPort,
					Client:          sqlClient,
					UserID:          whth.GetUserId(destType),
					EventsFilePath:  tc.eventFilePrefix + ".events-1.json",
					TransformerURL:  transformerURL,
					Destination:     destination,
				}
				ts1.VerifyEvents(t)

				t.Log("setting up cluster")
				tc.clusterSetup(t, context.Background())

				t.Log("verifying test case 2")
				ts2 := whth.TestConfig{
					WriteKey:           writeKey,
					Schema:             database,
					Tables:             tables,
					SourceID:           sourceID,
					DestinationID:      destinationID,
					WarehouseEventsMap: tc.warehouseEvents2,
					Config:             conf,
					WorkspaceID:        workspaceID,
					DestinationType:    destType,
					JobsDB:             jobsDB,
					HTTPPort:           httpPort,
					Client:             sqlClient,
					UserID:             whth.GetUserId(destType),
					EventsFilePath:     tc.eventFilePrefix + ".events-2.json",
					TransformerURL:     transformerURL,
					Destination:        destination,
				}
				ts2.VerifyEvents(t)

				t.Log("verifying schema")
				tc.verifySchema(t, db, database)

				t.Log("verifying records")
				tc.verifyRecords(t, db, sourceID, destinationID, database)
			})
		}
	})

	t.Run("Validations", func(t *testing.T) {
		c := testcompose.New(t, compose.FilePaths([]string{clickhouseCompose, "../testdata/docker-compose.minio.yml"}))
		c.Start(context.Background())

		clickhousePort := c.Port("clickhouse", 9000)
		minioEndpoint := fmt.Sprintf("localhost:%d", c.Port("minio", 9000))

		dest := backendconfig.DestinationT{
			ID: "21Ev6TI6emCFDKph2Zn6XfTP7PI",
			Config: map[string]any{
				"host":             host,
				"database":         database,
				"cluster":          "",
				"user":             user,
				"password":         password,
				"port":             strconv.Itoa(clickhousePort),
				"secure":           false,
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
				ID:          "test_destination_id",
				Name:        "CLICKHOUSE",
				DisplayName: "ClickHouse",
			},
			Name:       "clickhouse-demo",
			Enabled:    true,
			RevisionID: "29eeuTnqbBKn0XVTj5z9XQIbaru",
		}
		if useV2 {
			t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Warehouse.clickhouse.useV2Driver"), "true")
		}
		whth.VerifyConfigurationTest(t, dest)
	})

	t.Run("Fetch schema", func(t *testing.T) {
		c := testcompose.New(t, compose.FilePaths([]string{clickhouseCompose}))
		c.Start(context.Background())

		workspaceID := whutils.RandHex()
		clickhousePort := c.Port("clickhouse", 9000)

		ctx := context.Background()
		namespace := "test_namespace"
		table := "test_table"

		db := connectDB(t, ctx, clickhousePort)
		defer func() { _ = db.Close() }()

		t.Run("Success", func(t *testing.T) {
			ch := newClickhouse(config.New())

			warehouse := model.Warehouse{
				Namespace:   fmt.Sprintf("%s_success", namespace),
				WorkspaceID: workspaceID,
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"host":     host,
						"port":     strconv.Itoa(clickhousePort),
						"database": database,
						"user":     user,
						"password": password,
					},
				},
			}

			err := ch.Setup(ctx, warehouse, newMockUploader(t, "", nil, nil))
			require.NoError(t, err)

			err = ch.CreateSchema(ctx)
			require.NoError(t, err)

			err = ch.CreateTable(ctx, table, model.TableSchema{
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

			schema, err := ch.FetchSchema(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, schema)
		})

		t.Run("Invalid host", func(t *testing.T) {
			ch := newClickhouse(config.New())

			warehouse := model.Warehouse{
				Namespace:   fmt.Sprintf("%s_invalid_host", namespace),
				WorkspaceID: workspaceID,
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"host":     "clickhouse",
						"port":     strconv.Itoa(clickhousePort),
						"database": database,
						"user":     user,
						"password": password,
					},
				},
			}

			err := ch.Setup(ctx, warehouse, newMockUploader(t, "", nil, nil))
			require.NoError(t, err)

			schema, err := ch.FetchSchema(ctx)
			require.ErrorContains(t, err, errors.New("dial tcp: lookup clickhouse").Error())
			require.Empty(t, schema)
		})

		t.Run("Invalid database", func(t *testing.T) {
			ch := newClickhouse(config.New())

			warehouse := model.Warehouse{
				Namespace:   fmt.Sprintf("%s_invalid_database", namespace),
				WorkspaceID: workspaceID,
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"host":     host,
						"port":     strconv.Itoa(clickhousePort),
						"database": "invalid_database",
						"user":     user,
						"password": password,
					},
				},
			}

			err := ch.Setup(ctx, warehouse, newMockUploader(t, "", nil, nil))
			require.NoError(t, err)

			schema, err := ch.FetchSchema(ctx)
			require.NoError(t, err)
			require.Empty(t, schema)
		})

		t.Run("Empty schema", func(t *testing.T) {
			ch := newClickhouse(config.New())

			warehouse := model.Warehouse{
				Namespace:   fmt.Sprintf("%s_empty_schema", namespace),
				WorkspaceID: workspaceID,
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"host":     host,
						"port":     strconv.Itoa(clickhousePort),
						"database": database,
						"user":     user,
						"password": password,
					},
				},
			}

			err := ch.Setup(ctx, warehouse, newMockUploader(t, "", nil, nil))
			require.NoError(t, err)

			err = ch.CreateSchema(ctx)
			require.NoError(t, err)

			schema, err := ch.FetchSchema(ctx)
			require.NoError(t, err)
			require.Empty(t, schema)
		})

		t.Run("Unrecognized schema", func(t *testing.T) {
			ch := newClickhouse(config.New())

			warehouse := model.Warehouse{
				Namespace:   fmt.Sprintf("%s_unrecognized_schema", namespace),
				WorkspaceID: workspaceID,
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"host":     host,
						"port":     strconv.Itoa(clickhousePort),
						"database": database,
						"user":     user,
						"password": password,
					},
				},
			}

			err := ch.Setup(ctx, warehouse, newMockUploader(t, "", nil, nil))
			require.NoError(t, err)

			err = ch.CreateSchema(ctx)
			require.NoError(t, err)

			_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (x Enum('hello' = 1, 'world' = 2)) ENGINE = TinyLog;",
				warehouse.Namespace,
				table,
			))
			require.NoError(t, err)

			schema, err := ch.FetchSchema(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, schema)
		})
	})

	t.Run("Load Table round trip", func(t *testing.T) {
		c := testcompose.New(t, compose.FilePaths([]string{clickhouseCompose, "../testdata/docker-compose.minio.yml"}))
		c.Start(context.Background())

		workspaceID := whutils.RandHex()
		minioPort := c.Port("minio", 9000)
		clickhousePort := c.Port("clickhouse", 9000)
		minioEndpoint := fmt.Sprintf("localhost:%d", minioPort)

		region := "us-east-1"
		table := "test_table"

		db := connectDB(t, context.Background(), clickhousePort)
		defer func() { _ = db.Close() }()

		testCases := []struct {
			name                        string
			fileName                    string
			S3EngineEnabledWorkspaceIDs []string
			disableNullable             bool
			disableLoadTableStats       bool

			// commitEvery only changes behaviour on v2, which drives the commit
			// cadence itself; v1 hands block_size to the driver and ignores it.
			// The cases still run on both, which makes v1 the control: the same
			// load has to produce the same rows however it was committed.
			commitEvery int
		}{
			{
				name:                  "normal loading using downloading of load files",
				fileName:              "testdata/load.csv.gz",
				disableLoadTableStats: false,
			},
			{
				name:                        "using s3 engine for loading",
				S3EngineEnabledWorkspaceIDs: []string{workspaceID},
				fileName:                    "testdata/load-copy.csv.gz",
				disableLoadTableStats:       false,
			},
			{
				name:                  "normal loading using downloading of load files with disable nullable",
				fileName:              "testdata/load.csv.gz",
				disableNullable:       true,
				disableLoadTableStats: false,
			},
			{
				name:                        "using s3 engine for loading with disable nullable",
				S3EngineEnabledWorkspaceIDs: []string{workspaceID},
				fileName:                    "testdata/load-copy.csv.gz",
				disableNullable:             true,
				disableLoadTableStats:       false,
			},
			{
				name:                  "normal loading using downloading of load files with disable load table stats",
				fileName:              "testdata/load.csv.gz",
				disableLoadTableStats: true,
			},
			{
				// load.csv.gz holds 16 rows, so this commits four blocks and
				// exercises a mid-file commit boundary.
				name:        "loading in blocks smaller than the file",
				fileName:    "testdata/load.csv.gz",
				commitEvery: 5,
			},
			{
				// 16 rows in blocks of 8 ends exactly on a boundary, which is
				// the case that costs one extra call to learn the stream ended.
				name:        "row count an exact multiple of the block size",
				fileName:    "testdata/load.csv.gz",
				commitEvery: 8,
			},
			{
				name:        "one row per block",
				fileName:    "testdata/load.csv.gz",
				commitEvery: 1,
			},
		}

		for i, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				conf := config.New()
				conf.Set(configKey("s3EngineEnabledWorkspaceIDs"), tc.S3EngineEnabledWorkspaceIDs)
				conf.Set(configKey("disableNullable"), tc.disableNullable)
				conf.Set(configKey("disableLoadTableStats"), tc.disableLoadTableStats)
				if tc.commitEvery > 0 {
					conf.Set(configKey("commitEvery"), tc.commitEvery)
				}

				ch := newClickhouse(conf)

				warehouse := model.Warehouse{
					Namespace:   fmt.Sprintf("test_namespace_%d", i),
					WorkspaceID: workspaceID,
					Destination: backendconfig.DestinationT{
						Config: map[string]any{
							"bucketProvider":  whutils.MINIO,
							"host":            host,
							"port":            strconv.Itoa(clickhousePort),
							"database":        database,
							"user":            user,
							"password":        password,
							"bucketName":      bucketName,
							"accessKeyID":     accessKeyID,
							"secretAccessKey": secretAccessKey,
							"endPoint":        minioEndpoint,
						},
					},
				}

				t.Log("Preparing load files metadata")
				f, err := os.Open(tc.fileName)
				require.NoError(t, err)
				defer func() { _ = f.Close() }()

				fm, err := filemanager.New(&filemanager.Settings{
					Provider: whutils.MINIO,
					Config: map[string]any{
						"bucketName":      bucketName,
						"accessKeyID":     accessKeyID,
						"secretAccessKey": secretAccessKey,
						"endPoint":        minioEndpoint,
						"forcePathStyle":  true,
						"disableSSL":      true,
						"region":          region,
						"enableSSE":       false,
					},
					Conf: config.Default,
				})
				require.NoError(t, err)

				ctx := context.Background()
				uploadOutput, err := fm.Upload(ctx, f, fmt.Sprintf("test_prefix_%d", i))
				require.NoError(t, err)

				mockUploader := newMockUploader(t,
					strconv.Itoa(minioPort),
					model.TableSchema{
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
					[]whutils.LoadFile{{Location: uploadOutput.Location}},
				)

				t.Log("Setting up clickhouse")
				err = ch.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				t.Log("Verifying connection")
				_, err = ch.Connect(ctx, warehouse)
				require.NoError(t, err)

				t.Log("Verifying empty schema")
				schema, err := ch.FetchSchema(ctx)
				require.NoError(t, err)
				require.Empty(t, schema)

				t.Log("Creating schema")
				err = ch.CreateSchema(ctx)
				require.NoError(t, err)

				t.Log("Creating schema twice should not fail")
				err = ch.CreateSchema(ctx)
				require.NoError(t, err)

				t.Log("Creating table")
				err = ch.CreateTable(ctx, table, model.TableSchema{
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

				t.Log("Adding columns")
				err = ch.AddColumns(ctx, table, []whutils.ColumnInfo{
					{Name: "alter_test_int", Type: "int"},
					{Name: "alter_test_float", Type: "float"},
					{Name: "alter_test_bool", Type: "boolean"},
					{Name: "alter_test_string", Type: "string"},
					{Name: "alter_test_datetime", Type: "datetime"},
				})
				require.NoError(t, err)

				t.Log("Verifying schema")
				schema, err = ch.FetchSchema(ctx)
				require.NoError(t, err)
				require.NotEmpty(t, schema)

				t.Log("verifying if columns are not like Nullable(T) if disableNullable set to true")
				if tc.disableNullable {
					rows, err := db.QueryContext(ctx, fmt.Sprintf(`select table, name, type from system.columns where database = '%s'`, warehouse.Namespace))
					require.NoError(t, err)

					defer func() { _ = rows.Close() }()

					var (
						tableName  string
						columnName string
						columnType string
					)

					for rows.Next() {
						err = rows.Scan(&tableName, &columnName, &columnType)
						require.NoError(t, err)

						if strings.Contains(columnType, "Nullable") {
							require.Fail(t, fmt.Sprintf("table %s column %s is of Nullable type even when disableNullable is set to true", tableName, columnName))
						}
					}
					require.NoError(t, rows.Err())
				}

				t.Log("Loading data into table")
				loadTableStats, err := ch.LoadTable(ctx, table)
				require.NoError(t, err)
				if !tc.disableLoadTableStats {
					require.NotEmpty(t, loadTableStats.RowsInserted)
				}

				t.Log("Drop table")
				err = ch.DropTable(ctx, table)
				require.NoError(t, err)

				t.Log("Creating users identifies and table")
				for _, tableName := range []string{whutils.IdentifiesTable, whutils.UsersTable} {
					err = ch.CreateTable(ctx, tableName, model.TableSchema{
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
				}

				t.Log("Drop users identifies and table")
				for _, tableName := range []string{whutils.IdentifiesTable, whutils.UsersTable} {
					err = ch.DropTable(ctx, tableName)
					require.NoError(t, err)
				}

				t.Log("Verifying empty schema")
				schema, err = ch.FetchSchema(ctx)
				require.NoError(t, err)
				require.Empty(t, schema)
			})
		}

		// The v2 loader concatenates the load files into one gzip stream, so a
		// block ends after commitEvery rows rather than at a file boundary.
		// Three copies of a sixteen-row file committed every 5 puts a commit
		// inside every file and a block across every boundary. v1 runs this too
		// as a control: it commits once at the end and has to produce the same
		// rows.
		t.Run("blocks spanning multiple load files", func(t *testing.T) {
			const (
				loadFileCount = 3
				commitEvery   = 5
			)

			conf := config.New()
			conf.Set(configKey("commitEvery"), commitEvery)

			ch := newClickhouse(conf)

			warehouse := model.Warehouse{
				Namespace:   "test_namespace_multiple_load_files",
				WorkspaceID: workspaceID,
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"bucketProvider":  whutils.MINIO,
						"host":            host,
						"port":            strconv.Itoa(clickhousePort),
						"database":        database,
						"user":            user,
						"password":        password,
						"bucketName":      bucketName,
						"accessKeyID":     accessKeyID,
						"secretAccessKey": secretAccessKey,
						"endPoint":        minioEndpoint,
					},
				},
			}

			tableSchema := model.TableSchema{
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
			}

			fm, err := filemanager.New(&filemanager.Settings{
				Provider: whutils.MINIO,
				Config: map[string]any{
					"bucketName":      bucketName,
					"accessKeyID":     accessKeyID,
					"secretAccessKey": secretAccessKey,
					"endPoint":        minioEndpoint,
					"forcePathStyle":  true,
					"disableSSL":      true,
					"region":          region,
					"enableSSE":       false,
				},
				Conf: config.Default,
			})
			require.NoError(t, err)

			ctx := context.Background()

			t.Log("Preparing load files metadata")
			var expectedRows int
			loadFiles := make([]whutils.LoadFile, 0, loadFileCount)
			for n := range loadFileCount {
				path, rows := loadFileWithUniqueIDs(t, tableSchema, fmt.Sprintf("copy-%d", n))
				expectedRows += rows

				f, err := os.Open(path)
				require.NoError(t, err)

				uploadOutput, err := fm.Upload(ctx, f, fmt.Sprintf("test_prefix_multiple_load_files_%d", n))
				_ = f.Close()
				require.NoError(t, err)

				loadFiles = append(loadFiles, whutils.LoadFile{Location: uploadOutput.Location})
			}

			mockUploader := newMockUploader(t, strconv.Itoa(minioPort), tableSchema, loadFiles)

			t.Log("Setting up clickhouse")
			require.NoError(t, ch.Setup(ctx, warehouse, mockUploader))
			require.NoError(t, ch.CreateSchema(ctx))
			require.NoError(t, ch.CreateTable(ctx, table, tableSchema))

			t.Log("Loading data from every load file")
			loadTableStats, err := ch.LoadTable(ctx, table)
			require.NoError(t, err)
			require.EqualValues(t, expectedRows, loadTableStats.RowsInserted)

			// uniqExact rather than count: a block sent twice would still be
			// counted by RowsInserted, but it cannot invent new ids. Every row
			// was given a unique id, so this is the count that shows each of
			// them landed exactly once.
			var distinctIDs uint64
			require.NoError(t, db.QueryRowContext(ctx,
				fmt.Sprintf(`SELECT uniqExact(id) FROM %q.%q`, warehouse.Namespace, table),
			).Scan(&distinctIDs))
			require.EqualValues(t, expectedRows, distinctIDs)
		})
	})

	t.Run("Test connection", func(t *testing.T) {
		c := testcompose.New(t, compose.FilePaths([]string{clickhouseCompose}))
		c.Start(context.Background())

		workspaceID := whutils.RandHex()
		clickhousePort := c.Port("clickhouse", 9000)

		ctx := context.Background()
		namespace := "test_namespace"
		timeout := 5 * time.Second

		db := connectDB(t, context.Background(), clickhousePort)
		defer func() { _ = db.Close() }()

		testCases := []struct {
			name      string
			host      string
			tlConfig  string
			timeout   time.Duration
			wantError error
		}{
			{
				name:      "DeadlineExceeded",
				wantError: errors.New("connection timeout: context deadline exceeded"),
			},
			{
				name:    "Success",
				timeout: timeout,
			},
			{
				name:     "TLS config",
				timeout:  timeout,
				tlConfig: "test-tls-config",
			},
			{
				name:      "No such host",
				timeout:   timeout,
				wantError: errors.New(`dial tcp: lookup clickhouse`),
				host:      "clickhouse",
			},
		}

		for i, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ch := newClickhouse(config.New())

				host := host
				if tc.host != "" {
					host = tc.host
				}

				warehouse := model.Warehouse{
					Namespace:   namespace,
					WorkspaceID: workspaceID,
					Destination: backendconfig.DestinationT{
						ID: fmt.Sprintf("test-destination-%d", i),
						Config: map[string]any{
							"bucketProvider": whutils.MINIO,
							"host":           host,
							"port":           strconv.Itoa(clickhousePort),
							"database":       database,
							"user":           user,
							"password":       password,
							"caCertificate":  tc.tlConfig,
						},
					},
				}

				err := ch.Setup(ctx, warehouse, newMockUploader(t, "", nil, nil))
				require.NoError(t, err)

				ch.SetConnectionTimeout(tc.timeout)

				ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
				defer cancel()

				err = ch.TestConnection(ctx, warehouse)
				if tc.wantError != nil {
					require.ErrorContains(t, err, tc.wantError.Error())
					return
				}
				require.NoError(t, err)
			})
		}
	})

	// A path listed in jsonPaths reaches the warehouse as a json column, which v2
	// declares as a native JSON column. It needs ClickHouse 25.3 or newer, so it
	// only runs against the server v2 is tested on.
	t.Run("Load table with a JSON column", func(t *testing.T) {
		if !useV2 {
			t.Skip("native JSON columns are a v2 feature; v1 stores the payload as text")
		}

		c := testcompose.New(t, compose.FilePaths([]string{clickhouseCompose, "../testdata/docker-compose.minio.yml"}))
		c.Start(context.Background())

		workspaceID := whutils.RandHex()
		clickhousePort := c.Port("clickhouse", 9000)
		minioPort := c.Port("minio", 9000)
		minioEndpoint := fmt.Sprintf("localhost:%d", minioPort)

		ctx := context.Background()
		namespace := "test_namespace_json"
		tableName := "json_paths_test_table"
		notNullableTable := "json_paths_not_nullable_table"
		region := "us-east-1"

		dsn := fmt.Sprintf("tcp://%s:%d?compress=false&database=%s&password=%s&secure=false&skip_verify=true&username=%s",
			host, clickhousePort, database, password, user,
		)
		db := connectClickhouseDB(t, ctx, dsn)
		defer func() { _ = db.Close() }()

		tableSchema := model.TableSchema{
			"id":            "string",
			"context_props": "json",
		}

		ch := newClickhouse(config.New())

		warehouse := model.Warehouse{
			Namespace:   namespace,
			WorkspaceID: workspaceID,
			Destination: backendconfig.DestinationT{
				Config: map[string]any{
					"bucketProvider":  whutils.MINIO,
					"host":            host,
					"port":            strconv.Itoa(clickhousePort),
					"database":        database,
					"user":            user,
					"password":        password,
					"bucketName":      bucketName,
					"accessKeyID":     accessKeyID,
					"secretAccessKey": secretAccessKey,
					"endPoint":        minioEndpoint,
				},
			},
		}

		f, err := os.Open("testdata/load-json.csv.gz")
		require.NoError(t, err)
		defer func() { _ = f.Close() }()

		fm, err := filemanager.New(&filemanager.Settings{
			Provider: whutils.MINIO,
			Config: map[string]any{
				"bucketName":      bucketName,
				"accessKeyID":     accessKeyID,
				"secretAccessKey": secretAccessKey,
				"endPoint":        minioEndpoint,
				"forcePathStyle":  true,
				"disableSSL":      true,
				"region":          region,
				"enableSSE":       false,
			},
			Conf: config.Default,
		})
		require.NoError(t, err)

		uploadOutput, err := fm.Upload(ctx, f, "json_paths_prefix")
		require.NoError(t, err)

		mockUploader := newMockUploader(t, strconv.Itoa(minioPort), tableSchema, []whutils.LoadFile{
			{Location: uploadOutput.Location},
		})

		require.NoError(t, ch.Setup(ctx, warehouse, mockUploader))
		require.NoError(t, ch.CreateSchema(ctx))
		require.NoError(t, ch.CreateTable(ctx, tableName, tableSchema))

		t.Run("column is declared with the driver's JSON type", func(t *testing.T) {
			var columnType string
			require.NoError(t, db.QueryRowContext(ctx,
				"SELECT type FROM system.columns WHERE database = ? AND table = ? AND name = 'context_props'",
				namespace, tableName,
			).Scan(&columnType))

			// Nullable like any other scalar column: ClickHouse has allowed
			// Nullable(JSON) since 25.3.
			require.Equal(t, "Nullable(JSON)", columnType)
		})

		t.Run("load", func(t *testing.T) {
			_, err := ch.LoadTable(ctx, tableName)
			require.NoError(t, err)

			var rows int64
			require.NoError(t, db.QueryRowContext(ctx,
				fmt.Sprintf("SELECT count(*) FROM %q.%q", namespace, tableName),
			).Scan(&rows))
			require.EqualValues(t, 3, rows)
		})

		t.Run("payload round trips", func(t *testing.T) {
			var payload string
			require.NoError(t, db.QueryRowContext(ctx,
				fmt.Sprintf("SELECT toString(context_props) FROM %q.%q WHERE id = 'row-1'", namespace, tableName),
			).Scan(&payload))
			require.Contains(t, payload, "USD")
			require.Contains(t, payload, "10")
		})

		// An empty cell is not valid JSON, so it resolves to null exactly as a
		// cell that fails to parse does for every other type.
		t.Run("an empty cell is stored as null", func(t *testing.T) {
			var isNull bool
			require.NoError(t, db.QueryRowContext(ctx,
				fmt.Sprintf("SELECT context_props IS NULL FROM %q.%q WHERE id = 'row-2'", namespace, tableName),
			).Scan(&isNull))
			require.True(t, isNull)
		})

		// Reading a path out of the column is what separates a real JSON column
		// from text holding the same bytes.
		t.Run("paths inside the column are queryable", func(t *testing.T) {
			var revenue string
			require.NoError(t, db.QueryRowContext(ctx,
				fmt.Sprintf("SELECT toString(context_props.revenue) FROM %q.%q WHERE id = 'row-1'", namespace, tableName),
			).Scan(&revenue))
			require.Equal(t, "10", revenue)

			var nested string
			require.NoError(t, db.QueryRowContext(ctx,
				fmt.Sprintf("SELECT toString(context_props.nested.a) FROM %q.%q WHERE id = 'row-3'", namespace, tableName),
			).Scan(&nested))
			require.Contains(t, nested, "1")
		})

		// The users table wraps every column in SimpleAggregateFunction(anyLast, ...)
		// so rows collapsing on id take the newest value. That holds for JSON too.
		t.Run("users table", func(t *testing.T) {
			usersSchema := model.TableSchema{
				"id":                     "string",
				"received_at":            "datetime",
				"context_traits_address": "json",
			}
			require.NoError(t, ch.CreateTable(ctx, whutils.UsersTable, usersSchema))

			var columnType string
			require.NoError(t, db.QueryRowContext(ctx,
				"SELECT type FROM system.columns WHERE database = ? AND table = ? AND name = 'context_traits_address'",
				namespace, whutils.UsersTable,
			).Scan(&columnType))
			require.Equal(t, "SimpleAggregateFunction(anyLast, Nullable(JSON))", columnType)

			for _, city := range []string{"Bengaluru", "Mumbai"} {
				_, err := db.ExecContext(ctx, fmt.Sprintf(
					`INSERT INTO %q.%q (id, received_at, context_traits_address) VALUES ('user-1', now(), '{"city":"%s"}')`,
					namespace, whutils.UsersTable, city,
				))
				require.NoError(t, err)
			}

			_, err := db.ExecContext(ctx, fmt.Sprintf(`OPTIMIZE TABLE %q.%q FINAL`, namespace, whutils.UsersTable))
			require.NoError(t, err)

			var rows int64
			require.NoError(t, db.QueryRowContext(ctx,
				fmt.Sprintf(`SELECT count(*) FROM %q.%q`, namespace, whutils.UsersTable),
			).Scan(&rows))
			require.EqualValues(t, 1, rows, "the two rows for user-1 collapse on merge")

			// anyLast keeps the newest write, and the value stays queryable as JSON.
			var city string
			require.NoError(t, db.QueryRowContext(ctx,
				fmt.Sprintf(`SELECT toString(context_traits_address.city) FROM %q.%q WHERE id = 'user-1'`, namespace, whutils.UsersTable),
			).Scan(&city))
			require.Equal(t, "Mumbai", city)
		})

		// A jsonPath added to a destination after the table exists arrives through
		// ALTER TABLE, which takes the same declaration path as CREATE.
		t.Run("added to an existing table", func(t *testing.T) {
			require.NoError(t, ch.AddColumns(ctx, tableName, []whutils.ColumnInfo{
				{Name: "context_extra", Type: model.JSONDataType},
			}))

			var columnType string
			require.NoError(t, db.QueryRowContext(ctx,
				"SELECT type FROM system.columns WHERE database = ? AND table = ? AND name = 'context_extra'",
				namespace, tableName,
			).Scan(&columnType))
			require.Equal(t, "Nullable(JSON)", columnType)

			// Rows written before the column existed read back as null, the same
			// backfill every other Nullable column gets from ALTER TABLE.
			var isNull bool
			require.NoError(t, db.QueryRowContext(ctx,
				fmt.Sprintf(`SELECT context_extra IS NULL FROM %q.%q WHERE id = 'row-1'`, namespace, tableName),
			).Scan(&isNull))
			require.True(t, isNull)
		})

		// disableNullable is the other half of the matrix. The column is declared
		// bare, so an empty cell has no null to fall back to and resolves to the
		// type's entry in datatypeDefaultValuesMap instead.
		t.Run("nullable disabled", func(t *testing.T) {
			conf := config.New()
			conf.Set(configKey("disableNullable"), true)

			chNotNullable := newClickhouse(conf)
			require.NoError(t, chNotNullable.Setup(ctx, warehouse, mockUploader))
			require.NoError(t, chNotNullable.CreateTable(ctx, notNullableTable, tableSchema))

			var columnType string
			require.NoError(t, db.QueryRowContext(ctx,
				"SELECT type FROM system.columns WHERE database = ? AND table = ? AND name = 'context_props'",
				namespace, notNullableTable,
			).Scan(&columnType))
			require.Equal(t, "JSON", columnType)

			_, err := chNotNullable.LoadTable(ctx, notNullableTable)
			require.NoError(t, err)

			// The empty cell is the whole point of this mode: it has to land as
			// {} rather than null, and rows must stay countable.
			var isNull bool
			var payload string
			require.NoError(t, db.QueryRowContext(ctx,
				fmt.Sprintf(`SELECT context_props IS NULL, toString(context_props) FROM %q.%q WHERE id = 'row-2'`, namespace, notNullableTable),
			).Scan(&isNull, &payload))
			require.False(t, isNull)
			require.Equal(t, "{}", payload)

			// A populated cell is unaffected by the mode.
			var revenue string
			require.NoError(t, db.QueryRowContext(ctx,
				fmt.Sprintf(`SELECT toString(context_props.revenue) FROM %q.%q WHERE id = 'row-1'`, namespace, notNullableTable),
			).Scan(&revenue))
			require.Equal(t, "10", revenue)

			// identifies is exempt from disableNullable, for json as for
			// every other type.
			require.NoError(t, chNotNullable.CreateTable(ctx, whutils.IdentifiesTable, model.TableSchema{
				"id":            "string",
				"received_at":   "datetime",
				"context_props": "json",
			}))
			require.NoError(t, db.QueryRowContext(ctx,
				"SELECT type FROM system.columns WHERE database = ? AND table = ? AND name = 'context_props'",
				namespace, whutils.IdentifiesTable,
			).Scan(&columnType))
			require.Equal(t, "Nullable(JSON)", columnType)
		})

		// FetchSchema looks the rendered type up as a raw string, so a rendering
		// with no reverse mapping is dropped silently and only bumps a counter.
		// Every shape this block created has to survive the round trip.
		t.Run("fetched schema reports every rendering as json", func(t *testing.T) {
			schema, err := ch.FetchSchema(ctx)
			require.NoError(t, err)

			require.Equal(t, model.JSONDataType, schema[tableName]["context_props"], "Nullable(JSON)")
			require.Equal(t, model.JSONDataType, schema[tableName]["context_extra"], "Nullable(JSON) added by ALTER")
			require.Equal(t, model.JSONDataType, schema[notNullableTable]["context_props"], "JSON")
			require.Equal(t, model.JSONDataType, schema[whutils.UsersTable]["context_traits_address"], "SimpleAggregateFunction(anyLast, Nullable(JSON))")
			require.Equal(t, model.JSONDataType, schema[whutils.IdentifiesTable]["context_props"], "Nullable(JSON) in identifies")
		})
	})

	t.Run("Load test table", func(t *testing.T) {
		c := testcompose.New(t, compose.FilePaths([]string{clickhouseCompose}))
		c.Start(context.Background())

		workspaceID := whutils.RandHex()
		clickhousePort := c.Port("clickhouse", 9000)

		ctx := context.Background()
		namespace := "test_namespace"
		tableName := whutils.CTStagingTablePrefix + "_test_table"
		testColumns := model.TableSchema{
			"id":  "int",
			"val": "string",
		}
		testPayload := map[string]any{
			"id":  1,
			"val": "RudderStack",
		}

		db := connectDB(t, context.Background(), clickhousePort)
		defer func() { _ = db.Close() }()

		testCases := []struct {
			name      string
			wantError error
			payload   map[string]any
		}{
			{
				name: "Success",
			},
			{
				name: "Invalid columns",
				payload: map[string]any{
					"invalid_val": "Invalid Data",
				},
				wantError: errors.New("code: 16, message: No such column invalid_val in table test_namespace.setup_test_staging"),
			},
		}

		for i, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ch := newClickhouse(config.New())

				warehouse := model.Warehouse{
					Namespace:   namespace,
					WorkspaceID: workspaceID,
					Destination: backendconfig.DestinationT{
						Config: map[string]any{
							"bucketProvider": whutils.MINIO,
							"host":           host,
							"port":           strconv.Itoa(clickhousePort),
							"database":       database,
							"user":           user,
							"password":       password,
						},
					},
				}

				payload := make(map[string]any)
				maps.Copy(payload, tc.payload)
				maps.Copy(payload, testPayload)

				err := ch.Setup(ctx, warehouse, newMockUploader(t, "", nil, nil))
				require.NoError(t, err)

				err = ch.CreateSchema(ctx)
				require.NoError(t, err)

				tableName := fmt.Sprintf("%s_%d", tableName, i)

				err = ch.CreateTable(ctx, tableName, testColumns)
				require.NoError(t, err)

				err = ch.TestLoadTable(ctx, "", tableName, payload, "")
				if tc.wantError != nil {
					require.ErrorContains(t, err, tc.wantError.Error())
					return
				}
				require.NoError(t, err)
			})
		}
	})
}

// connectClickhouseDBV2 opens a connection through the v2 fork, for the cases
// that touch a type v1 cannot represent.
// clickhouseExceptionCode returns the server error code from whichever driver
// raised it. The two surface a server exception as different concrete types -
// v1 as *clickhouse-go.Exception, v2 as the fork's alias of its proto one - so
// matching on either alone fails under the other. Both carry an int32 Code.
func clickhouseExceptionCode(t testing.TB, err error) int32 {
	t.Helper()

	var v1Err *clickhousestd.Exception
	if errors.As(err, &v1Err) {
		return v1Err.Code
	}

	var v2Err *clickhousev2.Exception
	if errors.As(err, &v2Err) {
		return v2Err.Code
	}

	require.Failf(t, "not a clickhouse server exception", "%T: %v", err, err)
	return 0
}

func connectClickhouseDBV2(t testing.TB, host string, port int, database, user, password string) *sql.DB {
	t.Helper()

	db := clickhousev2.OpenDB(&clickhousev2.Options{
		Addr: []string{net.JoinHostPort(host, strconv.Itoa(port))},
		Auth: clickhousev2.Auth{
			Database: database,
			Username: user,
			Password: password,
		},
		Protocol: clickhousev2.Native,
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	require.Eventually(t, func() bool {
		if err := db.PingContext(ctx); err != nil {
			t.Log("Ping failed:", err)
			return false
		}
		return true
	}, time.Minute, time.Second)

	t.Cleanup(func() { _ = db.Close() })

	return db
}

func connectClickhouseDB(t testing.TB, ctx context.Context, dsn string) *sql.DB {
	t.Helper()

	db, err := sql.Open("clickhouse", dsn)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	require.Eventually(t, func() bool {
		if err := db.PingContext(ctx); err != nil {
			t.Log("Ping failed:", err)
			return false
		}
		return true
	}, time.Minute, time.Second)

	require.NoError(t, db.PingContext(ctx))
	return db
}

// connect comes from the suite so the DDL below runs over the driver under
// test, rather than always over v1.
func initializeClickhouseClusterMode(t *testing.T, clusterDBs []*sql.DB, tables []string, clusterPost int, connect func(testing.TB, context.Context, int) *sql.DB) {
	t.Helper()

	type columnInfo struct {
		columnName string
		columnType string
	}

	tableColumnInfoMap := map[string][]columnInfo{
		"identifies": {
			{
				columnName: "context_passed_ip",
				columnType: "Nullable(String)",
			},
			{
				columnName: "context_library_name",
				columnType: "Nullable(String)",
			},
		},
		"product_track": {
			{
				columnName: "revenue",
				columnType: "Nullable(Float64)",
			},
			{
				columnName: "context_passed_ip",
				columnType: "Nullable(String)",
			},
			{
				columnName: "context_library_name",
				columnType: "Nullable(String)",
			},
		},
		"tracks": {
			{
				columnName: "context_passed_ip",
				columnType: "Nullable(String)",
			},
			{
				columnName: "context_library_name",
				columnType: "Nullable(String)",
			},
		},
		"users": {
			{
				columnName: "context_passed_ip",
				columnType: "Nullable(String)",
			},
			{
				columnName: "context_library_name",
				columnType: "SimpleAggregateFunction(anyLast, Nullable(String))",
			},
		},
		"pages": {
			{
				columnName: "context_passed_ip",
				columnType: "Nullable(String)",
			},
			{
				columnName: "context_library_name",
				columnType: "Nullable(String)",
			},
		},
		"screens": {
			{
				columnName: "context_passed_ip",
				columnType: "Nullable(String)",
			},
			{
				columnName: "context_library_name",
				columnType: "Nullable(String)",
			},
		},
		"aliases": {
			{
				columnName: "context_passed_ip",
				columnType: "Nullable(String)",
			},
			{
				columnName: "context_library_name",
				columnType: "Nullable(String)",
			},
		},
		"groups": {
			{
				columnName: "context_passed_ip",
				columnType: "Nullable(String)",
			},
			{
				columnName: "context_library_name",
				columnType: "Nullable(String)",
			},
		},
	}

	clusterDB := clusterDBs[0]

	// Rename tables to tables_shard
	for _, table := range tables {
		sqlStatement := fmt.Sprintf("RENAME TABLE %[1]s to %[1]s_shard ON CLUSTER rudder_cluster;", table)
		log.Printf("Renaming tables to sharded tables for distribution view for clickhouse cluster with sqlStatement: %s", sqlStatement)

		require.NoError(t, whth.WithConstantRetries(func() error {
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

		require.NoError(t, whth.WithConstantRetries(func() error {
			_, err := clusterDB.Exec(sqlStatement)
			return err
		}))
	}

	t.Run("Create Drop Create", func(t *testing.T) {
		clusterDB := connect(t, context.Background(), clusterPost)
		defer func() {
			_ = clusterDB.Close()
		}()

		t.Run("with UUID", func(t *testing.T) {
			testTable := "test_table_with_uuid"

			createTableSQLStatement := func() string {
				return fmt.Sprintf(`
					CREATE TABLE IF NOT EXISTS "rudderdb".%[1]q ON CLUSTER "rudder_cluster" (
					  "id" String, "received_at" DateTime
					) ENGINE = ReplicatedReplacingMergeTree(
					  '/clickhouse/{cluster}/tables/%[2]s/{database}/{table}',
					  '{replica}'
					)
					ORDER BY
					  ("received_at", "id") PARTITION BY toDate(received_at);`,
					testTable,
					uuid.New().String(),
				)
			}

			require.NoError(t, whth.WithConstantRetries(func() error {
				_, err := clusterDB.Exec(createTableSQLStatement())
				return err
			}))
			require.NoError(t, whth.WithConstantRetries(func() error {
				_, err := clusterDB.Exec(fmt.Sprintf(`DROP TABLE rudderdb.%[1]s ON CLUSTER "rudder_cluster";`, testTable))
				return err
			}))
			require.NoError(t, whth.WithConstantRetries(func() error {
				_, err := clusterDB.Exec(createTableSQLStatement())
				return err
			}))
		})
		t.Run("without UUID", func(t *testing.T) {
			testTable := "test_table_without_uuid"

			createTableSQLStatement := func() string {
				return fmt.Sprintf(`
					CREATE TABLE IF NOT EXISTS "rudderdb".%[1]q ON CLUSTER "rudder_cluster" (
					  "id" String, "received_at" DateTime
					) ENGINE = ReplicatedReplacingMergeTree(
					  '/clickhouse/{cluster}/tables/{database}/{table}',
					  '{replica}'
					)
					ORDER BY
					  ("received_at", "id") PARTITION BY toDate(received_at);`,
					testTable,
				)
			}

			require.NoError(t, whth.WithConstantRetries(func() error {
				_, err := clusterDB.Exec(createTableSQLStatement())
				return err
			}))
			require.NoError(t, whth.WithConstantRetries(func() error {
				_, err := clusterDB.Exec(fmt.Sprintf(`DROP TABLE rudderdb.%[1]s ON CLUSTER "rudder_cluster";`, testTable))
				return err
			}))

			err := whth.WithConstantRetries(func() error {
				_, err := clusterDB.Exec(createTableSQLStatement())
				return err
			})
			require.Error(t, err)

			require.EqualValues(t, 253, clickhouseExceptionCode(t, err))
		})
	})
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
					columnInfo.columnName,
					columnInfo.columnType,
				)
			}
			sqlStatement = strings.TrimSuffix(sqlStatement, ",")
			log.Printf("Altering columns for distribution view for clickhouse cluster with sqlStatement: %s", sqlStatement)

			require.NoError(t, whth.WithConstantRetries(func() error {
				_, err := clusterDB.Exec(sqlStatement)
				return err
			}))
		}
	}
}

func newMockUploader(
	t testing.TB,
	minioPort string,
	tableSchema model.TableSchema,
	metadata []whutils.LoadFile,
) *mockuploader.MockUploader {
	var sampleLocation string
	if len(metadata) > 0 {
		minioHostPort := fmt.Sprintf("localhost:%s", minioPort)
		sampleLocation = strings.Replace(metadata[0].Location, minioHostPort, "minio:9000", 1)
	}

	ctrl := gomock.NewController(t)
	u := mockuploader.NewMockUploader(ctrl)
	u.EXPECT().GetSampleLoadFileLocation(gomock.Any(), gomock.Any()).Return(sampleLocation, nil).AnyTimes()
	u.EXPECT().GetTableSchemaInUpload(gomock.Any()).Return(tableSchema).AnyTimes()
	u.EXPECT().GetLoadFilesMetadata(gomock.Any(), gomock.Any()).Return(metadata, nil).AnyTimes()
	u.EXPECT().UseRudderStorage().Return(false).AnyTimes()
	u.EXPECT().IsWarehouseSchemaEmpty().Return(true).AnyTimes()

	return u
}

// loadFileWithUniqueIDs writes a copy of testdata/load.csv.gz into a temp dir
// with every id rewritten to be unique, and returns its path and row count.
//
// The fixture reuses just two ids across its sixteen rows, and the load table
// is a ReplacingMergeTree ordered by (received_at, id). Loaded as-is, the rows
// sharing a sort key are collapsed on merge and the table ends up holding one
// row per distinct id rather than one per row loaded.
func loadFileWithUniqueIDs(t testing.TB, schema model.TableSchema, tag string) (string, int) {
	t.Helper()

	idIndex := slices.Index(whutils.SortColumnKeysFromColumnMap(schema), "id")
	require.GreaterOrEqual(t, idIndex, 0, "schema has no id column")

	f, err := os.Open("testdata/load.csv.gz")
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	gzr, err := gzip.NewReader(f)
	require.NoError(t, err)
	defer func() { _ = gzr.Close() }()

	records, err := csv.NewReader(gzr).ReadAll()
	require.NoError(t, err)

	dst := filepath.Join(t.TempDir(), "load.csv.gz")
	out, err := os.Create(dst)
	require.NoError(t, err)

	gzw := gzip.NewWriter(out)
	w := csv.NewWriter(gzw)
	for i, record := range records {
		require.Greater(t, len(record), idIndex, "load file has fewer columns than the schema")
		record[idIndex] = fmt.Sprintf("%s-%s-%d", tag, record[idIndex], i)
		require.NoError(t, w.Write(record))
	}
	w.Flush()
	require.NoError(t, w.Error())
	require.NoError(t, gzw.Close())
	require.NoError(t, out.Close())

	return dst, len(records)
}
