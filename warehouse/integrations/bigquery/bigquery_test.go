package bigquery_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"go.uber.org/mock/gomock"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	whbigquery "github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery"
	bqhelper "github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery/testhelper"
	whth "github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}
	if _, exists := os.LookupEnv(bqhelper.TestKey); !exists {
		if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
			t.Fatalf("%s environment variable not set", bqhelper.TestKey)
		}
		t.Skipf("Skipping %s as %s is not set", t.Name(), bqhelper.TestKey)
	}

	misc.Init()
	validations.Init()
	whutils.Init()

	destType := whutils.BQ

	credentials, err := bqhelper.GetBQTestCredentials()
	require.NoError(t, err)

	t.Run("Event flow", func(t *testing.T) {
		httpPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		c := testcompose.New(t, compose.FilePaths([]string{"../testdata/docker-compose.jobsdb.yml", "../testdata/docker-compose.transformer.yml"}))
		c.Start(context.Background())

		workspaceID := whutils.RandHex()
		jobsDBPort := c.Port("jobsDb", 5432)
		transformerURL := fmt.Sprintf("http://localhost:%d", c.Port("transformer", 9090))

		jobsDB := whth.JobsDB(t, jobsDBPort)

		expectedUploadJobSchema := model.Schema{
			"pages":                       {"sent_at": "TIMESTAMP", "id": "STRING", "loaded_at": "TIMESTAMP", "_as": "STRING", "title": "STRING", "context_request_ip": "STRING", "context_destination_type": "STRING", "timestamp": "TIMESTAMP", "context_ip": "STRING", "received_at": "TIMESTAMP", "url": "STRING", "uuid_ts": "TIMESTAMP", "name": "STRING", "context_destination_id": "STRING", "user_id": "STRING", "context_source_type": "STRING", "_between": "STRING", "context_source_id": "STRING", "original_timestamp": "TIMESTAMP"},
			"users":                       {"context_source_id": "STRING", "received_at": "TIMESTAMP", "context_destination_id": "STRING", "context_source_type": "STRING", "context_traits_logins": "INT64", "_as": "STRING", "sent_at": "TIMESTAMP", "original_timestamp": "TIMESTAMP", "context_traits_as": "STRING", "context_traits_between": "STRING", "name": "STRING", "email": "STRING", "context_traits_name": "STRING", "timestamp": "TIMESTAMP", "context_traits_email": "STRING", "id": "STRING", "_between": "STRING", "context_ip": "STRING", "context_destination_type": "STRING", "uuid_ts": "TIMESTAMP", "context_request_ip": "STRING", "logins": "INT64", "loaded_at": "TIMESTAMP"},
			"rudder_identity_mappings":    {"merge_property_type": "STRING", "merge_property_value": "STRING", "rudder_id": "STRING", "updated_at": "TIMESTAMP"},
			"identifies":                  {"context_traits_email": "STRING", "uuid_ts": "TIMESTAMP", "loaded_at": "TIMESTAMP", "user_id": "STRING", "email": "STRING", "logins": "INT64", "received_at": "TIMESTAMP", "_as": "STRING", "context_traits_as": "STRING", "context_traits_between": "STRING", "sent_at": "TIMESTAMP", "original_timestamp": "TIMESTAMP", "context_source_type": "STRING", "context_traits_logins": "INT64", "context_source_id": "STRING", "context_traits_name": "STRING", "id": "STRING", "context_ip": "STRING", "context_destination_type": "STRING", "_between": "STRING", "timestamp": "TIMESTAMP", "name": "STRING", "context_request_ip": "STRING", "context_destination_id": "STRING"},
			"product_track":               {"event": "STRING", "context_source_id": "STRING", "review_id": "STRING", "event_text": "STRING", "id": "STRING", "uuid_ts": "TIMESTAMP", "context_destination_type": "STRING", "sent_at": "TIMESTAMP", "review_body": "STRING", "context_request_ip": "STRING", "_between": "STRING", "timestamp": "TIMESTAMP", "original_timestamp": "TIMESTAMP", "context_destination_id": "STRING", "product_id": "STRING", "context_ip": "STRING", "_as": "STRING", "loaded_at": "TIMESTAMP", "context_source_type": "STRING", "user_id": "STRING", "received_at": "TIMESTAMP", "rating": "INT64"},
			"rudder_identity_merge_rules": {"merge_property_1_type": "STRING", "merge_property_1_value": "STRING", "merge_property_2_type": "STRING", "merge_property_2_value": "STRING"},
			"screens":                     {"original_timestamp": "TIMESTAMP", "context_destination_type": "STRING", "title": "STRING", "context_ip": "STRING", "received_at": "TIMESTAMP", "context_request_ip": "STRING", "user_id": "STRING", "_between": "STRING", "context_destination_id": "STRING", "_as": "STRING", "context_source_id": "STRING", "sent_at": "TIMESTAMP", "timestamp": "TIMESTAMP", "context_source_type": "STRING", "id": "STRING", "uuid_ts": "TIMESTAMP", "loaded_at": "TIMESTAMP", "name": "STRING", "url": "STRING"},
			"_groups":                     {"sent_at": "TIMESTAMP", "context_source_id": "STRING", "context_destination_id": "STRING", "employees": "INT64", "group_id": "STRING", "industry": "STRING", "timestamp": "TIMESTAMP", "user_id": "STRING", "loaded_at": "TIMESTAMP", "plan": "STRING", "original_timestamp": "TIMESTAMP", "context_source_type": "STRING", "id": "STRING", "context_request_ip": "STRING", "uuid_ts": "TIMESTAMP", "_between": "STRING", "_as": "STRING", "name": "STRING", "context_ip": "STRING", "received_at": "TIMESTAMP", "context_destination_type": "STRING"},
			"tracks":                      {"user_id": "STRING", "context_source_id": "STRING", "uuid_ts": "TIMESTAMP", "original_timestamp": "TIMESTAMP", "loaded_at": "TIMESTAMP", "context_ip": "STRING", "id": "STRING", "context_destination_type": "STRING", "received_at": "TIMESTAMP", "context_destination_id": "STRING", "sent_at": "TIMESTAMP", "context_request_ip": "STRING", "context_source_type": "STRING", "event_text": "STRING", "event": "STRING", "timestamp": "TIMESTAMP"},
			"aliases":                     {"context_destination_id": "STRING", "context_ip": "STRING", "loaded_at": "TIMESTAMP", "received_at": "TIMESTAMP", "uuid_ts": "TIMESTAMP", "sent_at": "TIMESTAMP", "context_source_type": "STRING", "context_destination_type": "STRING", "previous_id": "STRING", "original_timestamp": "TIMESTAMP", "timestamp": "TIMESTAMP", "context_request_ip": "STRING", "user_id": "STRING", "context_source_id": "STRING", "id": "STRING"},
		}
		expectedSourcesSchema := model.Schema{
			"rudder_identity_mappings":    {"merge_property_type": "STRING", "merge_property_value": "STRING", "rudder_id": "STRING", "updated_at": "TIMESTAMP"},
			"google_sheet":                {"sent_at": "TIMESTAMP", "review_id": "STRING", "rating": "INT64", "received_at": "TIMESTAMP", "original_timestamp": "TIMESTAMP", "context_sources_job_run_id": "STRING", "context_sources_job_id": "STRING", "context_sources_version": "STRING", "context_destination_type": "STRING", "context_source_type": "STRING", "_as": "STRING", "loaded_at": "TIMESTAMP", "context_destination_id": "STRING", "context_source_id": "STRING", "event": "STRING", "user_id": "STRING", "id": "STRING", "review_body": "STRING", "product_id": "STRING", "channel": "STRING", "event_text": "STRING", "context_request_ip": "STRING", "timestamp": "TIMESTAMP", "context_sources_task_run_id": "STRING", "context_ip": "STRING", "_between": "STRING", "uuid_ts": "TIMESTAMP"},
			"rudder_identity_merge_rules": {"merge_property_1_type": "STRING", "merge_property_1_value": "STRING", "merge_property_2_type": "STRING", "merge_property_2_value": "STRING"},
			"tracks":                      {"context_sources_version": "STRING", "user_id": "STRING", "context_destination_type": "STRING", "event_text": "STRING", "context_source_type": "STRING", "event": "STRING", "id": "STRING", "loaded_at": "TIMESTAMP", "received_at": "TIMESTAMP", "timestamp": "TIMESTAMP", "channel": "STRING", "context_destination_id": "STRING", "context_ip": "STRING", "context_sources_job_run_id": "STRING", "original_timestamp": "TIMESTAMP", "context_sources_task_run_id": "STRING", "sent_at": "TIMESTAMP", "uuid_ts": "TIMESTAMP", "context_request_ip": "STRING", "context_source_id": "STRING", "context_sources_job_id": "STRING"},
		}
		userIDFormat := "userId_bq"
		userIDSQL := "SUBSTRING(user_id, 1, 9)"
		uuidTSSQL := "FORMAT_TIMESTAMP('%Y-%m-%d', uuid_ts)"

		testcase := []struct {
			name                               string
			tables                             []string
			warehouseEventsMap2                whth.EventsCountMap
			sourceJob                          bool
			stagingFilePath1, stagingFilePath2 string
			jobRunID1, taskRunID1              string
			jobRunID2, taskRunID2              string
			useSameUserID                      bool
			configOverride                     map[string]any
			preLoading                         func(testing.TB, context.Context, *bigquery.Client, string)
			postLoading                        func(testing.TB, context.Context, *bigquery.Client, string)
			verifySchema                       func(testing.TB, *bigquery.Client, string)
			verifyRecords                      func(testing.TB, *bigquery.Client, string, string, string, string, string)
		}{
			{
				name:             "Source Job",
				tables:           []string{"tracks", "google_sheet"},
				sourceJob:        true,
				jobRunID1:        misc.FastUUID().String(),
				taskRunID1:       misc.FastUUID().String(),
				jobRunID2:        misc.FastUUID().String(),
				taskRunID2:       misc.FastUUID().String(),
				stagingFilePath1: "../testdata/source-job.events-1.json",
				stagingFilePath2: "../testdata/source-job.events-2.json",
				verifySchema: func(t testing.TB, db *bigquery.Client, namespace string) {
					t.Helper()
					schema := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT t.table_name, c.column_name, c.data_type FROM %[1]s.INFORMATION_SCHEMA.TABLES as t LEFT JOIN %[1]s.INFORMATION_SCHEMA.COLUMNS as c ON (t.table_name = c.table_name) WHERE (t.table_type != 'VIEW') AND ( c.column_name != '_PARTITIONTIME' OR c.column_name IS NULL );`, namespace))
					require.Equal(t, expectedSourcesSchema, whth.ConvertRecordsToSchema(schema))
				},
				verifyRecords: func(t testing.TB, db *bigquery.Client, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					t.Helper()
					tracksRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT channel, context_sources_job_id, received_at, context_sources_version, %s, sent_at, context_ip, event, event_text, %s, context_destination_id, id, context_request_ip, context_source_type, original_timestamp, context_sources_job_run_id, context_sources_task_run_id, context_source_id, context_destination_type, timestamp FROM %s.%s ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.SourceJobTracksRecords(userIDFormat, sourceID, destinationID, destType, jobRunID, taskRunID))
					googleSheetRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT product_id, sent_at, _between, context_request_ip, context_sources_job_run_id, channel, review_body, context_source_id, original_timestamp, context_destination_id, context_sources_job_id, event, context_sources_task_run_id, context_source_type, %s, context_ip, timestamp, id, received_at, review_id, %s, context_sources_version, context_destination_type, event_text, _as, rating FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "google_sheet"))
					require.ElementsMatch(t, googleSheetRecords, whth.SourceJobGoogleSheetRecords(userIDFormat, sourceID, destinationID, destType, jobRunID, taskRunID))
				},
			},
			{
				name:   "Append mode",
				tables: []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "_groups"},
				warehouseEventsMap2: whth.EventsCountMap{
					// For all tables we will be appending because of preferAppend config
					"identifies": 8, "users": 2, "tracks": 8, "product_track": 8, "pages": 8, "screens": 8, "aliases": 8, "_groups": 8,
				},
				stagingFilePath1: "../testdata/upload-job.events-1.json",
				stagingFilePath2: "../testdata/upload-job.events-1.json",
				useSameUserID:    true,
				postLoading: func(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string) {
					t.Helper()

					checkTables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "_groups"}

					tables := listTables(t, ctx, db, namespace)
					filteredTables := lo.Filter(tables, func(table *bigquery.TableMetadata, _ int) bool {
						return lo.Contains(checkTables, table.Name)
					})
					for _, table := range filteredTables {
						require.NotNil(t, table.TimePartitioning)
						require.Empty(t, table.TimePartitioning.Field) // If empty, the table is partitioned by pseudo column '_PARTITIONTIME'
						require.Equal(t, bigquery.DayPartitioningType, table.TimePartitioning.Type)
					}

					verifyEventsUsingView(t, ctx, db, namespace, whth.EventsCountMap{
						"identifies": 4, "users": 1, "tracks": 4, "product_track": 4, "pages": 4, "screens": 4, "aliases": 4, "_groups": 4,
					})
				},
				verifySchema: func(t testing.TB, db *bigquery.Client, namespace string) {
					t.Helper()
					schema := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT t.table_name, c.column_name, c.data_type FROM %[1]s.INFORMATION_SCHEMA.TABLES as t LEFT JOIN %[1]s.INFORMATION_SCHEMA.COLUMNS as c ON (t.table_name = c.table_name) WHERE (t.table_type != 'VIEW') AND ( c.column_name != '_PARTITIONTIME' OR c.column_name IS NULL );`, namespace))
					require.Equal(t, expectedUploadJobSchema, whth.ConvertRecordsToSchema(schema))
				},
				verifyRecords: func(t testing.TB, db *bigquery.Client, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					t.Helper()
					identifiesRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, SUBSTRING(id, 1, 9), %s, received_at, name, original_timestamp FROM %s.%s ORDER BY id;`, uuidTSSQL, namespace, "users"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersAppendRecordsUsingUsersLoadFiles(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %s.%s ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksAppendRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "product_track"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackAppendRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "pages"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, _as FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "screens"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensAppendRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "aliases"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %s.%s ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "_groups"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsAppendRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
			{
				name:   "Append mode with default config (partitionColumn: _PARTITIONTIME, partitionType: day)",
				tables: []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "_groups"},
				warehouseEventsMap2: whth.EventsCountMap{
					// For all tables we will be appending because of preferAppend config
					"identifies": 8, "users": 2, "tracks": 8, "product_track": 8, "pages": 8, "screens": 8, "aliases": 8, "_groups": 8,
				},
				stagingFilePath1: "../testdata/upload-job.events-1.json",
				stagingFilePath2: "../testdata/upload-job.events-1.json",
				useSameUserID:    true,
				postLoading: func(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string) {
					t.Helper()

					checkTables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "_groups"}

					tables := listTables(t, ctx, db, namespace)
					filteredTables := lo.Filter(tables, func(table *bigquery.TableMetadata, _ int) bool {
						return lo.Contains(checkTables, table.Name)
					})
					for _, table := range filteredTables {
						require.NotNil(t, table.TimePartitioning)
						require.Empty(t, table.TimePartitioning.Field) // If empty, the table is partitioned by pseudo column '_PARTITIONTIME'
						require.Equal(t, bigquery.DayPartitioningType, table.TimePartitioning.Type)
					}

					verifyEventsUsingView(t, ctx, db, namespace, whth.EventsCountMap{
						"identifies": 4, "users": 1, "tracks": 4, "product_track": 4, "pages": 4, "screens": 4, "aliases": 4, "_groups": 4,
					})
				},
				configOverride: map[string]any{
					"partitionColumn": "_PARTITIONTIME",
					"partitionType":   "day",
				},
				verifySchema: func(t testing.TB, db *bigquery.Client, namespace string) {
					t.Helper()
					schema := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT t.table_name, c.column_name, c.data_type FROM %[1]s.INFORMATION_SCHEMA.TABLES as t LEFT JOIN %[1]s.INFORMATION_SCHEMA.COLUMNS as c ON (t.table_name = c.table_name) WHERE (t.table_type != 'VIEW') AND ( c.column_name != '_PARTITIONTIME' OR c.column_name IS NULL );`, namespace))
					require.Equal(t, expectedUploadJobSchema, whth.ConvertRecordsToSchema(schema))
				},
				verifyRecords: func(t testing.TB, db *bigquery.Client, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					t.Helper()
					identifiesRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, SUBSTRING(id, 1, 9), %s, received_at, name, original_timestamp FROM %s.%s ORDER BY id;`, uuidTSSQL, namespace, "users"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersAppendRecordsUsingUsersLoadFiles(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %s.%s ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksAppendRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "product_track"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackAppendRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "pages"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, _as FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "screens"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensAppendRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "aliases"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %s.%s ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "_groups"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsAppendRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
			{
				name:   "Append mode with ingestion-time hour partitioning (partitionColumn: _PARTITIONTIME, partitionType: hour)",
				tables: []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "_groups"},
				warehouseEventsMap2: whth.EventsCountMap{
					// For all tables we will be appending because of preferAppend config
					"identifies": 8, "users": 2, "tracks": 8, "product_track": 8, "pages": 8, "screens": 8, "aliases": 8, "_groups": 8,
				},
				stagingFilePath1: "../testdata/upload-job.events-1.json",
				stagingFilePath2: "../testdata/upload-job.events-1.json",
				useSameUserID:    true,
				postLoading: func(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string) {
					t.Helper()

					checkTables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "_groups"}

					tables := listTables(t, ctx, db, namespace)
					filteredTables := lo.Filter(tables, func(table *bigquery.TableMetadata, _ int) bool {
						return lo.Contains(checkTables, table.Name)
					})
					for _, table := range filteredTables {
						require.NotNil(t, table.TimePartitioning)
						require.Empty(t, table.TimePartitioning.Field) // If empty, the table is partitioned by pseudo column '_PARTITIONTIME'
						require.Equal(t, bigquery.HourPartitioningType, table.TimePartitioning.Type)
					}

					verifyEventsUsingView(t, ctx, db, namespace, whth.EventsCountMap{
						"identifies": 4, "users": 1, "tracks": 4, "product_track": 4, "pages": 4, "screens": 4, "aliases": 4, "_groups": 4,
					})
				},
				configOverride: map[string]any{
					"partitionColumn": "_PARTITIONTIME",
					"partitionType":   "hour",
				},
				verifySchema: func(t testing.TB, db *bigquery.Client, namespace string) {
					t.Helper()
					schema := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT t.table_name, c.column_name, c.data_type FROM %[1]s.INFORMATION_SCHEMA.TABLES as t LEFT JOIN %[1]s.INFORMATION_SCHEMA.COLUMNS as c ON (t.table_name = c.table_name) WHERE (t.table_type != 'VIEW') AND ( c.column_name != '_PARTITIONTIME' OR c.column_name IS NULL );`, namespace))
					require.Equal(t, expectedUploadJobSchema, whth.ConvertRecordsToSchema(schema))
				},
				verifyRecords: func(t testing.TB, db *bigquery.Client, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					t.Helper()
					identifiesRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, SUBSTRING(id, 1, 9), %s, received_at, name, original_timestamp FROM %s.%s ORDER BY id;`, uuidTSSQL, namespace, "users"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersAppendRecordsUsingUsersLoadFiles(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %s.%s ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksAppendRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "product_track"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackAppendRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "pages"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, _as FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "screens"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensAppendRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "aliases"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %s.%s ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "_groups"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsAppendRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
			{
				name:   "Append mode (partitionColumn: timestamp, partitionType: hour)",
				tables: []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "_groups"},
				warehouseEventsMap2: whth.EventsCountMap{
					// For all tables we will be appending because of preferAppend config
					"identifies": 8, "users": 2, "tracks": 8, "product_track": 8, "pages": 8, "screens": 8, "aliases": 8, "_groups": 8,
				},
				stagingFilePath1: "../testdata/upload-job.events-1.json",
				stagingFilePath2: "../testdata/upload-job.events-1.json",
				useSameUserID:    true,
				postLoading: func(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string) {
					t.Helper()

					checkTables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "_groups"}

					tables := listTables(t, ctx, db, namespace)
					filteredTables := lo.Filter(tables, func(table *bigquery.TableMetadata, _ int) bool {
						return lo.Contains(checkTables, table.Name)
					})
					for _, table := range filteredTables {
						require.NotNil(t, table.TimePartitioning)
						require.Equal(t, "timestamp", table.TimePartitioning.Field)
						require.Equal(t, bigquery.HourPartitioningType, table.TimePartitioning.Type)
					}
					partitions := listPartitions(t, ctx, db, namespace)
					filteredPartitions := lo.Filter(partitions, func(table lo.Tuple2[string, string], _ int) bool {
						return lo.Contains(checkTables, table.A)
					})
					for _, partition := range filteredPartitions {
						require.Equal(t, partition.B, "2023051204")
					}
				},
				configOverride: map[string]any{
					"partitionColumn": "timestamp",
					"partitionType":   "hour",
				},
				verifySchema: func(t testing.TB, db *bigquery.Client, namespace string) {
					t.Helper()
					schema := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT t.table_name, c.column_name, c.data_type FROM %[1]s.INFORMATION_SCHEMA.TABLES as t LEFT JOIN %[1]s.INFORMATION_SCHEMA.COLUMNS as c ON (t.table_name = c.table_name) WHERE (t.table_type != 'VIEW') AND ( c.column_name != '_PARTITIONTIME' OR c.column_name IS NULL );`, namespace))
					require.Equal(t, expectedUploadJobSchema, whth.ConvertRecordsToSchema(schema))
				},
				verifyRecords: func(t testing.TB, db *bigquery.Client, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					t.Helper()
					identifiesRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, SUBSTRING(id, 1, 9), %s, received_at, name, original_timestamp FROM %s.%s ORDER BY id;`, uuidTSSQL, namespace, "users"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersAppendRecordsUsingUsersLoadFiles(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %s.%s ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksAppendRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "product_track"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackAppendRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "pages"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, _as FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "screens"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensAppendRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "aliases"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %s.%s ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "_groups"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsAppendRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
			{
				name:   "Append mode (partitionColumn: received_at, partitionType: hour)",
				tables: []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "_groups"},
				warehouseEventsMap2: whth.EventsCountMap{
					// For all tables we will be appending because of preferAppend config
					"identifies": 8, "users": 2, "tracks": 8, "product_track": 8, "pages": 8, "screens": 8, "aliases": 8, "_groups": 8,
				},
				stagingFilePath1: "../testdata/upload-job.events-1.json",
				stagingFilePath2: "../testdata/upload-job.events-1.json",
				useSameUserID:    true,
				postLoading: func(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string) {
					t.Helper()

					checkTables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "_groups"}

					tables := listTables(t, ctx, db, namespace)
					filteredTables := lo.Filter(tables, func(table *bigquery.TableMetadata, _ int) bool {
						return lo.Contains(checkTables, table.Name)
					})
					for _, table := range filteredTables {
						require.NotNil(t, table.TimePartitioning)
						require.Equal(t, "received_at", table.TimePartitioning.Field)
						require.Equal(t, bigquery.HourPartitioningType, table.TimePartitioning.Type)
					}
					partitions := listPartitions(t, ctx, db, namespace)
					filteredPartitions := lo.Filter(partitions, func(table lo.Tuple2[string, string], _ int) bool {
						return lo.Contains(checkTables, table.A)
					})
					for _, partition := range filteredPartitions {
						require.Equal(t, partition.B, "2023051204")
					}
				},
				configOverride: map[string]any{
					"partitionColumn": "received_at",
					"partitionType":   "hour",
				},
				verifySchema: func(t testing.TB, db *bigquery.Client, namespace string) {
					t.Helper()
					schema := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT t.table_name, c.column_name, c.data_type FROM %[1]s.INFORMATION_SCHEMA.TABLES as t LEFT JOIN %[1]s.INFORMATION_SCHEMA.COLUMNS as c ON (t.table_name = c.table_name) WHERE (t.table_type != 'VIEW') AND ( c.column_name != '_PARTITIONTIME' OR c.column_name IS NULL );`, namespace))
					require.Equal(t, expectedUploadJobSchema, whth.ConvertRecordsToSchema(schema))
				},
				verifyRecords: func(t testing.TB, db *bigquery.Client, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					t.Helper()
					identifiesRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, SUBSTRING(id, 1, 9), %s, received_at, name, original_timestamp FROM %s.%s ORDER BY id;`, uuidTSSQL, namespace, "users"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersAppendRecordsUsingUsersLoadFiles(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %s.%s ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksAppendRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "product_track"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackAppendRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "pages"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, _as FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "screens"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensAppendRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "aliases"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %s.%s ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "_groups"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsAppendRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
			{
				name:   "Append mode (partitionColumn: received_at, partitionType: day)",
				tables: []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "_groups"},
				warehouseEventsMap2: whth.EventsCountMap{
					// For all tables we will be appending because of preferAppend config
					"identifies": 8, "users": 2, "tracks": 8, "product_track": 8, "pages": 8, "screens": 8, "aliases": 8, "_groups": 8,
				},
				stagingFilePath1: "../testdata/upload-job.events-1.json",
				stagingFilePath2: "../testdata/upload-job.events-1.json",
				useSameUserID:    true,
				postLoading: func(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string) {
					t.Helper()

					checkTables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "_groups"}

					tables := listTables(t, ctx, db, namespace)
					filteredTables := lo.Filter(tables, func(table *bigquery.TableMetadata, _ int) bool {
						return lo.Contains(checkTables, table.Name)
					})
					for _, table := range filteredTables {
						require.NotNil(t, table.TimePartitioning)
						require.Equal(t, "received_at", table.TimePartitioning.Field)
						require.Equal(t, bigquery.DayPartitioningType, table.TimePartitioning.Type)
					}
					partitions := listPartitions(t, ctx, db, namespace)
					filteredPartitions := lo.Filter(partitions, func(table lo.Tuple2[string, string], _ int) bool {
						return lo.Contains(checkTables, table.A)
					})
					for _, partition := range filteredPartitions {
						require.Equal(t, partition.B, "20230512")
					}
				},
				configOverride: map[string]any{
					"partitionColumn": "received_at",
					"partitionType":   "day",
				},
				verifySchema: func(t testing.TB, db *bigquery.Client, namespace string) {
					t.Helper()
					schema := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT t.table_name, c.column_name, c.data_type FROM %[1]s.INFORMATION_SCHEMA.TABLES as t LEFT JOIN %[1]s.INFORMATION_SCHEMA.COLUMNS as c ON (t.table_name = c.table_name) WHERE (t.table_type != 'VIEW') AND ( c.column_name != '_PARTITIONTIME' OR c.column_name IS NULL );`, namespace))
					require.Equal(t, expectedUploadJobSchema, whth.ConvertRecordsToSchema(schema))
				},
				verifyRecords: func(t testing.TB, db *bigquery.Client, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					t.Helper()
					identifiesRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, SUBSTRING(id, 1, 9), %s, received_at, name, original_timestamp FROM %s.%s ORDER BY id;`, uuidTSSQL, namespace, "users"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersAppendRecordsUsingUsersLoadFiles(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %s.%s ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksAppendRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "product_track"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackAppendRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "pages"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, _as FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "screens"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensAppendRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "aliases"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %s.%s ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "_groups"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsAppendRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
			{
				name:   "Append mode with table already created (partitionColumn: received_at, partitionType: day)",
				tables: []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "_groups"},
				warehouseEventsMap2: whth.EventsCountMap{
					// For all tables we will be appending because of preferAppend config
					"identifies": 8, "users": 2, "tracks": 8, "product_track": 8, "pages": 8, "screens": 8, "aliases": 8, "_groups": 8,
				},
				stagingFilePath1: "../testdata/upload-job.events-1.json",
				stagingFilePath2: "../testdata/upload-job.events-1.json",
				useSameUserID:    true,
				preLoading: func(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string) {
					t.Helper()

					err = db.Dataset(namespace).Create(context.Background(), &bigquery.DatasetMetadata{
						Location: "US",
					})
					require.NoError(t, err)

					for _, table := range []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "_groups"} {
						err = db.Dataset(namespace).Table(table).Create(context.Background(), &bigquery.TableMetadata{
							Schema: []*bigquery.FieldSchema{
								{Name: "received_at", Type: bigquery.TimestampFieldType},
							},
							TimePartitioning: &bigquery.TimePartitioning{
								Field: "received_at",
								Type:  bigquery.DayPartitioningType,
							},
						})
						require.NoError(t, err)
					}
				},
				postLoading: func(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string) {
					t.Helper()

					checkTables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "_groups"}

					tables := listTables(t, ctx, db, namespace)
					filteredTables := lo.Filter(tables, func(table *bigquery.TableMetadata, _ int) bool {
						return lo.Contains(checkTables, table.Name)
					})
					for _, table := range filteredTables {
						require.NotNil(t, table.TimePartitioning)
						require.Equal(t, "received_at", table.TimePartitioning.Field)
						require.Equal(t, bigquery.DayPartitioningType, table.TimePartitioning.Type)
					}
					partitions := listPartitions(t, ctx, db, namespace)
					filteredPartitions := lo.Filter(partitions, func(table lo.Tuple2[string, string], _ int) bool {
						return lo.Contains(checkTables, table.A)
					})
					for _, partition := range filteredPartitions {
						require.Equal(t, partition.B, "20230512")
					}
				},
				configOverride: map[string]any{
					"partitionColumn": "received_at",
					"partitionType":   "day",
				},
				verifySchema: func(t testing.TB, db *bigquery.Client, namespace string) {
					t.Helper()
					schema := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT t.table_name, c.column_name, c.data_type FROM %[1]s.INFORMATION_SCHEMA.TABLES as t LEFT JOIN %[1]s.INFORMATION_SCHEMA.COLUMNS as c ON (t.table_name = c.table_name) WHERE (t.table_type != 'VIEW') AND ( c.column_name != '_PARTITIONTIME' OR c.column_name IS NULL );`, namespace))
					require.Equal(t, expectedUploadJobSchema, whth.ConvertRecordsToSchema(schema))
				},
				verifyRecords: func(t testing.TB, db *bigquery.Client, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					t.Helper()
					identifiesRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, SUBSTRING(id, 1, 9), %s, received_at, name, original_timestamp FROM %s.%s ORDER BY id;`, uuidTSSQL, namespace, "users"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersAppendRecordsUsingUsersLoadFiles(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %s.%s ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksAppendRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "product_track"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackAppendRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "pages"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, _as FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "screens"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensAppendRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "aliases"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := bqhelper.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %s.%s ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "_groups"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsAppendRecords(userIDFormat, sourceID, destinationID, destType))
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

				destinationBuilder := backendconfigtest.NewDestinationBuilder(destType).
					WithID(destinationID).
					WithRevisionID(destinationID).
					WithConfigOption("project", credentials.ProjectID).
					WithConfigOption("location", credentials.Location).
					WithConfigOption("bucketName", credentials.BucketName).
					WithConfigOption("credentials", credentials.Credentials).
					WithConfigOption("namespace", namespace).
					WithConfigOption("syncFrequency", "30").
					WithConfigOption("allowUsersContextTraits", true).
					WithConfigOption("underscoreDivideNumbers", true)
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

				t.Setenv("RSERVER_WAREHOUSE_BIGQUERY_ENABLE_DELETE_BY_JOBS", "true")
				t.Setenv("RSERVER_WAREHOUSE_BIGQUERY_MAX_PARALLEL_LOADS", "8")
				t.Setenv("RSERVER_WAREHOUSE_BIGQUERY_SLOW_QUERY_THRESHOLD", "0s")

				whth.BootstrapSvc(t, workspaceConfig, httpPort, jobsDBPort)

				ctx := context.Background()

				db, err := bigquery.NewClient(
					ctx,
					credentials.ProjectID,
					option.WithCredentialsJSON([]byte(credentials.Credentials)),
				)
				require.NoError(t, err)
				t.Cleanup(func() { _ = db.Close() })
				t.Cleanup(func() {
					dropSchema(t, db, namespace)
				})

				if tc.preLoading != nil {
					tc.preLoading(t, ctx, db, namespace)
				}

				sqlClient := &client.Client{
					BQ:   db,
					Type: client.BQClient,
				}

				conf := map[string]any{
					"bucketName":  credentials.BucketName,
					"credentials": credentials.Credentials,
				}

				t.Log("verifying test case 1")
				ts1 := whth.TestConfig{
					WriteKey:        writeKey,
					Schema:          namespace,
					Tables:          tc.tables,
					SourceID:        sourceID,
					DestinationID:   destinationID,
					SourceJob:       tc.sourceJob,
					Config:          conf,
					WorkspaceID:     workspaceID,
					DestinationType: destType,
					JobsDB:          jobsDB,
					HTTPPort:        httpPort,
					Client:          sqlClient,
					JobRunID:        tc.jobRunID1,
					TaskRunID:       tc.taskRunID1,
					EventsFilePath:  tc.stagingFilePath1,
					UserID:          whth.GetUserId(destType),
					TransformerURL:  transformerURL,
					Destination:     destination,
				}
				ts1.VerifyEvents(t)

				t.Log("verifying test case 2")
				ts2 := whth.TestConfig{
					WriteKey:           writeKey,
					Schema:             namespace,
					Tables:             tc.tables,
					SourceID:           sourceID,
					DestinationID:      destinationID,
					WarehouseEventsMap: tc.warehouseEventsMap2,
					SourceJob:          tc.sourceJob,
					Config:             conf,
					WorkspaceID:        workspaceID,
					DestinationType:    destType,
					JobsDB:             jobsDB,
					HTTPPort:           httpPort,
					Client:             sqlClient,
					JobRunID:           tc.jobRunID2,
					TaskRunID:          tc.taskRunID2,
					EventsFilePath:     tc.stagingFilePath2,
					UserID:             whth.GetUserId(destType),
					TransformerURL:     transformerURL,
					Destination:        destination,
				}
				if tc.useSameUserID {
					ts2.UserID = ts1.UserID
				}
				ts2.VerifyEvents(t)

				if tc.postLoading != nil {
					t.Log("verifying post loading")
					tc.postLoading(t, ctx, db, namespace)
				}

				t.Log("verifying schema")
				tc.verifySchema(t, db, namespace)

				t.Log("verifying records")
				tc.verifyRecords(t, db, sourceID, destinationID, namespace, ts2.JobRunID, ts2.TaskRunID)
			})
		}
	})

	t.Run("Validations", func(t *testing.T) {
		testCases := []struct {
			name           string
			configOverride map[string]any
		}{
			{
				name: "default partitionColumn and partitionType",
			},
			{
				name: "partitionColumn: _PARTITIONTIME, partitionType: day",
				configOverride: map[string]any{
					"partitionColumn": "_PARTITIONTIME",
					"partitionType":   "day",
				},
			},
			{
				name: "partitionColumn: _PARTITIONTIME, partitionType: hour",
				configOverride: map[string]any{
					"partitionColumn": "_PARTITIONTIME",
					"partitionType":   "hour",
				},
			},
			{
				name: "partitionColumn: received_at, partitionType: hour",
				configOverride: map[string]any{
					"partitionColumn": "received_at",
					"partitionType":   "hour",
				},
			},
			{
				name: "partitionColumn: received_at, partitionType: day",
				configOverride: map[string]any{
					"partitionColumn": "loaded_at",
					"partitionType":   "day",
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				namespace := whth.RandSchema(destType)

				db, err := bigquery.NewClient(ctx,
					credentials.ProjectID,
					option.WithCredentialsJSON([]byte(credentials.Credentials)),
				)
				require.NoError(t, err)
				t.Cleanup(func() { _ = db.Close() })
				t.Cleanup(func() {
					dropSchema(t, db, namespace)
				})

				conf := map[string]interface{}{
					"project":       credentials.ProjectID,
					"location":      credentials.Location,
					"bucketName":    credentials.BucketName,
					"credentials":   credentials.Credentials,
					"prefix":        "",
					"namespace":     namespace,
					"syncFrequency": "30",
				}
				for k, v := range tc.configOverride {
					conf[k] = v
				}

				dest := backendconfig.DestinationT{
					ID:     "test_destination_id",
					Config: conf,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "1UmeD7xhVGHsPDEHoCiSPEGytS3",
						Name:        "BQ",
						DisplayName: "BigQuery",
					},
					Name:       "bigquery-integration",
					Enabled:    true,
					RevisionID: "test_destination_id",
				}
				whth.VerifyConfigurationTest(t, dest)
			})
		}
	})

	t.Run("Load Table", func(t *testing.T) {
		ctx := context.Background()
		namespace := whth.RandSchema(destType)

		db, err := bigquery.NewClient(ctx,
			credentials.ProjectID,
			option.WithCredentialsJSON([]byte(credentials.Credentials)),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		t.Cleanup(func() {
			dropSchema(t, db, namespace)
		})

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
					"project":     credentials.ProjectID,
					"location":    credentials.Location,
					"bucketName":  credentials.BucketName,
					"credentials": credentials.Credentials,
					"namespace":   namespace,
				},
			},
			WorkspaceID: "test_workspace_id",
			Namespace:   namespace,
		}

		fm, err := filemanager.New(&filemanager.Settings{
			Provider: whutils.GCS,
			Config: map[string]any{
				"project":     credentials.ProjectID,
				"location":    credentials.Location,
				"bucketName":  credentials.BucketName,
				"credentials": credentials.Credentials,
			},
		})
		require.NoError(t, err)

		t.Run("schema does not exist", func(t *testing.T) {
			tableName := "schema_not_exists_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.json.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("table does not exist", func(t *testing.T) {
			tableName := "table_not_exists_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.json.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("append", func(t *testing.T) {
			tableName := "append_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.json.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			err = bq.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			loadTableStat, err = bq.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := bqhelper.RetrieveRecordsFromWarehouse(t, db,
				fmt.Sprintf(`
					SELECT
					  id,
					  received_at,
					  test_bool,
					  test_datetime,
					  test_float,
					  test_int,
					  test_string
					FROM %s.%s
					WHERE _PARTITIONTIME BETWEEN TIMESTAMP('%s') AND TIMESTAMP('%s')
					ORDER BY id;`,
					namespace,
					tableName,
					time.Now().Add(-24*time.Hour).Format("2006-01-02"),
					time.Now().Add(+24*time.Hour).Format("2006-01-02"),
				),
			)
			require.Equal(t, records, whth.AppendTestRecords())
		})
		t.Run("load file does not exists", func(t *testing.T) {
			tableName := "load_file_not_exists_test_table"

			loadFiles := []whutils.LoadFile{{
				Location: "https://storage.googleapis.com/project/rudder-warehouse-load-objects/load_file_not_exists_test_table/test_source_id/2e04b6bd-8007-461e-a338-91224a8b7d3d-load_file_not_exists_test_table/load.json.gz",
			}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			err = bq.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("mismatch in number of columns", func(t *testing.T) {
			tableName := "mismatch_columns_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/mismatch-columns.json.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			err = bq.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("mismatch in schema", func(t *testing.T) {
			tableName := "mismatch_schema_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/mismatch-schema.json.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			err = bq.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("discards", func(t *testing.T) {
			tableName := whutils.DiscardsTable

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/discards.json.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, whutils.DiscardsSchema, whutils.DiscardsSchema)

			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			err = bq.CreateTable(ctx, tableName, whutils.DiscardsSchema)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(6))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := bqhelper.RetrieveRecordsFromWarehouse(t, db,
				fmt.Sprintf(
					`SELECT
						column_name,
						column_value,
						reason,
						received_at,
						row_id,
						table_name,
						uuid_ts
					FROM %s
					ORDER BY row_id ASC;`,
					fmt.Sprintf("`%s`.`%s`", namespace, tableName),
				),
			)
			require.Equal(t, records, whth.DiscardTestRecords())
		})
		t.Run("custom partition", func(t *testing.T) {
			tableName := "partition_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.json.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(
				t, loadFiles, tableName, schemaInUpload,
				schemaInWarehouse,
			)

			c := config.New()
			c.Set("Warehouse.bigquery.customPartitionsEnabled", true)
			c.Set("Warehouse.bigquery.customPartitionsEnabledWorkspaceIDs", []string{"test_workspace_id"})

			bq := whbigquery.New(c, logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			err = bq.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := bqhelper.RetrieveRecordsFromWarehouse(t, db,
				fmt.Sprintf(
					`SELECT
						id,
						received_at,
						test_bool,
						test_datetime,
						test_float,
						test_int,
						test_string
					FROM %s.%s
					WHERE _PARTITIONTIME BETWEEN TIMESTAMP('%s') AND TIMESTAMP('%s')
					ORDER BY id;`,
					namespace,
					tableName,
					time.Now().Add(-24*time.Hour).Format("2006-01-02"),
					time.Now().Add(+24*time.Hour).Format("2006-01-02"),
				),
			)
			require.Equal(t, records, whth.SampleTestRecords())
		})
		t.Run("multiple files", func(t *testing.T) {
			testCases := []struct {
				name             string
				loadByFolderPath bool
			}{
				{name: "loadByFolderPath = false", loadByFolderPath: false},
				{name: "loadByFolderPath = true", loadByFolderPath: true},
			}
			for i, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					tableName := "multiple_files_test_table" + strconv.Itoa(i)
					repeat := 10
					loadObjectFolder := "rudder-warehouse-load-objects"
					sourceID := "test_source_id"

					prefixes := []string{loadObjectFolder, tableName, sourceID, uuid.New().String() + "-" + tableName}

					loadFiles := lo.RepeatBy(repeat, func(int) whutils.LoadFile {
						sourceFile, err := os.Open("../testdata/load.json.gz")
						require.NoError(t, err)
						defer func() { _ = sourceFile.Close() }()

						tempFile, err := os.CreateTemp("", "clone_*.json.gz")
						require.NoError(t, err)
						defer func() { _ = tempFile.Close() }()

						_, err = io.Copy(tempFile, sourceFile)
						require.NoError(t, err)

						f, err := os.Open(tempFile.Name())
						require.NoError(t, err)
						defer func() { _ = f.Close() }()

						uploadOutput, err := fm.Upload(context.Background(), f, prefixes...)
						require.NoError(t, err)
						return whutils.LoadFile{Location: uploadOutput.Location}
					})
					mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)
					if tc.loadByFolderPath {
						mockUploader.EXPECT().GetSampleLoadFileLocation(gomock.Any(), tableName).Return(loadFiles[0].Location, nil).Times(1)
					} else {
						mockUploader.EXPECT().GetSampleLoadFileLocation(gomock.Any(), tableName).Times(0)
					}

					c := config.New()
					c.Set("Warehouse.bigquery.loadByFolderPath", tc.loadByFolderPath)

					bq := whbigquery.New(c, logger.NOP)
					require.NoError(t, bq.Setup(ctx, warehouse, mockUploader))
					require.NoError(t, bq.CreateSchema(ctx))
					require.NoError(t, bq.CreateTable(ctx, tableName, schemaInWarehouse))

					loadTableStat, err := bq.LoadTable(ctx, tableName)
					require.NoError(t, err)
					require.Equal(t, loadTableStat.RowsInserted, int64(repeat*14))
					require.Equal(t, loadTableStat.RowsUpdated, int64(0))

					records := bqhelper.RetrieveRecordsFromWarehouse(t, db,
						fmt.Sprintf(`
					SELECT
					  id,
					  received_at,
					  test_bool,
					  test_datetime,
					  test_float,
					  test_int,
					  test_string
					FROM %s.%s
					WHERE _PARTITIONTIME BETWEEN TIMESTAMP('%s') AND TIMESTAMP('%s')
					ORDER BY id;`,
							namespace,
							tableName,
							time.Now().Add(-24*time.Hour).Format("2006-01-02"),
							time.Now().Add(+24*time.Hour).Format("2006-01-02"),
						),
					)
					expectedRecords := make([][]string, 0, repeat)
					for i := 0; i < repeat; i++ {
						expectedRecords = append(expectedRecords, whth.SampleTestRecords()...)
					}
					require.ElementsMatch(t, expectedRecords, records)
				})
			}
		})
	})

	t.Run("Fetch schema", func(t *testing.T) {
		ctx := context.Background()
		namespace := whth.RandSchema(destType)

		db, err := bigquery.NewClient(ctx,
			credentials.ProjectID,
			option.WithCredentialsJSON([]byte(credentials.Credentials)),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		t.Cleanup(func() {
			dropSchema(t, db, namespace)
		})

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
					"project":     credentials.ProjectID,
					"location":    credentials.Location,
					"bucketName":  credentials.BucketName,
					"credentials": credentials.Credentials,
					"namespace":   namespace,
				},
			},
			WorkspaceID: "test_workspace_id",
			Namespace:   namespace,
		}

		t.Run("should not contain staging like schema", func(t *testing.T) {
			tableName := "test_table"
			stagingTableName := whutils.StagingTableName(destType, whutils.UsersTable, 127)

			ctrl := gomock.NewController(t)
			mockUploader := mockuploader.NewMockUploader(ctrl)

			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			for _, table := range []string{tableName, stagingTableName} {
				require.NoError(t, db.Dataset(namespace).Table(table).Create(ctx, &bigquery.TableMetadata{
					Schema: []*bigquery.FieldSchema{
						{Name: "id", Type: bigquery.StringFieldType},
					},
					TimePartitioning: &bigquery.TimePartitioning{
						Type: bigquery.DayPartitioningType,
					},
				}))
			}

			tables := listTables(t, ctx, db, namespace)
			require.Equal(t, []string{stagingTableName, tableName}, lo.Map(tables, func(item *bigquery.TableMetadata, index int) string {
				return item.Name
			}))

			warehouseSchema, err := bq.FetchSchema(ctx)
			require.NoError(t, err)

			fetchedTables := lo.Keys(warehouseSchema)
			require.Equal(t, []string{tableName}, fetchedTables)
		})
	})

	t.Run("Crash recovery", func(t *testing.T) {
		ctx := context.Background()
		namespace := whth.RandSchema(destType)

		db, err := bigquery.NewClient(ctx,
			credentials.ProjectID,
			option.WithCredentialsJSON([]byte(credentials.Credentials)),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		t.Cleanup(func() {
			dropSchema(t, db, namespace)
		})

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
					"project":     credentials.ProjectID,
					"location":    credentials.Location,
					"bucketName":  credentials.BucketName,
					"credentials": credentials.Credentials,
					"namespace":   namespace,
				},
			},
			WorkspaceID: "test_workspace_id",
			Namespace:   namespace,
		}

		t.Run("should delete staging like table", func(t *testing.T) {
			tableName := "test_table"
			stagingTableName := whutils.StagingTableName(destType, whutils.UsersTable, 127)

			ctrl := gomock.NewController(t)
			mockUploader := mockuploader.NewMockUploader(ctrl)

			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			for _, table := range []string{tableName, stagingTableName} {
				require.NoError(t, db.Dataset(namespace).Table(table).Create(ctx, &bigquery.TableMetadata{
					Schema: []*bigquery.FieldSchema{
						{Name: "id", Type: bigquery.StringFieldType},
					},
					TimePartitioning: &bigquery.TimePartitioning{
						Type: bigquery.DayPartitioningType,
					},
				}))
			}

			tables := listTables(t, ctx, db, namespace)
			require.Equal(t, []string{stagingTableName, tableName}, lo.Map(tables, func(item *bigquery.TableMetadata, index int) string {
				return item.Name
			}))

			bq.Cleanup(ctx)

			tables = listTables(t, ctx, db, namespace)
			require.Equal(t, []string{tableName}, lo.Map(tables, func(item *bigquery.TableMetadata, index int) string {
				return item.Name
			}))
		})
	})

	t.Run("IsEmpty", func(t *testing.T) {
		ctx := context.Background()
		namespace := whth.RandSchema(destType)

		db, err := bigquery.NewClient(ctx,
			credentials.ProjectID,
			option.WithCredentialsJSON([]byte(credentials.Credentials)),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		t.Cleanup(func() {
			dropSchema(t, db, namespace)
		})

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
					"project":     credentials.ProjectID,
					"location":    credentials.Location,
					"bucketName":  credentials.BucketName,
					"credentials": credentials.Credentials,
					"namespace":   namespace,
				},
			},
			WorkspaceID: "test_workspace_id",
			Namespace:   namespace,
		}

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		mockUploader := mockuploader.NewMockUploader(ctrl)
		mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()

		insertRecords := func(t testing.TB, tableName string) {
			t.Helper()

			query := db.Query(`
				INSERT INTO ` + tableName + ` (
				  id, received_at, test_bool, test_datetime,
				  test_float, test_int, test_string
				)
				VALUES
				  (
					'1', '2020-01-01 00:00:00', true,
					'2020-01-01 00:00:00', 1.1, 1, 'test'
				  );`,
			)
			job, err := query.Run(ctx)
			require.NoError(t, err)

			status, err := job.Wait(ctx)
			require.NoError(t, err)
			require.Nil(t, status.Err())
		}

		t.Run("tables doesn't exists", func(t *testing.T) {
			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			isEmpty, err := bq.IsEmpty(ctx, warehouse)
			require.NoError(t, err)
			require.True(t, isEmpty)
		})
		t.Run("tables empty", func(t *testing.T) {
			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			tables := []string{"pages", "screens"}
			for _, table := range tables {
				err = bq.CreateTable(ctx, table, model.TableSchema{
					"test_bool":     "boolean",
					"test_datetime": "datetime",
					"test_float":    "float",
					"test_int":      "int",
					"test_string":   "string",
					"id":            "string",
					"received_at":   "datetime",
				})
				require.NoError(t, err)
			}

			isEmpty, err := bq.IsEmpty(ctx, warehouse)
			require.NoError(t, err)
			require.True(t, isEmpty)
		})
		t.Run("tables not empty", func(t *testing.T) {
			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			insertRecords(t, fmt.Sprintf("`%s`.`%s`", namespace, "pages"))
			insertRecords(t, fmt.Sprintf("`%s`.`%s`", namespace, "screens"))

			isEmpty, err := bq.IsEmpty(ctx, warehouse)
			require.NoError(t, err)
			require.False(t, isEmpty)
		})
	})
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

	return lo.Filter(tables, func(item *bigquery.TableMetadata, index int) bool {
		return item.Type == "TABLE"
	})
}

func listPartitions(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string) (partitions []lo.Tuple2[string, string]) {
	t.Helper()
	t.Log("Listing partitions in namespace", namespace)

	query := fmt.Sprintf(`SELECT table_name, partition_id FROM %s.INFORMATION_SCHEMA.PARTITIONS;`,
		namespace,
	)

	it, err := db.Query(query).Read(ctx)
	require.NoError(t, err)

	for {
		var row []bigquery.Value

		if err = it.Next(&row); err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}
			require.NoError(t, err)
		}
		require.Len(t, row, 2)

		tableName, tableNameOK := row[0].(string)
		require.True(t, tableNameOK)
		partitionID, partitionIDOK := row[1].(string)
		require.True(t, partitionIDOK)

		partitions = append(partitions, lo.Tuple2[string, string]{
			A: tableName,
			B: partitionID,
		})
	}
	return
}

func verifyEventsUsingView(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string, expectedEvents whth.EventsCountMap) {
	t.Helper()
	t.Log("Verifying events in view in namespace", namespace)

	for table, count := range expectedEvents {
		view := fmt.Sprintf("%s_view", table)
		query := fmt.Sprintf(`SELECT COUNT(*) FROM %s.%s;`, namespace, view)

		t.Logf("checking view %s", view)

		it, err := db.Query(query).Read(ctx)
		require.NoError(t, err)

		var row []bigquery.Value
		err = it.Next(&row)
		require.NoError(t, err)
		require.Len(t, row, 1)
		require.EqualValues(t, count, row[0])
	}
}

func dropSchema(t testing.TB, db *bigquery.Client, namespace string) {
	t.Helper()
	t.Log("Dropping schema", namespace)

	require.Eventually(t, func() bool {
		if err := db.Dataset(namespace).DeleteWithContents(context.Background()); err != nil {
			t.Logf("error deleting dataset: %v", err)
			return false
		}
		return true
	},
		time.Minute,
		time.Second,
	)
}

func newMockUploader(
	t testing.TB,
	loadFiles []whutils.LoadFile,
	tableName string,
	schemaInUpload model.TableSchema,
	schemaInWarehouse model.TableSchema,
) *mockuploader.MockUploader {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockUploader := mockuploader.NewMockUploader(ctrl)
	mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()
	mockUploader.EXPECT().GetLoadFilesMetadata(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, options whutils.GetLoadFilesOptions) ([]whutils.LoadFile, error) {
			return slices.Clone(loadFiles), nil
		},
	).AnyTimes()
	mockUploader.EXPECT().GetTableSchemaInUpload(tableName).Return(schemaInUpload).AnyTimes()
	mockUploader.EXPECT().GetTableSchemaInWarehouse(tableName).Return(schemaInWarehouse).AnyTimes()

	return mockUploader
}
