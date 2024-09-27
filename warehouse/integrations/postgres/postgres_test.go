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
	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
	dockerpg "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/sshserver"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"
	"github.com/rudderlabs/rudder-go-kit/testhelper/keygen"
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

		c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.postgres.yml", "../testdata/docker-compose.jobsdb.yml", "../testdata/docker-compose.minio.yml", "../testdata/docker-compose.transformer.yml"}))
		c.Start(context.Background())

		workspaceID := whutils.RandHex()
		jobsDBPort := c.Port("jobsDb", 5432)
		postgresPort := c.Port("postgres", 5432)
		minioEndpoint := fmt.Sprintf("localhost:%d", c.Port("minio", 9000))
		transformerURL := fmt.Sprintf("http://localhost:%d", c.Port("transformer", 9090))

		jobsDB := whth.JobsDB(t, jobsDBPort)

		expectedUploadJobSchema := model.Schema{
			"screens":       {"context_source_id": "text", "user_id": "text", "sent_at": "timestamp with time zone", "context_request_ip": "text", "original_timestamp": "timestamp with time zone", "url": "text", "context_source_type": "text", "_between": "text", "timestamp": "timestamp with time zone", "context_ip": "text", "context_destination_type": "text", "received_at": "timestamp with time zone", "title": "text", "uuid_ts": "timestamp with time zone", "context_destination_id": "text", "name": "text", "id": "text", "_as": "text"},
			"identifies":    {"context_ip": "text", "context_destination_id": "text", "email": "text", "context_request_ip": "text", "sent_at": "timestamp with time zone", "uuid_ts": "timestamp with time zone", "_as": "text", "logins": "bigint", "context_source_type": "text", "context_traits_logins": "bigint", "name": "text", "context_destination_type": "text", "_between": "text", "id": "text", "timestamp": "timestamp with time zone", "received_at": "timestamp with time zone", "user_id": "text", "context_traits_email": "text", "context_traits_as": "text", "context_traits_name": "text", "original_timestamp": "timestamp with time zone", "context_traits_between": "text", "context_source_id": "text"},
			"users":         {"context_traits_name": "text", "context_traits_between": "text", "context_request_ip": "text", "context_traits_logins": "bigint", "context_destination_id": "text", "email": "text", "logins": "bigint", "_as": "text", "context_source_id": "text", "uuid_ts": "timestamp with time zone", "context_source_type": "text", "context_traits_email": "text", "name": "text", "id": "text", "_between": "text", "context_ip": "text", "received_at": "timestamp with time zone", "sent_at": "timestamp with time zone", "context_traits_as": "text", "context_destination_type": "text", "timestamp": "timestamp with time zone", "original_timestamp": "timestamp with time zone"},
			"product_track": {"review_id": "text", "context_source_id": "text", "user_id": "text", "timestamp": "timestamp with time zone", "uuid_ts": "timestamp with time zone", "review_body": "text", "context_source_type": "text", "_as": "text", "_between": "text", "id": "text", "rating": "bigint", "event": "text", "original_timestamp": "timestamp with time zone", "context_destination_type": "text", "context_ip": "text", "context_destination_id": "text", "sent_at": "timestamp with time zone", "received_at": "timestamp with time zone", "event_text": "text", "product_id": "text", "context_request_ip": "text"},
			"tracks":        {"original_timestamp": "timestamp with time zone", "context_destination_id": "text", "event": "text", "context_request_ip": "text", "uuid_ts": "timestamp with time zone", "context_destination_type": "text", "user_id": "text", "sent_at": "timestamp with time zone", "context_source_type": "text", "context_ip": "text", "timestamp": "timestamp with time zone", "received_at": "timestamp with time zone", "context_source_id": "text", "event_text": "text", "id": "text"},
			"aliases":       {"context_request_ip": "text", "context_destination_type": "text", "context_destination_id": "text", "previous_id": "text", "context_ip": "text", "sent_at": "timestamp with time zone", "id": "text", "uuid_ts": "timestamp with time zone", "timestamp": "timestamp with time zone", "original_timestamp": "timestamp with time zone", "context_source_id": "text", "user_id": "text", "context_source_type": "text", "received_at": "timestamp with time zone"},
			"pages":         {"name": "text", "url": "text", "id": "text", "timestamp": "timestamp with time zone", "title": "text", "user_id": "text", "context_source_id": "text", "context_source_type": "text", "original_timestamp": "timestamp with time zone", "context_request_ip": "text", "received_at": "timestamp with time zone", "_between": "text", "context_destination_type": "text", "uuid_ts": "timestamp with time zone", "context_destination_id": "text", "sent_at": "timestamp with time zone", "context_ip": "text", "_as": "text"},
			"groups":        {"_as": "text", "user_id": "text", "context_destination_type": "text", "sent_at": "timestamp with time zone", "context_source_type": "text", "received_at": "timestamp with time zone", "context_ip": "text", "industry": "text", "timestamp": "timestamp with time zone", "group_id": "text", "uuid_ts": "timestamp with time zone", "context_source_id": "text", "context_request_ip": "text", "_between": "text", "original_timestamp": "timestamp with time zone", "name": "text", "plan": "text", "context_destination_id": "text", "employees": "bigint", "id": "text"},
		}
		expectedSourceJobSchema := model.Schema{
			"google_sheet": {"_as": "text", "review_body": "text", "rating": "bigint", "context_source_type": "text", "_between": "text", "context_destination_id": "text", "review_id": "text", "context_sources_version": "text", "context_destination_type": "text", "id": "text", "user_id": "text", "context_request_ip": "text", "original_timestamp": "timestamp with time zone", "received_at": "timestamp with time zone", "product_id": "text", "context_sources_task_run_id": "text", "event": "text", "context_source_id": "text", "sent_at": "timestamp with time zone", "uuid_ts": "timestamp with time zone", "timestamp": "timestamp with time zone", "context_sources_job_run_id": "text", "context_ip": "text", "context_sources_job_id": "text", "channel": "text", "event_text": "text"},
			"tracks":       {"original_timestamp": "timestamp with time zone", "sent_at": "timestamp with time zone", "timestamp": "timestamp with time zone", "context_source_id": "text", "context_ip": "text", "context_destination_type": "text", "uuid_ts": "timestamp with time zone", "event_text": "text", "context_request_ip": "text", "context_sources_job_id": "text", "context_sources_version": "text", "context_sources_task_run_id": "text", "id": "text", "channel": "text", "received_at": "timestamp with time zone", "context_destination_id": "text", "context_source_type": "text", "user_id": "text", "context_sources_job_run_id": "text", "event": "text"},
		}
		userIDFormat := "userId_postgres"
		userIDSQL := "SUBSTRING(user_id from 1 for 15)"
		uuidTSSQL := "TO_CHAR(uuid_ts, 'YYYY-MM-DD')"

		testCases := []struct {
			name                           string
			tables                         []string
			warehouseEventsMap2            whth.EventsCountMap
			sourceJob                      bool
			eventFilePath1, eventFilePath2 string
			jobRunID1, taskRunID1          string
			jobRunID2, taskRunID2          string
			useSameUserID                  bool
			additionalEnvs                 func(destinationID string) map[string]string
			configOverride                 map[string]any
			verifySchema                   func(t *testing.T, db *sql.DB, namespace string)
			verifyRecords                  func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace, jobRunID, taskRunID string)
		}{
			{
				name:           "Upload Job",
				tables:         []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				eventFilePath1: "../testdata/upload-job.events-1.json",
				eventFilePath2: "../testdata/upload-job.events-2.json",
				verifySchema: func(t *testing.T, db *sql.DB, namespace string) {
					t.Helper()
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
					require.Equal(t, expectedUploadJobSchema, whth.ConvertRecordsToSchema(schema))
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					t.Helper()
					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, substring(id from 1 for 15), %s, received_at, name, original_timestamp FROM %q.%q ORDER BY id;`, uuidTSSQL, namespace, "users"))
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
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "groups"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
			{
				name:   "Append Mode",
				tables: []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				warehouseEventsMap2: whth.EventsCountMap{
					// For all tables except users we will be appending because of:
					// * preferAppend
					// For users table we will not be appending since the following config are not set
					// * Warehouse.postgres.skipDedupDestinationIDs
					// * Warehouse.postgres.skipComputingUserLatestTraits
					"identifies": 8, "users": 1, "tracks": 8, "product_track": 8, "pages": 8, "screens": 8, "aliases": 8, "groups": 8,
				},
				configOverride: map[string]any{
					"preferAppend": true,
				},
				eventFilePath1: "../testdata/upload-job.events-1.json",
				eventFilePath2: "../testdata/upload-job.events-1.json",
				useSameUserID:  true,
				verifySchema: func(t *testing.T, db *sql.DB, namespace string) {
					t.Helper()
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
					require.Equal(t, expectedUploadJobSchema, whth.ConvertRecordsToSchema(schema))
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					t.Helper()
					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, substring(id from 1 for 15), %s, received_at, name, original_timestamp FROM %q.%q ORDER BY id;`, uuidTSSQL, namespace, "users"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersMergeRecord(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksAppendRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "product_track"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackAppendRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "pages"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, _as FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "screens"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensAppendRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "aliases"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "groups"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsAppendRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
			{
				name:           "Undefined preferAppend",
				tables:         []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				eventFilePath1: "../testdata/upload-job.events-1.json",
				eventFilePath2: "../testdata/upload-job.events-1.json",
				useSameUserID:  true,
				verifySchema: func(t *testing.T, db *sql.DB, namespace string) {
					t.Helper()
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
					require.Equal(t, expectedUploadJobSchema, whth.ConvertRecordsToSchema(schema))
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					t.Helper()
					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesMergeRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, substring(id from 1 for 15), %s, received_at, name, original_timestamp FROM %q.%q ORDER BY id;`, uuidTSSQL, namespace, "users"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersMergeRecord(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksMergeRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "product_track"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackMergeRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "pages"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesMergeRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, _as FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "screens"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensMergeRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "aliases"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesMergeRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "groups"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsMergeRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
			{
				name:   "Append Users",
				tables: []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				warehouseEventsMap2: whth.EventsCountMap{
					// For all tables except users we will be appending because of:
					// * preferAppend
					// * Warehouse.postgres.skipComputingUserLatestTraits
					// For users table we will be appending because of:
					// * Warehouse.postgres.skipDedupDestinationIDs
					// * Warehouse.postgres.skipComputingUserLatestTraits
					"identifies": 8, "users": 2, "tracks": 8, "product_track": 8, "pages": 8, "screens": 8, "aliases": 8, "groups": 8,
				},
				configOverride: map[string]any{
					"preferAppend": true,
				},
				eventFilePath1: "../testdata/upload-job.events-1.json",
				eventFilePath2: "../testdata/upload-job.events-1.json",
				useSameUserID:  true,
				additionalEnvs: func(destinationID string) map[string]string {
					return map[string]string{
						"RSERVER_WAREHOUSE_POSTGRES_SKIP_DEDUP_DESTINATION_IDS":        destinationID,
						"RSERVER_WAREHOUSE_POSTGRES_SKIP_COMPUTING_USER_LATEST_TRAITS": "true",
					}
				},
				verifySchema: func(t *testing.T, db *sql.DB, namespace string) {
					t.Helper()
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
					require.Equal(t, expectedUploadJobSchema, whth.ConvertRecordsToSchema(schema))
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					t.Helper()
					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, substring(id from 1 for 15), %s, received_at, name, original_timestamp FROM %q.%q ORDER BY id;`, uuidTSSQL, namespace, "users"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersAppendRecordsUsingUsersLoadFiles(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksAppendRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "product_track"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackAppendRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "pages"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, _as FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "screens"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensAppendRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "aliases"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "groups"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsAppendRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
			{
				name:           "Source Job",
				tables:         []string{"tracks", "google_sheet"},
				sourceJob:      true,
				eventFilePath1: "../testdata/source-job.events-1.json",
				eventFilePath2: "../testdata/source-job.events-2.json",
				jobRunID1:      misc.FastUUID().String(),
				taskRunID1:     misc.FastUUID().String(),
				jobRunID2:      misc.FastUUID().String(),
				taskRunID2:     misc.FastUUID().String(),
				verifySchema: func(t *testing.T, db *sql.DB, namespace string) {
					t.Helper()
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
					require.Equal(t, expectedSourceJobSchema, whth.ConvertRecordsToSchema(schema))
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					t.Helper()
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT channel, context_sources_job_id, received_at, context_sources_version, %s, sent_at, context_ip, event, event_text, %s, context_destination_id, id, context_request_ip, context_source_type, original_timestamp, context_sources_job_run_id, context_sources_task_run_id, context_source_id, context_destination_type, timestamp FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.SourceJobTracksRecords(userIDFormat, sourceID, destinationID, destType, jobRunID, taskRunID))
					googleSheetRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT product_id, sent_at, _between, context_request_ip, context_sources_job_run_id, channel, review_body, context_source_id, original_timestamp, context_destination_id, context_sources_job_id, event, context_sources_task_run_id, context_source_type, %s, context_ip, timestamp, id, received_at, review_id, %s, context_sources_version, context_destination_type, event_text, _as, rating FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "google_sheet"))
					require.ElementsMatch(t, googleSheetRecords, whth.SourceJobGoogleSheetRecords(userIDFormat, sourceID, destinationID, destType, jobRunID, taskRunID))
				},
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

				t.Setenv("RSERVER_WAREHOUSE_POSTGRES_MAX_PARALLEL_LOADS", "8")
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
					SourceJob:       tc.sourceJob,
					Client:          sqlClient,
					JobRunID:        tc.jobRunID1,
					TaskRunID:       tc.taskRunID1,
					EventsFilePath:  tc.eventFilePath1,
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
					EventsFilePath:     tc.eventFilePath2,
					UserID:             whth.GetUserId(destType),
					TransformerURL:     transformerURL,
					Destination:        destination,
				}
				if tc.useSameUserID {
					ts2.UserID = ts1.UserID
				}
				ts2.VerifyEvents(t)

				t.Log("verifying schema")
				tc.verifySchema(t, db, namespace)

				t.Log("verifying records")
				tc.verifyRecords(t, db, sourceID, destinationID, namespace, ts2.JobRunID, ts2.TaskRunID)
			})
		}
	})

	t.Run("Events flow with SSH Tunnel", func(t *testing.T) {
		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		// Start shared Docker network
		network, err := pool.Client.CreateNetwork(dc.CreateNetworkOptions{Name: "event_flows_with_tunnel_network"})
		require.NoError(t, err)
		t.Cleanup(func() {
			if err := pool.Client.RemoveNetwork(network.ID); err != nil {
				t.Logf("Error while removing Docker network: %v", err)
			}
		})

		privateKeyPath, publicKeyPath, err := keygen.NewRSAKeyPair(2048, keygen.SaveTo(t.TempDir()))
		require.NoError(t, err)

		var (
			group               errgroup.Group
			postgresResource    *dockerpg.Resource
			sshServerResource   *sshserver.Resource
			minioResource       *minio.Resource
			transformerResource *transformer.Resource
		)
		group.Go(func() (err error) {
			postgresResource, err = dockerpg.Setup(pool, t, dockerpg.WithNetwork(network))
			return err
		})
		group.Go(func() (err error) {
			sshServerResource, err = sshserver.Setup(pool, t,
				sshserver.WithPublicKeyPath(publicKeyPath),
				sshserver.WithCredentials("linuxserver.io", ""),
				sshserver.WithDockerNetwork(network),
			)
			return err
		})
		group.Go(func() (err error) {
			minioResource, err = minio.Setup(pool, t, minio.WithNetwork(network))
			return err
		})
		group.Go(func() (err error) {
			transformerResource, err = transformer.Setup(pool, t, transformer.WithDockerNetwork(network))
			return err
		})
		require.NoError(t, group.Wait())

		httpPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		workspaceID := whutils.RandHex()
		jobsDBPort, err := strconv.Atoi(postgresResource.Port)
		require.NoError(t, err)
		sshPort := sshServerResource.Port
		minioEndpoint := minioResource.Endpoint
		postgresContainer, err := pool.Client.InspectContainer(postgresResource.ContainerID)
		require.NoError(t, err)

		transformerURL := transformerResource.TransformerURL
		tunnelledHost := postgresContainer.NetworkSettings.Networks[network.Name].IPAddress
		tunnelledDatabase := "jobsdb"
		tunnelledUser := "rudder"
		tunnelledPassword := "password"
		tunnelledPort := "5432"
		tunnelledSSHUser := "linuxserver.io"
		tunnelledSSHHost := "localhost"
		tunnelledPrivateKey, err := os.ReadFile(privateKeyPath)
		require.NoError(t, err)

		jobsDB := whth.JobsDB(t, jobsDBPort)

		expectedUploadJobSchema := model.Schema{
			"screens":       {"context_source_id": "text", "user_id": "text", "sent_at": "timestamp with time zone", "context_request_ip": "text", "original_timestamp": "timestamp with time zone", "url": "text", "context_source_type": "text", "_between": "text", "timestamp": "timestamp with time zone", "context_ip": "text", "context_destination_type": "text", "received_at": "timestamp with time zone", "title": "text", "uuid_ts": "timestamp with time zone", "context_destination_id": "text", "name": "text", "id": "text", "_as": "text"},
			"identifies":    {"context_ip": "text", "context_destination_id": "text", "email": "text", "context_request_ip": "text", "sent_at": "timestamp with time zone", "uuid_ts": "timestamp with time zone", "_as": "text", "logins": "bigint", "context_source_type": "text", "context_traits_logins": "bigint", "name": "text", "context_destination_type": "text", "_between": "text", "id": "text", "timestamp": "timestamp with time zone", "received_at": "timestamp with time zone", "user_id": "text", "context_traits_email": "text", "context_traits_as": "text", "context_traits_name": "text", "original_timestamp": "timestamp with time zone", "context_traits_between": "text", "context_source_id": "text"},
			"users":         {"context_traits_name": "text", "context_traits_between": "text", "context_request_ip": "text", "context_traits_logins": "bigint", "context_destination_id": "text", "email": "text", "logins": "bigint", "_as": "text", "context_source_id": "text", "uuid_ts": "timestamp with time zone", "context_source_type": "text", "context_traits_email": "text", "name": "text", "id": "text", "_between": "text", "context_ip": "text", "received_at": "timestamp with time zone", "sent_at": "timestamp with time zone", "context_traits_as": "text", "context_destination_type": "text", "timestamp": "timestamp with time zone", "original_timestamp": "timestamp with time zone"},
			"product_track": {"review_id": "text", "context_source_id": "text", "user_id": "text", "timestamp": "timestamp with time zone", "uuid_ts": "timestamp with time zone", "review_body": "text", "context_source_type": "text", "_as": "text", "_between": "text", "id": "text", "rating": "bigint", "event": "text", "original_timestamp": "timestamp with time zone", "context_destination_type": "text", "context_ip": "text", "context_destination_id": "text", "sent_at": "timestamp with time zone", "received_at": "timestamp with time zone", "event_text": "text", "product_id": "text", "context_request_ip": "text"},
			"tracks":        {"original_timestamp": "timestamp with time zone", "context_destination_id": "text", "context_destination_type": "text", "user_id": "text", "context_source_type": "text", "timestamp": "timestamp with time zone", "id": "text", "event": "text", "sent_at": "timestamp with time zone", "context_ip": "text", "event_text": "text", "context_source_id": "text", "context_request_ip": "text", "received_at": "timestamp with time zone", "uuid_ts": "timestamp with time zone"},
			"pages":         {"user_id": "text", "context_source_id": "text", "id": "text", "title": "text", "timestamp": "timestamp with time zone", "context_source_type": "text", "_as": "text", "received_at": "timestamp with time zone", "context_destination_id": "text", "context_ip": "text", "context_destination_type": "text", "name": "text", "original_timestamp": "timestamp with time zone", "_between": "text", "context_request_ip": "text", "sent_at": "timestamp with time zone", "url": "text", "uuid_ts": "timestamp with time zone"},
			"aliases":       {"context_source_id": "text", "context_destination_id": "text", "context_ip": "text", "sent_at": "timestamp with time zone", "id": "text", "user_id": "text", "uuid_ts": "timestamp with time zone", "previous_id": "text", "original_timestamp": "timestamp with time zone", "context_source_type": "text", "received_at": "timestamp with time zone", "context_destination_type": "text", "context_request_ip": "text", "timestamp": "timestamp with time zone"},
			"groups":        {"context_destination_type": "text", "id": "text", "_between": "text", "plan": "text", "original_timestamp": "timestamp with time zone", "user_id": "text", "context_source_id": "text", "sent_at": "timestamp with time zone", "uuid_ts": "timestamp with time zone", "group_id": "text", "industry": "text", "context_request_ip": "text", "context_source_type": "text", "timestamp": "timestamp with time zone", "employees": "bigint", "_as": "text", "context_destination_id": "text", "received_at": "timestamp with time zone", "name": "text", "context_ip": "text"},
		}
		userIDFormat := "userId_postgres"
		userIDSQL := "SUBSTRING(user_id from 1 for 15)"
		uuidTSSQL := "TO_CHAR(uuid_ts, 'YYYY-MM-DD')"

		testcases := []struct {
			name                           string
			tables                         []string
			eventFilePath1, eventFilePath2 string
			verifySchema                   func(t *testing.T, db *sql.DB, namespace string)
			verifyRecords                  func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace string)
		}{
			{
				name:           "upload job through ssh tunnelling",
				tables:         []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				eventFilePath1: "../testdata/upload-job.events-1.json",
				eventFilePath2: "../testdata/upload-job.events-2.json",
				verifySchema: func(t *testing.T, db *sql.DB, namespace string) {
					t.Helper()
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
					require.Equal(t, expectedUploadJobSchema, whth.ConvertRecordsToSchema(schema))
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace string) {
					t.Helper()
					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, substring(id from 1 for 15), %s, received_at, name, original_timestamp FROM %q.%q ORDER BY id;`, uuidTSSQL, namespace, "users"))
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
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "groups"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsRecords(userIDFormat, sourceID, destinationID, destType))
				},
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

				destination := backendconfigtest.NewDestinationBuilder(destType).
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
					WithConfigOption("sshPrivateKey", strings.ReplaceAll(string(tunnelledPrivateKey), "\\n", "\n")).
					WithConfigOption("allowUsersContextTraits", true).
					WithConfigOption("underscoreDivideNumbers", true).
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

				t.Setenv("RSERVER_WAREHOUSE_POSTGRES_MAX_PARALLEL_LOADS", "8")
				t.Setenv("RSERVER_WAREHOUSE_POSTGRES_ENABLE_SQLSTATEMENT_EXECUTION_PLAN_WORKSPACE_IDS", workspaceID)
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
						"sshPrivateKey": strings.ReplaceAll(string(tunnelledPrivateKey), "\\n", "\n"),
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
					WriteKey:        writeKey,
					Schema:          namespace,
					SourceID:        sourceID,
					DestinationID:   destinationID,
					Tables:          tc.tables,
					Config:          conf,
					WorkspaceID:     workspaceID,
					DestinationType: destType,
					JobsDB:          jobsDB,
					HTTPPort:        httpPort,
					Client:          sqlClient,
					EventsFilePath:  tc.eventFilePath1,
					UserID:          whth.GetUserId(destType),
					TransformerURL:  transformerURL,
					Destination:     destination,
				}
				ts1.VerifyEvents(t)

				t.Log("verifying test case 2")
				ts2 := whth.TestConfig{
					WriteKey:        writeKey,
					Schema:          namespace,
					SourceID:        sourceID,
					DestinationID:   destinationID,
					Tables:          tc.tables,
					Config:          conf,
					WorkspaceID:     workspaceID,
					DestinationType: destType,
					JobsDB:          jobsDB,
					HTTPPort:        httpPort,
					Client:          sqlClient,
					EventsFilePath:  tc.eventFilePath2,
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
					  reason,
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
