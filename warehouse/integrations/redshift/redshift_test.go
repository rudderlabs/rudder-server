package redshift_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	"github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	th "github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/redshift"
	whth "github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

type testCredentials struct {
	Host          string `json:"host"`
	Port          string `json:"port"`
	UserName      string `json:"userName"`
	IAMUserName   string `json:"iamUserName"`
	Password      string `json:"password"`
	DbName        string `json:"dbName"`
	BucketName    string `json:"bucketName"`
	AccessKeyID   string `json:"accessKeyID"`
	AccessKey     string `json:"accessKey"`
	IAMRoleARN    string `json:"iamRoleARN"`
	ClusterID     string `json:"clusterID"`
	ClusterRegion string `json:"clusterRegion"`
}

const testKey = "REDSHIFT_INTEGRATION_TEST_CREDENTIALS"

func rsTestCredentials() (*testCredentials, error) {
	cred, exists := os.LookupEnv(testKey)
	if !exists {
		return nil, fmt.Errorf("missing redshift test credentials")
	}

	var credentials testCredentials
	err := json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal redshift test credentials: %w", err)
	}
	return &credentials, nil
}

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}
	if _, exists := os.LookupEnv(testKey); !exists {
		if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
			t.Fatalf("%s environment variable not set", testKey)
		}
		t.Skipf("Skipping %s as %s is not set", t.Name(), testKey)
	}

	misc.Init()
	validations.Init()
	whutils.Init()

	destType := whutils.RS

	credentials, err := rsTestCredentials()
	require.NoError(t, err)

	t.Run("Events flow", func(t *testing.T) {
		httpPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		c := testcompose.New(t, compose.FilePaths([]string{"../testdata/docker-compose.jobsdb.yml", "../testdata/docker-compose.transformer.yml"}))
		c.Start(context.Background())

		workspaceID := whutils.RandHex()
		jobsDBPort := c.Port("jobsDb", 5432)
		transformerURL := fmt.Sprintf("http://localhost:%d", c.Port("transformer", 9090))

		jobsDB := whth.JobsDB(t, jobsDBPort)

		expectedUploadJobSchema := [][]string{
			{"screens", "context_source_id", "character varying"},
			{"screens", "user_id", "character varying"},
			{"screens", "sent_at", "timestamp without time zone"},
			{"screens", "context_request_ip", "character varying"},
			{"screens", "original_timestamp", "timestamp without time zone"},
			{"screens", "url", "character varying"},
			{"screens", "context_source_type", "character varying"},
			{"screens", "_between", "character varying"},
			{"screens", "timestamp", "timestamp without time zone"},
			{"screens", "context_ip", "character varying"},
			{"screens", "context_destination_type", "character varying"},
			{"screens", "received_at", "timestamp without time zone"},
			{"screens", "title", "character varying"},
			{"screens", "uuid_ts", "timestamp without time zone"},
			{"screens", "context_destination_id", "character varying"},
			{"screens", "name", "character varying"},
			{"screens", "id", "character varying"},
			{"screens", "_as", "character varying"},
			{"identifies", "context_ip", "character varying"},
			{"identifies", "context_destination_id", "character varying"},
			{"identifies", "email", "character varying"},
			{"identifies", "context_request_ip", "character varying"},
			{"identifies", "sent_at", "timestamp without time zone"},
			{"identifies", "uuid_ts", "timestamp without time zone"},
			{"identifies", "_as", "character varying"},
			{"identifies", "logins", "bigint"},
			{"identifies", "context_source_type", "character varying"},
			{"identifies", "context_traits_logins", "bigint"},
			{"identifies", "name", "character varying"},
			{"identifies", "context_destination_type", "character varying"},
			{"identifies", "_between", "character varying"},
			{"identifies", "id", "character varying"},
			{"identifies", "timestamp", "timestamp without time zone"},
			{"identifies", "received_at", "timestamp without time zone"},
			{"identifies", "user_id", "character varying"},
			{"identifies", "context_traits_email", "character varying"},
			{"identifies", "context_traits_as", "character varying"},
			{"identifies", "context_traits_name", "character varying"},
			{"identifies", "original_timestamp", "timestamp without time zone"},
			{"identifies", "context_traits_between", "character varying"},
			{"identifies", "context_source_id", "character varying"},
			{"users", "context_traits_name", "character varying"},
			{"users", "context_traits_between", "character varying"},
			{"users", "context_request_ip", "character varying"},
			{"users", "context_traits_logins", "bigint"},
			{"users", "context_destination_id", "character varying"},
			{"users", "email", "character varying"},
			{"users", "logins", "bigint"},
			{"users", "_as", "character varying"},
			{"users", "context_source_id", "character varying"},
			{"users", "uuid_ts", "timestamp without time zone"},
			{"users", "context_source_type", "character varying"},
			{"users", "context_traits_email", "character varying"},
			{"users", "name", "character varying"},
			{"users", "id", "character varying"},
			{"users", "_between", "character varying"},
			{"users", "context_ip", "character varying"},
			{"users", "received_at", "timestamp without time zone"},
			{"users", "sent_at", "timestamp without time zone"},
			{"users", "context_traits_as", "character varying"},
			{"users", "context_destination_type", "character varying"},
			{"users", "timestamp", "timestamp without time zone"},
			{"users", "original_timestamp", "timestamp without time zone"},
			{"product_track", "review_id", "character varying"},
			{"product_track", "context_source_id", "character varying"},
			{"product_track", "user_id", "character varying"},
			{"product_track", "timestamp", "timestamp without time zone"},
			{"product_track", "uuid_ts", "timestamp without time zone"},
			{"product_track", "review_body", "character varying"},
			{"product_track", "context_source_type", "character varying"},
			{"product_track", "_as", "character varying"},
			{"product_track", "_between", "character varying"},
			{"product_track", "id", "character varying"},
			{"product_track", "rating", "bigint"},
			{"product_track", "event", "character varying"},
			{"product_track", "original_timestamp", "timestamp without time zone"},
			{"product_track", "context_destination_type", "character varying"},
			{"product_track", "context_ip", "character varying"},
			{"product_track", "context_destination_id", "character varying"},
			{"product_track", "sent_at", "timestamp without time zone"},
			{"product_track", "received_at", "timestamp without time zone"},
			{"product_track", "event_text", "character varying"},
			{"product_track", "product_id", "character varying"},
			{"product_track", "context_request_ip", "character varying"},
			{"tracks", "original_timestamp", "timestamp without time zone"},
			{"tracks", "context_destination_id", "character varying"},
			{"tracks", "event", "character varying"},
			{"tracks", "context_request_ip", "character varying"},
			{"tracks", "uuid_ts", "timestamp without time zone"},
			{"tracks", "context_destination_type", "character varying"},
			{"tracks", "user_id", "character varying"},
			{"tracks", "sent_at", "timestamp without time zone"},
			{"tracks", "context_source_type", "character varying"},
			{"tracks", "context_ip", "character varying"},
			{"tracks", "timestamp", "timestamp without time zone"},
			{"tracks", "received_at", "timestamp without time zone"},
			{"tracks", "context_source_id", "character varying"},
			{"tracks", "event_text", "character varying"},
			{"tracks", "id", "character varying"},
			{"aliases", "context_request_ip", "character varying"},
			{"aliases", "context_destination_type", "character varying"},
			{"aliases", "context_destination_id", "character varying"},
			{"aliases", "previous_id", "character varying"},
			{"aliases", "context_ip", "character varying"},
			{"aliases", "sent_at", "timestamp without time zone"},
			{"aliases", "id", "character varying"},
			{"aliases", "uuid_ts", "timestamp without time zone"},
			{"aliases", "timestamp", "timestamp without time zone"},
			{"aliases", "original_timestamp", "timestamp without time zone"},
			{"aliases", "context_source_id", "character varying"},
			{"aliases", "user_id", "character varying"},
			{"aliases", "context_source_type", "character varying"},
			{"aliases", "received_at", "timestamp without time zone"},
			{"pages", "name", "character varying"},
			{"pages", "url", "character varying"},
			{"pages", "id", "character varying"},
			{"pages", "timestamp", "timestamp without time zone"},
			{"pages", "title", "character varying"},
			{"pages", "user_id", "character varying"},
			{"pages", "context_source_id", "character varying"},
			{"pages", "context_source_type", "character varying"},
			{"pages", "original_timestamp", "timestamp without time zone"},
			{"pages", "context_request_ip", "character varying"},
			{"pages", "received_at", "timestamp without time zone"},
			{"pages", "_between", "character varying"},
			{"pages", "context_destination_type", "character varying"},
			{"pages", "uuid_ts", "timestamp without time zone"},
			{"pages", "context_destination_id", "character varying"},
			{"pages", "sent_at", "timestamp without time zone"},
			{"pages", "context_ip", "character varying"},
			{"pages", "_as", "character varying"},
			{"groups", "_as", "character varying"},
			{"groups", "user_id", "character varying"},
			{"groups", "context_destination_type", "character varying"},
			{"groups", "sent_at", "timestamp without time zone"},
			{"groups", "context_source_type", "character varying"},
			{"groups", "received_at", "timestamp without time zone"},
			{"groups", "context_ip", "character varying"},
			{"groups", "industry", "character varying"},
			{"groups", "timestamp", "timestamp without time zone"},
			{"groups", "group_id", "character varying"},
			{"groups", "uuid_ts", "timestamp without time zone"},
			{"groups", "context_source_id", "character varying"},
			{"groups", "context_request_ip", "character varying"},
			{"groups", "_between", "character varying"},
			{"groups", "original_timestamp", "timestamp without time zone"},
			{"groups", "name", "character varying"},
			{"groups", "plan", "character varying"},
			{"groups", "context_destination_id", "character varying"},
			{"groups", "employees", "bigint"},
			{"groups", "id", "character varying"},
		}
		expectedSourceJobSchema := [][]string{
			{"tracks", "original_timestamp", "timestamp without time zone"},
			{"tracks", "sent_at", "timestamp without time zone"},
			{"tracks", "timestamp", "timestamp without time zone"},
			{"tracks", "context_source_id", "character varying"},
			{"tracks", "context_ip", "character varying"},
			{"tracks", "context_destination_type", "character varying"},
			{"tracks", "uuid_ts", "timestamp without time zone"},
			{"tracks", "event_text", "character varying"},
			{"tracks", "context_request_ip", "character varying"},
			{"tracks", "context_sources_job_id", "character varying"},
			{"tracks", "context_sources_version", "character varying"},
			{"tracks", "context_sources_task_run_id", "character varying"},
			{"tracks", "id", "character varying"},
			{"tracks", "channel", "character varying"},
			{"tracks", "received_at", "timestamp without time zone"},
			{"tracks", "context_destination_id", "character varying"},
			{"tracks", "context_source_type", "character varying"},
			{"tracks", "user_id", "character varying"},
			{"tracks", "context_sources_job_run_id", "character varying"},
			{"tracks", "event", "character varying"},
			{"google_sheet", "_as", "character varying"},
			{"google_sheet", "review_body", "character varying"},
			{"google_sheet", "rating", "bigint"},
			{"google_sheet", "context_source_type", "character varying"},
			{"google_sheet", "_between", "character varying"},
			{"google_sheet", "context_destination_id", "character varying"},
			{"google_sheet", "review_id", "character varying"},
			{"google_sheet", "context_sources_version", "character varying"},
			{"google_sheet", "context_destination_type", "character varying"},
			{"google_sheet", "id", "character varying"},
			{"google_sheet", "user_id", "character varying"},
			{"google_sheet", "context_request_ip", "character varying"},
			{"google_sheet", "original_timestamp", "timestamp without time zone"},
			{"google_sheet", "received_at", "timestamp without time zone"},
			{"google_sheet", "product_id", "character varying"},
			{"google_sheet", "context_sources_task_run_id", "character varying"},
			{"google_sheet", "event", "character varying"},
			{"google_sheet", "context_source_id", "character varying"},
			{"google_sheet", "sent_at", "timestamp without time zone"},
			{"google_sheet", "uuid_ts", "timestamp without time zone"},
			{"google_sheet", "timestamp", "timestamp without time zone"},
			{"google_sheet", "context_sources_job_run_id", "character varying"},
			{"google_sheet", "context_ip", "character varying"},
			{"google_sheet", "context_sources_job_id", "character varying"},
			{"google_sheet", "channel", "character varying"},
			{"google_sheet", "event_text", "character varying"},
		}

		testcase := []struct {
			name                               string
			tables                             []string
			warehouseEventsMap2                whth.EventsCountMap
			sourceJob                          bool
			stagingFilePath1, stagingFilePath2 string
			jobRunID1, taskRunID1              string
			jobRunID2, taskRunID2              string
			useSameUserID                      bool
			additionalEnvs                     func(destinationID string) map[string]string
			configOverride                     map[string]any
			verifySchema                       func(t *testing.T, db *sql.DB, namespace string)
			verifyRecords                      func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace, jobRunID, taskRunID string)
		}{
			{
				name:             "Upload Job",
				tables:           []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				stagingFilePath1: "../testdata/upload-job.events-1.json",
				stagingFilePath2: "../testdata/upload-job.events-2.json",
				configOverride: map[string]any{
					"host":     credentials.Host,
					"port":     credentials.Port,
					"user":     credentials.UserName,
					"password": credentials.Password,
				},
				verifySchema: func(t *testing.T, db *sql.DB, namespace string) {
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
					require.ElementsMatch(t, schema, expectedUploadJobSchema)
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					userIDFormat := "userId_rs"
					userIDSQL := "SUBSTRING(user_id from 1 for 9)"
					uuidTSSQL := "TO_CHAR(uuid_ts, 'YYYY-MM-DD')"

					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, "timestamp", received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, "timestamp", context_destination_id, email, context_traits_as, context_source_type, substring(id from 1 for 9), %s, received_at, name, original_timestamp FROM %q.%q ORDER BY id;`, uuidTSSQL, namespace, "users"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersRecords(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, "timestamp", id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT "timestamp", %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "product_track"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, "timestamp", context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "pages"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, "timestamp", sent_at, _as FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "screens"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, "timestamp" FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "aliases"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, "timestamp", employees, _as, context_destination_id, received_at, name, context_ip FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "groups"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
			{
				name:             "IAM Upload Job",
				tables:           []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				stagingFilePath1: "../testdata/upload-job.events-1.json",
				stagingFilePath2: "../testdata/upload-job.events-2.json",
				configOverride: map[string]any{
					"useIAMForAuth":     true,
					"user":              credentials.IAMUserName,
					"iamRoleARNForAuth": credentials.IAMRoleARN,
					"clusterId":         credentials.ClusterID,
					"clusterRegion":     credentials.ClusterRegion,
				},
				verifySchema: func(t *testing.T, db *sql.DB, namespace string) {
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
					require.ElementsMatch(t, schema, expectedUploadJobSchema)
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					userIDFormat := "userId_rs"
					userIDSQL := "SUBSTRING(user_id from 1 for 9)"
					uuidTSSQL := "TO_CHAR(uuid_ts, 'YYYY-MM-DD')"

					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, "timestamp", received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, "timestamp", context_destination_id, email, context_traits_as, context_source_type, substring(id from 1 for 9), %s, received_at, name, original_timestamp FROM %q.%q ORDER BY id;`, uuidTSSQL, namespace, "users"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersRecords(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, "timestamp", id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT "timestamp", %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "product_track"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, "timestamp", context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "pages"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, "timestamp", sent_at, _as FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "screens"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, "timestamp" FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "aliases"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, "timestamp", employees, _as, context_destination_id, received_at, name, context_ip FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "groups"))
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
					// * Warehouse.rs.skipDedupDestinationIDs
					// * Warehouse.rs.skipComputingUserLatestTraits
					"identifies": 8, "users": 1, "tracks": 8, "product_track": 8, "pages": 8, "screens": 8, "aliases": 8, "groups": 8,
				},
				stagingFilePath1: "../testdata/upload-job.events-1.json",
				stagingFilePath2: "../testdata/upload-job.events-1.json",
				useSameUserID:    true,
				configOverride: map[string]any{
					"preferAppend": true,
					"host":         credentials.Host,
					"port":         credentials.Port,
					"user":         credentials.UserName,
					"password":     credentials.Password,
				},
				verifySchema: func(t *testing.T, db *sql.DB, namespace string) {
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
					require.ElementsMatch(t, schema, expectedUploadJobSchema)
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					userIDFormat := "userId_rs"
					userIDSQL := "SUBSTRING(user_id from 1 for 9)"
					uuidTSSQL := "TO_CHAR(uuid_ts, 'YYYY-MM-DD')"

					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, "timestamp", received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, "timestamp", context_destination_id, email, context_traits_as, context_source_type, substring(id from 1 for 9), %s, received_at, name, original_timestamp FROM %q.%q ORDER BY id;`, uuidTSSQL, namespace, "users"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersMergeRecord(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, "timestamp", id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksAppendRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT "timestamp", %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "product_track"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackAppendRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, "timestamp", context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "pages"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, "timestamp", sent_at, _as FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "screens"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensAppendRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, "timestamp" FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "aliases"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, "timestamp", employees, _as, context_destination_id, received_at, name, context_ip FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "groups"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsAppendRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
			{
				name:   "IAM Append Mode",
				tables: []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				warehouseEventsMap2: whth.EventsCountMap{
					// For all tables except users we will be appending because of:
					// * preferAppend
					// For users table we will not be appending since the following config are not set
					// * Warehouse.rs.skipDedupDestinationIDs
					// * Warehouse.rs.skipComputingUserLatestTraits
					"identifies": 8, "users": 1, "tracks": 8, "product_track": 8, "pages": 8, "screens": 8, "aliases": 8, "groups": 8,
				},
				stagingFilePath1: "../testdata/upload-job.events-1.json",
				stagingFilePath2: "../testdata/upload-job.events-1.json",
				useSameUserID:    true,
				configOverride: map[string]any{
					"preferAppend":      true,
					"useIAMForAuth":     true,
					"user":              credentials.IAMUserName,
					"iamRoleARNForAuth": credentials.IAMRoleARN,
					"clusterId":         credentials.ClusterID,
					"clusterRegion":     credentials.ClusterRegion,
				},
				verifySchema: func(t *testing.T, db *sql.DB, namespace string) {
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
					require.ElementsMatch(t, schema, expectedUploadJobSchema)
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					userIDFormat := "userId_rs"
					userIDSQL := "SUBSTRING(user_id from 1 for 9)"
					uuidTSSQL := "TO_CHAR(uuid_ts, 'YYYY-MM-DD')"

					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, "timestamp", received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, "timestamp", context_destination_id, email, context_traits_as, context_source_type, substring(id from 1 for 9), %s, received_at, name, original_timestamp FROM %q.%q ORDER BY id;`, uuidTSSQL, namespace, "users"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersMergeRecord(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, "timestamp", id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksAppendRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT "timestamp", %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "product_track"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackAppendRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, "timestamp", context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "pages"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, "timestamp", sent_at, _as FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "screens"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensAppendRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, "timestamp" FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "aliases"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, "timestamp", employees, _as, context_destination_id, received_at, name, context_ip FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "groups"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsAppendRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
			{
				name:             "Undefined preferAppend",
				tables:           []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				stagingFilePath1: "../testdata/upload-job.events-1.json",
				stagingFilePath2: "../testdata/upload-job.events-1.json",
				useSameUserID:    true,
				configOverride: map[string]any{
					"host":     credentials.Host,
					"port":     credentials.Port,
					"user":     credentials.UserName,
					"password": credentials.Password,
				},
				verifySchema: func(t *testing.T, db *sql.DB, namespace string) {
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
					require.ElementsMatch(t, schema, expectedUploadJobSchema)
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					userIDFormat := "userId_rs"
					userIDSQL := "SUBSTRING(user_id from 1 for 9)"
					uuidTSSQL := "TO_CHAR(uuid_ts, 'YYYY-MM-DD')"

					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, "timestamp", received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesMergeRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, "timestamp", context_destination_id, email, context_traits_as, context_source_type, substring(id from 1 for 9), %s, received_at, name, original_timestamp FROM %q.%q ORDER BY id;`, uuidTSSQL, namespace, "users"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersMergeRecord(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, "timestamp", id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksMergeRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT "timestamp", %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "product_track"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackMergeRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, "timestamp", context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "pages"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesMergeRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, "timestamp", sent_at, _as FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "screens"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensMergeRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, "timestamp" FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "aliases"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesMergeRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, "timestamp", employees, _as, context_destination_id, received_at, name, context_ip FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "groups"))
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
				stagingFilePath1: "../testdata/upload-job.events-1.json",
				stagingFilePath2: "../testdata/upload-job.events-1.json",
				useSameUserID:    true,
				additionalEnvs: func(destinationID string) map[string]string {
					return map[string]string{
						"RSERVER_WAREHOUSE_REDSHIFT_SKIP_DEDUP_DESTINATION_IDS":        destinationID,
						"RSERVER_WAREHOUSE_REDSHIFT_SKIP_COMPUTING_USER_LATEST_TRAITS": "true",
					}
				},
				configOverride: map[string]any{
					"preferAppend": true,
					"host":         credentials.Host,
					"port":         credentials.Port,
					"user":         credentials.UserName,
					"password":     credentials.Password,
				},
				verifySchema: func(t *testing.T, db *sql.DB, namespace string) {
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
					require.ElementsMatch(t, schema, expectedUploadJobSchema)
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					userIDFormat := "userId_rs"
					userIDSQL := "SUBSTRING(user_id from 1 for 9)"
					uuidTSSQL := "TO_CHAR(uuid_ts, 'YYYY-MM-DD')"

					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, "timestamp", received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, "timestamp", context_destination_id, email, context_traits_as, context_source_type, substring(id from 1 for 9), %s, received_at, name, original_timestamp FROM %q.%q ORDER BY id;`, uuidTSSQL, namespace, "users"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersAppendRecordsUsingUsersLoadFiles(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, "timestamp", id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksAppendRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT "timestamp", %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "product_track"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackAppendRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, "timestamp", context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "pages"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, "timestamp", sent_at, _as FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "screens"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensAppendRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, "timestamp" FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "aliases"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, "timestamp", employees, _as, context_destination_id, received_at, name, context_ip FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "groups"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsAppendRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
			{
				name:             "Source Job",
				tables:           []string{"tracks", "google_sheet"},
				sourceJob:        true,
				stagingFilePath1: "../testdata/source-job.events-1.json",
				stagingFilePath2: "../testdata/source-job.events-2.json",
				jobRunID1:        misc.FastUUID().String(),
				taskRunID1:       misc.FastUUID().String(),
				jobRunID2:        misc.FastUUID().String(),
				taskRunID2:       misc.FastUUID().String(),
				configOverride: map[string]any{
					"host":     credentials.Host,
					"port":     credentials.Port,
					"user":     credentials.UserName,
					"password": credentials.Password,
				},
				verifySchema: func(t *testing.T, db *sql.DB, namespace string) {
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
					require.ElementsMatch(t, schema, expectedSourceJobSchema)
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					userIDFormat := "userId_rs"
					userIDSQL := "SUBSTRING(user_id from 1 for 9)"
					uuidTSSQL := "TO_CHAR(uuid_ts, 'YYYY-MM-DD')"

					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT channel, context_sources_job_id, received_at, context_sources_version, %s, sent_at, context_ip, event, event_text, %s, context_destination_id, id, context_request_ip, context_source_type, original_timestamp, context_sources_job_run_id, context_sources_task_run_id, context_source_id, context_destination_type, "timestamp" FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.SourceJobTracksRecords(userIDFormat, sourceID, destinationID, destType, jobRunID, taskRunID))
					googleSheetRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT product_id, sent_at, _between, context_request_ip, context_sources_job_run_id, channel, review_body, context_source_id, original_timestamp, context_destination_id, context_sources_job_id, event, context_sources_task_run_id, context_source_type, %s, context_ip, "timestamp", id, received_at, review_id, %s, context_sources_version, context_destination_type, event_text, _as, rating FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "google_sheet"))
					require.ElementsMatch(t, googleSheetRecords, whth.SourceJobGoogleSheetRecords(userIDFormat, sourceID, destinationID, destType, jobRunID, taskRunID))
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
					WithConfigOption("database", credentials.DbName).
					WithConfigOption("bucketName", credentials.BucketName).
					WithConfigOption("accessKeyID", credentials.AccessKeyID).
					WithConfigOption("accessKey", credentials.AccessKey).
					WithConfigOption("namespace", namespace).
					WithConfigOption("enableSSE", false).
					WithConfigOption("useRudderStorage", false).
					WithConfigOption("syncFrequency", "30")
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

				t.Setenv("RSERVER_WAREHOUSE_REDSHIFT_MAX_PARALLEL_LOADS", "8")
				t.Setenv("RSERVER_WAREHOUSE_REDSHIFT_ENABLE_DELETE_BY_JOBS", "true")
				t.Setenv("RSERVER_WAREHOUSE_REDSHIFT_SLOW_QUERY_THRESHOLD", "0s")
				if tc.additionalEnvs != nil {
					for envKey, envValue := range tc.additionalEnvs(destinationID) {
						t.Setenv(envKey, envValue)
					}
				}

				whth.BootstrapSvc(t, workspaceConfig, httpPort, jobsDBPort)

				dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
					credentials.UserName, credentials.Password, credentials.Host, credentials.Port, credentials.DbName,
				)
				db, err := sql.Open("postgres", dsn)
				require.NoError(t, err)
				require.NoError(t, db.Ping())
				t.Cleanup(func() { _ = db.Close() })
				t.Cleanup(func() {
					dropSchema(t, db, namespace)
				})

				sqlClient := &client.Client{
					SQL:  db,
					Type: client.SQLClient,
				}

				conf := map[string]any{
					"bucketName":       credentials.BucketName,
					"accessKeyID":      credentials.AccessKeyID,
					"accessKey":        credentials.AccessKey,
					"enableSSE":        false,
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
					SourceJob:       tc.sourceJob,
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

				t.Log("verifying schema")
				tc.verifySchema(t, db, namespace)

				t.Log("verifying records")
				tc.verifyRecords(t, db, sourceID, destinationID, namespace, ts2.JobRunID, ts2.TaskRunID)
			})
		}
	})

	t.Run("Validation", func(t *testing.T) {
		namespace := whth.RandSchema(destType)
		iamNamespace := whth.RandSchema(destType)

		dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
			credentials.UserName, credentials.Password, credentials.Host, credentials.Port, credentials.DbName,
		)
		db, err := sql.Open("postgres", dsn)
		require.NoError(t, err)
		require.NoError(t, db.Ping())
		t.Cleanup(func() { _ = db.Close() })

		testCases := []struct {
			name        string
			destination backendconfig.DestinationT
		}{
			{
				name: "With password",
				destination: backendconfig.DestinationT{
					ID: "test_destination_id",
					Config: map[string]interface{}{
						"host":             credentials.Host,
						"port":             credentials.Port,
						"user":             credentials.UserName,
						"password":         credentials.Password,
						"database":         credentials.DbName,
						"bucketName":       credentials.BucketName,
						"accessKeyID":      credentials.AccessKeyID,
						"accessKey":        credentials.AccessKey,
						"namespace":        namespace,
						"syncFrequency":    "30",
						"enableSSE":        false,
						"useRudderStorage": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "1UVZiJF7OgLaiIY2Jts8XOQE3M6",
						Name:        "RS",
						DisplayName: "Redshift",
					},
					Name:        "redshift-demo",
					Enabled:     true,
					RevisionID:  "29HgOWobrn0RYZLpaSwPIbN2987",
					WorkspaceID: "test_workspace_id",
				},
			},
			{
				name: "with IAM Role",
				destination: backendconfig.DestinationT{
					ID: "test_destination_id",
					Config: map[string]interface{}{
						"user":              credentials.IAMUserName,
						"database":          credentials.DbName,
						"bucketName":        credentials.BucketName,
						"accessKeyID":       credentials.AccessKeyID,
						"accessKey":         credentials.AccessKey,
						"namespace":         iamNamespace,
						"useIAMForAuth":     true,
						"iamRoleARNForAuth": credentials.IAMRoleARN,
						"clusterId":         credentials.ClusterID,
						"clusterRegion":     credentials.ClusterRegion,
						"syncFrequency":     "30",
						"enableSSE":         false,
						"useRudderStorage":  false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "1UVZiJF7OgLaiIY2Jts8XOQE3M6",
						Name:        "RS",
						DisplayName: "Redshift",
					},
					Name:        "redshift-demo",
					Enabled:     true,
					RevisionID:  "29HgOWobrn0RYZLpaSwPIbN2987",
					WorkspaceID: "test_workspace_id",
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Cleanup(func() {
					dropSchema(t, db, tc.destination.Config["namespace"].(string))
				})

				whth.VerifyConfigurationTest(t, tc.destination)
			})
		}
	})

	t.Run("Load Table", func(t *testing.T) {
		namespace := whth.RandSchema(destType)
		iamNamespace := whth.RandSchema(destType)

		dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
			credentials.UserName, credentials.Password, credentials.Host, credentials.Port, credentials.DbName,
		)
		db, err := sql.Open("postgres", dsn)
		require.NoError(t, err)
		require.NoError(t, db.Ping())
		t.Cleanup(func() { _ = db.Close() })

		testCases := []struct {
			name      string
			warehouse model.Warehouse
		}{
			{
				name: "With password",
				warehouse: model.Warehouse{
					Source: backendconfig.SourceT{
						ID: "test_source_id",
					},
					Destination: backendconfig.DestinationT{
						ID: "test_destination_id",
						Config: map[string]interface{}{
							"host":             credentials.Host,
							"port":             credentials.Port,
							"user":             credentials.UserName,
							"password":         credentials.Password,
							"database":         credentials.DbName,
							"bucketName":       credentials.BucketName,
							"accessKeyID":      credentials.AccessKeyID,
							"accessKey":        credentials.AccessKey,
							"namespace":        namespace,
							"syncFrequency":    "30",
							"enableSSE":        false,
							"useRudderStorage": false,
						},
						DestinationDefinition: backendconfig.DestinationDefinitionT{
							ID:          "1UVZiJF7OgLaiIY2Jts8XOQE3M6",
							Name:        "RS",
							DisplayName: "Redshift",
						},
						Name:        "redshift-demo",
						Enabled:     true,
						RevisionID:  "29HgOWobrn0RYZLpaSwPIbN2987",
						WorkspaceID: "test_workspace_id",
					},
					WorkspaceID: "test_workspace_id",
					Namespace:   namespace,
				},
			},
			{
				name: "with IAM Role",
				warehouse: model.Warehouse{
					Source: backendconfig.SourceT{
						ID: "test_source_id",
					},
					Destination: backendconfig.DestinationT{
						ID: "test_destination_id",
						Config: map[string]interface{}{
							"user":              credentials.IAMUserName,
							"database":          credentials.DbName,
							"bucketName":        credentials.BucketName,
							"accessKeyID":       credentials.AccessKeyID,
							"accessKey":         credentials.AccessKey,
							"namespace":         iamNamespace,
							"useIAMForAuth":     true,
							"iamRoleARNForAuth": credentials.IAMRoleARN,
							"clusterId":         credentials.ClusterID,
							"clusterRegion":     credentials.ClusterRegion,
							"syncFrequency":     "30",
							"enableSSE":         false,
							"useRudderStorage":  false,
						},
						DestinationDefinition: backendconfig.DestinationDefinitionT{
							ID:          "1UVZiJF7OgLaiIY2Jts8XOQE3M6",
							Name:        "RS",
							DisplayName: "Redshift",
						},
						Name:        "redshift-iam-demo",
						Enabled:     true,
						RevisionID:  "29HgOWobrn0RYZLpaSwPIbN2987",
						WorkspaceID: "test_workspace_id",
					},
					WorkspaceID: "test_workspace_id",
					Namespace:   iamNamespace,
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Cleanup(func() {
					dropSchema(t, db, tc.warehouse.Namespace)
				})

				warehouse := tc.warehouse

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

				fm, err := filemanager.New(&filemanager.Settings{
					Provider: whutils.S3,
					Config: map[string]any{
						"bucketName":     credentials.BucketName,
						"accessKeyID":    credentials.AccessKeyID,
						"accessKey":      credentials.AccessKey,
						"bucketProvider": whutils.S3,
					},
				})
				require.NoError(t, err)

				t.Run("schema does not exists", func(t *testing.T) {
					ctx := context.Background()
					tableName := "schema_not_exists_test_table"

					uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

					loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
					mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv)

					rs := redshift.New(config.New(), logger.NOP, stats.NOP)
					err := rs.Setup(ctx, warehouse, mockUploader)
					require.NoError(t, err)

					loadTableStat, err := rs.LoadTable(ctx, tableName)
					require.Error(t, err)
					require.Nil(t, loadTableStat)
				})
				t.Run("table does not exists", func(t *testing.T) {
					ctx := context.Background()
					tableName := "table_not_exists_test_table"

					uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

					loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
					mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv)

					rs := redshift.New(config.New(), logger.NOP, stats.NOP)
					err := rs.Setup(ctx, warehouse, mockUploader)
					require.NoError(t, err)

					err = rs.CreateSchema(ctx)
					require.NoError(t, err)

					loadTableStat, err := rs.LoadTable(ctx, tableName)
					require.Error(t, err)
					require.Nil(t, loadTableStat)
				})
				t.Run("merge", func(t *testing.T) {
					t.Run("without dedup", func(t *testing.T) {
						ctx := context.Background()
						tableName := "merge_without_dedup_test_table"
						uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/dedup.csv.gz", tableName)

						loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
						mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv)

						appendWarehouse := th.Clone(t, warehouse)
						appendWarehouse.Destination.Config[model.PreferAppendSetting.String()] = true

						d := redshift.New(config.New(), logger.NOP, stats.NOP)
						err := d.Setup(ctx, appendWarehouse, mockUploader)
						require.NoError(t, err)

						err = d.CreateSchema(ctx)
						require.NoError(t, err)

						err = d.CreateTable(ctx, tableName, schemaInWarehouse)
						require.NoError(t, err)

						loadTableStat, err := d.LoadTable(ctx, tableName)
						require.NoError(t, err)
						require.Equal(t, loadTableStat.RowsInserted, int64(14))
						require.Equal(t, loadTableStat.RowsUpdated, int64(0))

						loadTableStat, err = d.LoadTable(ctx, tableName)
						require.NoError(t, err)
						require.Equal(t, loadTableStat.RowsInserted, int64(14))
						require.Equal(t, loadTableStat.RowsUpdated, int64(0))

						records := whth.RetrieveRecordsFromWarehouse(
							t,
							d.DB.DB,
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
								  %s.%s
								ORDER BY
								  id ASC;
								`,
								tc.warehouse.Namespace,
								tableName,
							),
						)
						require.Equal(t, whth.DedupTwiceTestRecords(), records)
					})
					t.Run("with dedup", func(t *testing.T) {
						ctx := context.Background()
						tableName := "merge_with_dedup_test_table"
						uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/dedup.csv.gz", tableName)

						loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
						mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv)

						d := redshift.New(config.New(), logger.NOP, stats.NOP)
						err := d.Setup(ctx, warehouse, mockUploader)
						require.NoError(t, err)

						err = d.CreateSchema(ctx)
						require.NoError(t, err)

						err = d.CreateTable(ctx, tableName, schemaInWarehouse)
						require.NoError(t, err)

						loadTableStat, err := d.LoadTable(ctx, tableName)
						require.NoError(t, err)
						require.Equal(t, loadTableStat.RowsInserted, int64(14))
						require.Equal(t, loadTableStat.RowsUpdated, int64(0))

						loadTableStat, err = d.LoadTable(ctx, tableName)
						require.NoError(t, err)
						require.Equal(t, loadTableStat.RowsInserted, int64(0))
						require.Equal(t, loadTableStat.RowsUpdated, int64(14))

						records := whth.RetrieveRecordsFromWarehouse(
							t,
							d.DB.DB,
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
								  %s.%s
								ORDER BY
								  id ASC;
							`,
								tc.warehouse.Namespace,
								tableName,
							),
						)
						require.Equal(t, whth.DedupTestRecords(), records)
					})
					t.Run("with dedup window", func(t *testing.T) {
						ctx := context.Background()
						tableName := "merge_with_dedup_window_test_table"
						uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/dedup.csv.gz", tableName)

						loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
						mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv)

						c := config.New()
						c.Set("Warehouse.redshift.dedupWindow", true)
						c.Set("Warehouse.redshift.dedupWindowInHours", 999999)

						d := redshift.New(c, logger.NOP, stats.NOP)
						err := d.Setup(ctx, warehouse, mockUploader)
						require.NoError(t, err)

						err = d.CreateSchema(ctx)
						require.NoError(t, err)

						err = d.CreateTable(ctx, tableName, schemaInWarehouse)
						require.NoError(t, err)

						loadTableStat, err := d.LoadTable(ctx, tableName)
						require.NoError(t, err)
						require.Equal(t, loadTableStat.RowsInserted, int64(14))
						require.Equal(t, loadTableStat.RowsUpdated, int64(0))

						loadTableStat, err = d.LoadTable(ctx, tableName)
						require.NoError(t, err)
						require.Equal(t, loadTableStat.RowsInserted, int64(0))
						require.Equal(t, loadTableStat.RowsUpdated, int64(14))

						records := whth.RetrieveRecordsFromWarehouse(
							t,
							d.DB.DB,
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
								  %s.%s
								ORDER BY
								  id ASC;
							`,
								tc.warehouse.Namespace,
								tableName,
							),
						)
						require.Equal(t, whth.DedupTestRecords(), records)
					})
					t.Run("with short dedup window", func(t *testing.T) {
						ctx := context.Background()
						tableName := "merge_with_short_dedup_window_test_table"
						uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/dedup.csv.gz", tableName)

						loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
						mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv)

						c := config.New()
						c.Set("Warehouse.redshift.dedupWindow", true)
						c.Set("Warehouse.redshift.dedupWindowInHours", 0)

						d := redshift.New(c, logger.NOP, stats.NOP)
						err := d.Setup(ctx, warehouse, mockUploader)
						require.NoError(t, err)

						err = d.CreateSchema(ctx)
						require.NoError(t, err)

						err = d.CreateTable(ctx, tableName, schemaInWarehouse)
						require.NoError(t, err)

						loadTableStat, err := d.LoadTable(ctx, tableName)
						require.NoError(t, err)
						require.Equal(t, loadTableStat.RowsInserted, int64(14))
						require.Equal(t, loadTableStat.RowsUpdated, int64(0))

						loadTableStat, err = d.LoadTable(ctx, tableName)
						require.NoError(t, err)
						require.Equal(t, loadTableStat.RowsInserted, int64(14))
						require.Equal(t, loadTableStat.RowsUpdated, int64(0))

						records := whth.RetrieveRecordsFromWarehouse(
							t,
							d.DB.DB,
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
								ORDER BY
								  id;
							`,
								tc.warehouse.Namespace,
								tableName,
							),
						)
						require.Equal(t, whth.DedupTwiceTestRecords(), records)
					})
				})
				t.Run("append", func(t *testing.T) {
					ctx := context.Background()
					tableName := "append_test_table"

					uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

					loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
					mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv)

					c := config.New()
					c.Set("Warehouse.redshift.skipDedupDestinationIDs", []string{"test_destination_id"})

					appendWarehouse := th.Clone(t, warehouse)
					appendWarehouse.Destination.Config[model.PreferAppendSetting.String()] = true

					rs := redshift.New(c, logger.NOP, stats.NOP)
					err := rs.Setup(ctx, appendWarehouse, mockUploader)
					require.NoError(t, err)

					err = rs.CreateSchema(ctx)
					require.NoError(t, err)

					err = rs.CreateTable(ctx, tableName, schemaInWarehouse)
					require.NoError(t, err)

					loadTableStat, err := rs.LoadTable(ctx, tableName)
					require.NoError(t, err)
					require.Equal(t, loadTableStat.RowsInserted, int64(14))
					require.Equal(t, loadTableStat.RowsUpdated, int64(0))

					loadTableStat, err = rs.LoadTable(ctx, tableName)
					require.NoError(t, err)
					require.Equal(t, loadTableStat.RowsInserted, int64(14))
					require.Equal(t, loadTableStat.RowsUpdated, int64(0))

					records := whth.RetrieveRecordsFromWarehouse(
						t,
						rs.DB.DB,
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
							tc.warehouse.Namespace,
							tableName,
						),
					)
					require.Equal(t, records, whth.AppendTestRecords())
				})
				t.Run("load file does not exists", func(t *testing.T) {
					ctx := context.Background()
					tableName := "load_file_not_exists_test_table"

					loadFiles := []whutils.LoadFile{{
						Location: "https://bucket.s3.amazonaws.com/rudder-warehouse-load-objects/load_file_not_exists_test_table/test_source_id/0ef75cb0-3fd0-4408-98b9-2bea9e476916-load_file_not_exists_test_table/load.csv.gz",
					}}
					mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv)

					rs := redshift.New(config.New(), logger.NOP, stats.NOP)
					err := rs.Setup(ctx, warehouse, mockUploader)
					require.NoError(t, err)

					err = rs.CreateSchema(ctx)
					require.NoError(t, err)

					err = rs.CreateTable(ctx, tableName, schemaInWarehouse)
					require.NoError(t, err)

					loadTableStat, err := rs.LoadTable(ctx, tableName)
					require.Error(t, err)
					require.Nil(t, loadTableStat)
				})
				t.Run("mismatch in number of columns", func(t *testing.T) {
					ctx := context.Background()
					tableName := "mismatch_columns_test_table"

					uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/mismatch-columns.csv.gz", tableName)

					loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
					mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv)

					rs := redshift.New(config.New(), logger.NOP, stats.NOP)
					err := rs.Setup(ctx, warehouse, mockUploader)
					require.NoError(t, err)

					err = rs.CreateSchema(ctx)
					require.NoError(t, err)

					err = rs.CreateTable(ctx, tableName, schemaInWarehouse)
					require.NoError(t, err)

					loadTableStat, err := rs.LoadTable(ctx, tableName)
					require.Error(t, err)
					require.Nil(t, loadTableStat)
				})
				t.Run("mismatch in schema", func(t *testing.T) {
					ctx := context.Background()
					tableName := "mismatch_schema_test_table"

					uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/mismatch-schema.csv.gz", tableName)

					loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
					mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv)

					rs := redshift.New(config.New(), logger.NOP, stats.NOP)
					err := rs.Setup(ctx, warehouse, mockUploader)
					require.NoError(t, err)

					err = rs.CreateSchema(ctx)
					require.NoError(t, err)

					err = rs.CreateTable(ctx, tableName, schemaInWarehouse)
					require.NoError(t, err)

					loadTableStat, err := rs.LoadTable(ctx, tableName)
					require.Error(t, err)
					require.Nil(t, loadTableStat)
				})
				t.Run("discards", func(t *testing.T) {
					ctx := context.Background()
					tableName := whutils.DiscardsTable

					uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/discards.csv.gz", tableName)

					loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
					mockUploader := newMockUploader(t, loadFiles, tableName, whutils.DiscardsSchema, whutils.DiscardsSchema, whutils.LoadFileTypeCsv)

					rs := redshift.New(config.New(), logger.NOP, stats.NOP)
					err := rs.Setup(ctx, warehouse, mockUploader)
					require.NoError(t, err)

					err = rs.CreateSchema(ctx)
					require.NoError(t, err)

					err = rs.CreateTable(ctx, tableName, whutils.DiscardsSchema)
					require.NoError(t, err)

					loadTableStat, err := rs.LoadTable(ctx, tableName)
					require.NoError(t, err)
					require.Equal(t, loadTableStat.RowsInserted, int64(6))
					require.Equal(t, loadTableStat.RowsUpdated, int64(0))

					records := whth.RetrieveRecordsFromWarehouse(
						t,
						rs.DB.DB,
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
							ORDER BY
							  row_id ASC;
						`,
							tc.warehouse.Namespace,
							tableName,
						),
					)
					require.Equal(t, records, whth.DiscardTestRecords())
				})
				t.Run("parquet", func(t *testing.T) {
					ctx := context.Background()
					tableName := "parquet_test_table"

					uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.parquet", tableName)

					fileStat, err := os.Stat("../testdata/load.parquet")
					require.NoError(t, err)

					loadFiles := []whutils.LoadFile{{
						Location: uploadOutput.Location,
						Metadata: json.RawMessage(fmt.Sprintf(`{"content_length": %d}`, fileStat.Size())),
					}}
					mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInUpload, whutils.LoadFileTypeParquet)

					rs := redshift.New(config.New(), logger.NOP, stats.NOP)
					err = rs.Setup(ctx, warehouse, mockUploader)
					require.NoError(t, err)

					err = rs.CreateSchema(ctx)
					require.NoError(t, err)

					err = rs.CreateTable(ctx, tableName, schemaInUpload)
					require.NoError(t, err)

					loadTableStat, err := rs.LoadTable(ctx, tableName)
					require.NoError(t, err)
					require.Equal(t, loadTableStat.RowsInserted, int64(14))
					require.Equal(t, loadTableStat.RowsUpdated, int64(0))

					records := whth.RetrieveRecordsFromWarehouse(
						t,
						rs.DB.DB,
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
							  id ASC;
						`,
							tc.warehouse.Namespace,
							tableName,
						),
					)
					require.Equal(t, records, whth.SampleTestRecords())
				})
				t.Run("crashRecover", func(t *testing.T) {
					ctx := context.Background()
					tableName := "crash_recovery_test_table"
					stgTableName := whutils.StagingTableName(destType, tableName, 64)
					mockUploader := newMockUploader(t, nil, tableName, schemaInUpload, schemaInUpload, whutils.LoadFileTypeParquet)

					rs := redshift.New(config.New(), logger.NOP, stats.NOP)
					err := rs.Setup(ctx, warehouse, mockUploader)
					require.NoError(t, err)

					err = rs.CreateSchema(ctx)
					require.NoError(t, err)

					err = rs.CreateTable(ctx, stgTableName, schemaInWarehouse)
					require.NoError(t, err)

					tableExists := func(t *testing.T) bool {
						t.Helper()

						var count int
						err = rs.DB.DB.QueryRow(`
							SELECT
							  count(*)
							FROM
							  information_schema.tables
							WHERE
							  table_schema = $1
							  AND table_name = $2;
						`,
							tc.warehouse.Namespace,
							stgTableName,
						).Scan(&count)
						require.NoError(t, err)

						return count == 1
					}
					require.True(t, tableExists(t))

					err = rs.CrashRecover(ctx)
					require.NoError(t, err)

					require.False(t, tableExists(t))
				})
			})
		}
	})

	t.Run("Connection timeout using password", func(t *testing.T) {
		namespace := whth.RandSchema(destType)

		warehouse := model.Warehouse{
			Source: backendconfig.SourceT{
				ID: "test_source_id",
			},
			Destination: backendconfig.DestinationT{
				ID: "test_destination_id",
				Config: map[string]interface{}{
					"host":             credentials.Host,
					"port":             credentials.Port,
					"user":             credentials.UserName,
					"password":         credentials.Password,
					"database":         credentials.DbName,
					"bucketName":       credentials.BucketName,
					"accessKeyID":      credentials.AccessKeyID,
					"accessKey":        credentials.AccessKey,
					"namespace":        namespace,
					"syncFrequency":    "30",
					"enableSSE":        false,
					"useRudderStorage": false,
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:          "1UVZiJF7OgLaiIY2Jts8XOQE3M6",
					Name:        "RS",
					DisplayName: "Redshift",
				},
				Name:        "redshift-demo",
				Enabled:     true,
				RevisionID:  "29HgOWobrn0RYZLpaSwPIbN2987",
				WorkspaceID: "test_workspace_id",
			},
			WorkspaceID: "test_workspace_id",
			Namespace:   namespace,
		}
		mockCtrl := gomock.NewController(t)
		uploader := mockuploader.NewMockUploader(mockCtrl)

		t.Run("no timeout", func(t *testing.T) {
			ctx := context.Background()
			rs := redshift.New(config.New(), logger.NOP, stats.NOP)
			require.NoError(t, rs.Setup(ctx, warehouse, uploader))
			require.NoError(t, rs.DB.PingContext(ctx))
		})
		t.Run("timeout = 0s", func(t *testing.T) {
			ctx := context.Background()
			rs := redshift.New(config.New(), logger.NOP, stats.NOP)
			rs.SetConnectionTimeout(0)
			require.NoError(t, rs.Setup(ctx, warehouse, uploader))
			require.NoError(t, rs.DB.PingContext(ctx))
		})
		t.Run("0s < timeout < 1s", func(t *testing.T) {
			ctx := context.Background()
			rs := redshift.New(config.New(), logger.NOP, stats.NOP)
			rs.SetConnectionTimeout(500 * time.Millisecond)
			require.Error(t, rs.Setup(ctx, warehouse, uploader))
		})
		t.Run("timeout >= 1s", func(t *testing.T) {
			ctx := context.Background()
			rs := redshift.New(config.New(), logger.NOP, stats.NOP)
			rs.SetConnectionTimeout(10 * time.Second)
			require.NoError(t, rs.Setup(ctx, warehouse, uploader))
			require.NoError(t, rs.DB.PingContext(ctx))
		})
	})
}

func dropSchema(t *testing.T, db *sql.DB, namespace string) {
	t.Helper()
	t.Log("dropping schema", namespace)

	require.Eventually(t,
		func() bool {
			_, err := db.ExecContext(context.Background(), fmt.Sprintf(`DROP SCHEMA %q CASCADE;`, namespace))
			if err != nil {
				t.Logf("error deleting schema %q: %v", namespace, err)
				return false
			}
			return true
		},
		time.Minute,
		time.Second,
	)
}

func TestRedshift_ShouldMerge(t *testing.T) {
	testCases := []struct {
		name              string
		preferAppend      bool
		tableName         string
		appendOnlyTables  []string
		uploaderCanAppend bool
		expected          bool
	}{
		{
			name:              "uploader says we can append and user prefers append",
			preferAppend:      true,
			uploaderCanAppend: true,
			tableName:         "tracks",
			expected:          false,
		},
		{
			name:              "uploader says we cannot append and user prefers append",
			preferAppend:      true,
			uploaderCanAppend: false,
			tableName:         "tracks",
			expected:          true,
		},
		{
			name:              "uploader says we can append and user prefers not to append",
			preferAppend:      false,
			uploaderCanAppend: true,
			tableName:         "tracks",
			expected:          true,
		},
		{
			name:              "uploader says we cannot append and user prefers not to append",
			preferAppend:      false,
			uploaderCanAppend: false,
			tableName:         "tracks",
			expected:          true,
		},
		{
			name:              "uploader says we can append, in merge mode, but table is in append only",
			preferAppend:      false,
			uploaderCanAppend: true,
			tableName:         "tracks",
			appendOnlyTables:  []string{"tracks"},
			expected:          false,
		},
		{
			name:              "uploader says we can append, in append mode, but table is in append only",
			preferAppend:      true,
			uploaderCanAppend: true,
			tableName:         "tracks",
			appendOnlyTables:  []string{"tracks"},
			expected:          false,
		},
		{
			name:              "uploader says we can append, in append mode, but table is not in append only",
			preferAppend:      true,
			uploaderCanAppend: true,
			tableName:         "page_views",
			appendOnlyTables:  []string{"tracks"},
			expected:          false,
		},
		{
			name:              "uploader says we cannot append, in merge mode, but table is in append only",
			preferAppend:      false,
			uploaderCanAppend: false,
			tableName:         "tracks",
			appendOnlyTables:  []string{"tracks"},
			expected:          false,
		},
		{
			name:              "uploader says we can append, in merge mode, but table is not in append only",
			preferAppend:      false,
			uploaderCanAppend: true,
			tableName:         "page_views",
			appendOnlyTables:  []string{"tracks"},
			expected:          true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			destID := "test_destination_id"

			c := config.New()
			c.Set("Warehouse.redshift.appendOnlyTables."+destID, tc.appendOnlyTables)

			rs := redshift.New(c, logger.NOP, stats.NOP)

			rs.Warehouse = model.Warehouse{
				Destination: backendconfig.DestinationT{
					ID: destID,
					Config: map[string]any{
						model.PreferAppendSetting.String(): tc.preferAppend,
					},
				},
			}

			mockCtrl := gomock.NewController(t)
			uploader := mockuploader.NewMockUploader(mockCtrl)
			uploader.EXPECT().CanAppend().AnyTimes().Return(tc.uploaderCanAppend)

			rs.Uploader = uploader
			require.Equal(t, rs.ShouldMerge(tc.tableName), tc.expected)
		})
	}
}

func TestCheckAndIgnoreColumnAlreadyExistError(t *testing.T) {
	testCases := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			expected: true,
		},
		{
			name: "column already exists error",
			err: &pq.Error{
				Code: "42701",
			},
			expected: true,
		},
		{
			name:     "other error",
			err:      errors.New("other error"),
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, redshift.CheckAndIgnoreColumnAlreadyExistError(tc.err))
		})
	}
}

func TestRedshift_AlterColumn(t *testing.T) {
	var (
		bigString      = strings.Repeat("a", 1024)
		smallString    = strings.Repeat("a", 510)
		testNamespace  = "test_namespace"
		testTable      = "test_table"
		testColumn     = "test_column"
		testColumnType = "text"
	)

	testCases := []struct {
		name       string
		createView bool
	}{
		{
			name: "success",
		},
		{
			name:       "view/rule",
			createView: true,
		},
	}

	ctx := context.Background()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			pgResource, err := postgres.Setup(pool, t)
			require.NoError(t, err)

			t.Log("db:", pgResource.DBDsn)

			rs := redshift.New(config.New(), logger.NOP, stats.NOP)

			rs.DB = sqlmiddleware.New(pgResource.DB)
			rs.Namespace = testNamespace

			_, err = rs.DB.Exec(
				fmt.Sprintf("CREATE SCHEMA %s;",
					testNamespace,
				),
			)
			require.NoError(t, err)

			_, err = rs.DB.Exec(
				fmt.Sprintf("CREATE TABLE %q.%q (%s VARCHAR(512));",
					testNamespace,
					testTable,
					testColumn,
				),
			)
			require.NoError(t, err)

			if tc.createView {
				_, err = rs.DB.Exec(
					fmt.Sprintf("CREATE VIEW %[1]q.%[2]q AS SELECT * FROM %[1]q.%[3]q;",
						testNamespace,
						fmt.Sprintf("%s_view", testTable),
						testTable,
					),
				)
				require.NoError(t, err)
			}

			_, err = rs.DB.Exec(
				fmt.Sprintf("INSERT INTO %q.%q (%s) VALUES ('%s');",
					testNamespace,
					testTable,
					testColumn,
					smallString,
				),
			)
			require.NoError(t, err)

			_, err = rs.DB.Exec(
				fmt.Sprintf("INSERT INTO %q.%q (%s) VALUES ('%s');",
					testNamespace,
					testTable,
					testColumn,
					bigString,
				),
			)
			require.ErrorContains(t, err, errors.New("pq: value too long for type character varying(512)").Error())

			res, err := rs.AlterColumn(ctx, testTable, testColumn, testColumnType)
			require.NoError(t, err)

			if tc.createView {
				require.True(t, res.IsDependent)
				require.NotEmpty(t, res.Query)
			}

			_, err = rs.DB.Exec(
				fmt.Sprintf("INSERT INTO %q.%q (%s) VALUES ('%s');",
					testNamespace,
					testTable,
					testColumn,
					bigString,
				),
			)
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
	loadFileType string,
) whutils.Uploader {
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
	mockUploader.EXPECT().GetLoadFileType().Return(loadFileType).AnyTimes()
	mockUploader.EXPECT().CanAppend().Return(true).AnyTimes()

	return mockUploader
}
