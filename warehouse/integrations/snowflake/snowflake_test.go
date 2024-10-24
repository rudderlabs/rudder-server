package snowflake_test

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

	"github.com/samber/lo"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	sqlconnectconfig "github.com/rudderlabs/sqlconnect-go/sqlconnect/config"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"go.uber.org/mock/gomock"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	th "github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/snowflake"
	whth "github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

type testCredentials struct {
	Account              string `json:"account"`
	User                 string `json:"user"`
	Password             string `json:"password"`
	Role                 string `json:"role"`
	Database             string `json:"database"`
	Warehouse            string `json:"warehouse"`
	BucketName           string `json:"bucketName"`
	AccessKeyID          string `json:"accessKeyID"`
	AccessKey            string `json:"accessKey"`
	UseKeyPairAuth       bool   `json:"useKeyPairAuth"`
	PrivateKey           string `json:"privateKey"`
	PrivateKeyPassphrase string `json:"privateKeyPassphrase"`
}

const (
	testKey                = "SNOWFLAKE_INTEGRATION_TEST_CREDENTIALS"
	testRBACKey            = "SNOWFLAKE_RBAC_INTEGRATION_TEST_CREDENTIALS"
	testKeyPairEncrypted   = "SNOWFLAKE_KEYPAIR_ENCRYPTED_INTEGRATION_TEST_CREDENTIALS"
	testKeyPairUnencrypted = "SNOWFLAKE_KEYPAIR_UNENCRYPTED_INTEGRATION_TEST_CREDENTIALS"
)

func getSnowflakeTestCredentials(key string) (*testCredentials, error) {
	cred, exists := os.LookupEnv(key)
	if !exists {
		return nil, errors.New("snowflake test credentials not found")
	}

	var credentials testCredentials
	err := json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		return nil, fmt.Errorf("unable to marshall %s to snowflake test credentials: %v", key, err)
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

	destType := whutils.SNOWFLAKE

	credentials, err := getSnowflakeTestCredentials(testKey)
	require.NoError(t, err)

	t.Run("Event flow", func(t *testing.T) {
		for _, key := range []string{
			testRBACKey,
			testKeyPairEncrypted,
			testKeyPairUnencrypted,
		} {
			if _, exists := os.LookupEnv(key); !exists {
				if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
					t.Fatalf("%s environment variable not set", key)
				}
				t.Skipf("Skipping %s as %s is not set", t.Name(), key)
			}
		}

		rbacCredentials, err := getSnowflakeTestCredentials(testRBACKey)
		require.NoError(t, err)
		keyPairEncryptedCredentials, err := getSnowflakeTestCredentials(testKeyPairEncrypted)
		require.NoError(t, err)
		keyPairUnEncryptedCredentials, err := getSnowflakeTestCredentials(testKeyPairUnencrypted)
		require.NoError(t, err)

		httpPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		c := testcompose.New(t, compose.FilePaths([]string{"../testdata/docker-compose.jobsdb.yml", "../testdata/docker-compose.transformer.yml"}))
		c.Start(context.Background())

		workspaceID := whutils.RandHex()
		jobsDBPort := c.Port("jobsDb", 5432)
		transformerURL := fmt.Sprintf("http://localhost:%d", c.Port("transformer", 9090))

		jobsDB := whth.JobsDB(t, jobsDBPort)

		expectedUploadJobSchema := model.Schema{
			"SCREENS":                     {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "ID": "TEXT", "NAME": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "TITLE": "TEXT", "URL": "TEXT", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ", "_AS": "TEXT", "_BETWEEN": "TEXT"},
			"IDENTIFIES":                  {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "CONTEXT_TRAITS_AS": "TEXT", "CONTEXT_TRAITS_BETWEEN": "TEXT", "CONTEXT_TRAITS_EMAIL": "TEXT", "CONTEXT_TRAITS_LOGINS": "NUMBER", "CONTEXT_TRAITS_NAME": "TEXT", "EMAIL": "TEXT", "ID": "TEXT", "LOGINS": "NUMBER", "NAME": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ", "_AS": "TEXT", "_BETWEEN": "TEXT"},
			"USERS":                       {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "CONTEXT_TRAITS_AS": "TEXT", "CONTEXT_TRAITS_BETWEEN": "TEXT", "CONTEXT_TRAITS_EMAIL": "TEXT", "CONTEXT_TRAITS_LOGINS": "NUMBER", "CONTEXT_TRAITS_NAME": "TEXT", "EMAIL": "TEXT", "ID": "TEXT", "LOGINS": "NUMBER", "NAME": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "UUID_TS": "TIMESTAMP_TZ", "_AS": "TEXT", "_BETWEEN": "TEXT"},
			"PRODUCT_TRACK":               {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "PRODUCT_ID": "TEXT", "RATING": "NUMBER", "RECEIVED_AT": "TIMESTAMP_TZ", "REVIEW_BODY": "TEXT", "REVIEW_ID": "TEXT", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ", "_AS": "TEXT", "_BETWEEN": "TEXT"},
			"TRACKS":                      {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EVENT": "TEXT", "EVENT_TEXT": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"ALIASES":                     {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "PREVIOUS_ID": "TEXT", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"PAGES":                       {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "ID": "TEXT", "NAME": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "TITLE": "TEXT", "URL": "TEXT", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ", "_AS": "TEXT", "_BETWEEN": "TEXT"},
			"GROUPS":                      {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "EMPLOYEES": "NUMBER", "GROUP_ID": "TEXT", "ID": "TEXT", "INDUSTRY": "TEXT", "NAME": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "PLAN": "TEXT", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ", "_AS": "TEXT", "_BETWEEN": "TEXT"},
			"RUDDER_IDENTITY_MERGE_RULES": {"MERGE_PROPERTY_1_TYPE": "TEXT", "MERGE_PROPERTY_1_VALUE": "TEXT", "MERGE_PROPERTY_2_TYPE": "TEXT", "MERGE_PROPERTY_2_VALUE": "TEXT"},
			"RUDDER_IDENTITY_MAPPINGS":    {"MERGE_PROPERTY_TYPE": "TEXT", "MERGE_PROPERTY_VALUE": "TEXT", "RUDDER_ID": "TEXT", "UPDATED_AT": "TIMESTAMP_TZ"},
		}
		expectedSourceJobSchema := model.Schema{
			"TRACKS":                      {"ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "UUID_TS": "TIMESTAMP_TZ", "EVENT_TEXT": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCES_JOB_ID": "TEXT", "CONTEXT_SOURCES_VERSION": "TEXT", "CONTEXT_SOURCES_TASK_RUN_ID": "TEXT", "ID": "TEXT", "CHANNEL": "TEXT", "RECEIVED_AT": "TIMESTAMP_TZ", "CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "USER_ID": "TEXT", "CONTEXT_SOURCES_JOB_RUN_ID": "TEXT", "EVENT": "TEXT"},
			"GOOGLE_SHEET":                {"_AS": "TEXT", "REVIEW_BODY": "TEXT", "RATING": "NUMBER", "CONTEXT_SOURCE_TYPE": "TEXT", "_BETWEEN": "TEXT", "CONTEXT_DESTINATION_ID": "TEXT", "REVIEW_ID": "TEXT", "CONTEXT_SOURCES_VERSION": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "ID": "TEXT", "USER_ID": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "PRODUCT_ID": "TEXT", "CONTEXT_SOURCES_TASK_RUN_ID": "TEXT", "EVENT": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "SENT_AT": "TIMESTAMP_TZ", "UUID_TS": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "CONTEXT_SOURCES_JOB_RUN_ID": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_SOURCES_JOB_ID": "TEXT", "CHANNEL": "TEXT", "EVENT_TEXT": "TEXT"},
			"RUDDER_IDENTITY_MERGE_RULES": {"MERGE_PROPERTY_1_VALUE": "TEXT", "MERGE_PROPERTY_2_VALUE": "TEXT", "MERGE_PROPERTY_2_TYPE": "TEXT", "MERGE_PROPERTY_1_TYPE": "TEXT"},
			"RUDDER_IDENTITY_MAPPINGS":    {"MERGE_PROPERTY_VALUE": "TEXT", "MERGE_PROPERTY_TYPE": "TEXT", "RUDDER_ID": "TEXT", "UPDATED_AT": "TIMESTAMP_TZ"},
		}
		userIDFormat := "userId_snowflake"
		userIDSQL := "SUBSTR(user_id, 1, 16)"
		uuidTSSQL := "TO_CHAR(uuid_ts, 'YYYY-MM-DD')"

		testcase := []struct {
			name                           string
			tables                         []string
			cred                           *testCredentials
			database                       string
			warehouseEventsMap2            whth.EventsCountMap
			sourceJob                      bool
			eventFilePath1, eventFilePath2 string
			jobRunID1, taskRunID1          string
			jobRunID2, taskRunID2          string
			useSameUserID                  bool
			configOverride                 map[string]any
			verifySchema                   func(t *testing.T, db *sql.DB, namespace string)
			verifyRecords                  func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace, jobRunID, taskRunID string)
		}{
			{
				name:           "Upload Job",
				tables:         []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				cred:           credentials,
				database:       credentials.Database,
				eventFilePath1: "../testdata/upload-job.events-1.json",
				eventFilePath2: "../testdata/upload-job.events-2.json",
				configOverride: map[string]any{
					"preferAppend": false,
					"password":     credentials.Password,
				},
				verifySchema: func(t *testing.T, db *sql.DB, namespace string) {
					t.Helper()
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
					require.Equal(t, expectedUploadJobSchema, whth.ConvertRecordsToSchema(schema))
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					t.Helper()
					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "IDENTIFIES"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, substring(id, 1, 16), %s, received_at, name, original_timestamp FROM %q.%q ORDER BY id;`, uuidTSSQL, namespace, "USERS"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersRecords(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "TRACKS"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "PRODUCT_TRACK"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "PAGES"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, _as FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "SCREENS"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "ALIASES"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "GROUPS"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
			{
				name:           "Upload Job with Role",
				tables:         []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				cred:           rbacCredentials,
				database:       rbacCredentials.Database,
				eventFilePath1: "../testdata/upload-job.events-1.json",
				eventFilePath2: "../testdata/upload-job.events-2.json",
				configOverride: map[string]any{
					"preferAppend": false,
					"role":         rbacCredentials.Role,
					"password":     rbacCredentials.Password,
				},
				verifySchema: func(t *testing.T, db *sql.DB, namespace string) {
					t.Helper()
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
					require.Equal(t, expectedUploadJobSchema, whth.ConvertRecordsToSchema(schema))
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					t.Helper()
					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "IDENTIFIES"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, substring(id, 1, 16), %s, received_at, name, original_timestamp FROM %q.%q ORDER BY id;`, uuidTSSQL, namespace, "USERS"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersRecords(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "TRACKS"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "PRODUCT_TRACK"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "PAGES"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, _as FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "SCREENS"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "ALIASES"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "GROUPS"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
			{
				name:           "Upload Job with Case Sensitive Database",
				tables:         []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				cred:           credentials,
				database:       strings.ToLower(credentials.Database),
				eventFilePath1: "../testdata/upload-job.events-1.json",
				eventFilePath2: "../testdata/upload-job.events-2.json",
				configOverride: map[string]any{
					"preferAppend": false,
					"password":     credentials.Password,
				},
				verifySchema: func(t *testing.T, db *sql.DB, namespace string) {
					t.Helper()
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
					require.Equal(t, expectedUploadJobSchema, whth.ConvertRecordsToSchema(schema))
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					t.Helper()
					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "IDENTIFIES"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, substring(id, 1, 16), %s, received_at, name, original_timestamp FROM %q.%q ORDER BY id;`, uuidTSSQL, namespace, "USERS"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersRecords(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "TRACKS"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "PRODUCT_TRACK"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "PAGES"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, _as FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "SCREENS"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "ALIASES"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "GROUPS"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
			{
				name:           "Upload Job with Key Pair Unencrypted Key",
				tables:         []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				cred:           keyPairUnEncryptedCredentials,
				database:       keyPairUnEncryptedCredentials.Database,
				eventFilePath1: "../testdata/upload-job.events-1.json",
				eventFilePath2: "../testdata/upload-job.events-2.json",
				configOverride: map[string]any{
					"preferAppend":   false,
					"useKeyPairAuth": true,
					"privateKey":     keyPairUnEncryptedCredentials.PrivateKey,
				},

				verifySchema: func(t *testing.T, db *sql.DB, namespace string) {
					t.Helper()
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
					require.Equal(t, expectedUploadJobSchema, whth.ConvertRecordsToSchema(schema))
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					t.Helper()
					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "IDENTIFIES"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, substring(id, 1, 16), %s, received_at, name, original_timestamp FROM %q.%q ORDER BY id;`, uuidTSSQL, namespace, "USERS"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersRecords(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "TRACKS"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "PRODUCT_TRACK"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "PAGES"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, _as FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "SCREENS"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "ALIASES"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "GROUPS"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
			{
				name:           "Upload Job with Key Pair Encrypted Key",
				tables:         []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				cred:           keyPairEncryptedCredentials,
				database:       keyPairEncryptedCredentials.Database,
				eventFilePath1: "../testdata/upload-job.events-1.json",
				eventFilePath2: "../testdata/upload-job.events-2.json",
				configOverride: map[string]any{
					"preferAppend":         false,
					"useKeyPairAuth":       true,
					"privateKey":           keyPairEncryptedCredentials.PrivateKey,
					"privateKeyPassphrase": keyPairEncryptedCredentials.PrivateKeyPassphrase,
				},
				verifySchema: func(t *testing.T, db *sql.DB, namespace string) {
					t.Helper()
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
					require.Equal(t, expectedUploadJobSchema, whth.ConvertRecordsToSchema(schema))
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					t.Helper()
					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "IDENTIFIES"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, substring(id, 1, 16), %s, received_at, name, original_timestamp FROM %q.%q ORDER BY id;`, uuidTSSQL, namespace, "USERS"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersRecords(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "TRACKS"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "PRODUCT_TRACK"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "PAGES"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, _as FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "SCREENS"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "ALIASES"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "GROUPS"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
			{
				name:           "Source Job with Sources",
				tables:         []string{"tracks", "google_sheet"},
				cred:           credentials,
				database:       credentials.Database,
				sourceJob:      true,
				jobRunID1:      misc.FastUUID().String(),
				taskRunID1:     misc.FastUUID().String(),
				jobRunID2:      misc.FastUUID().String(),
				taskRunID2:     misc.FastUUID().String(),
				eventFilePath1: "../testdata/source-job.events-1.json",
				eventFilePath2: "../testdata/source-job.events-2.json",
				configOverride: map[string]any{
					"preferAppend": false,
					"password":     credentials.Password,
				},
				verifySchema: func(t *testing.T, db *sql.DB, namespace string) {
					t.Helper()
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
					require.Equal(t, expectedSourceJobSchema, whth.ConvertRecordsToSchema(schema))
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					t.Helper()
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT channel, context_sources_job_id, received_at, context_sources_version, %s, sent_at, context_ip, event, event_text, %s, context_destination_id, id, context_request_ip, context_source_type, original_timestamp, context_sources_job_run_id, context_sources_task_run_id, context_source_id, context_destination_type, timestamp FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "TRACKS"))
					require.ElementsMatch(t, tracksRecords, whth.SourceJobTracksRecords(userIDFormat, sourceID, destinationID, destType, jobRunID, taskRunID))
					googleSheetRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT product_id, sent_at, _between, context_request_ip, context_sources_job_run_id, channel, review_body, context_source_id, original_timestamp, context_destination_id, context_sources_job_id, event, context_sources_task_run_id, context_source_type, %s, context_ip, timestamp, id, received_at, review_id, %s, context_sources_version, context_destination_type, event_text, _as, rating FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "GOOGLE_SHEET"))
					require.ElementsMatch(t, googleSheetRecords, whth.SourceJobGoogleSheetRecords(userIDFormat, sourceID, destinationID, destType, jobRunID, taskRunID))
				},
			},
			{
				name:     "Upload Job in append mode",
				tables:   []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				cred:     credentials,
				database: credentials.Database,
				warehouseEventsMap2: whth.EventsCountMap{
					// For all tables except users we will be appending because of preferAppend config
					"identifies": 8, "users": 1, "tracks": 8, "product_track": 8, "pages": 8, "screens": 8, "aliases": 8, "groups": 8,
				},
				eventFilePath1: "../testdata/upload-job.events-1.json",
				eventFilePath2: "../testdata/upload-job.events-1.json",
				useSameUserID:  true,
				configOverride: map[string]any{
					"preferAppend": true,
					"password":     credentials.Password,
				},
				verifySchema: func(t *testing.T, db *sql.DB, namespace string) {
					t.Helper()
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
					require.Equal(t, expectedUploadJobSchema, whth.ConvertRecordsToSchema(schema))
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					t.Helper()
					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "IDENTIFIES"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, substring(id, 1, 16), %s, received_at, name, original_timestamp FROM %q.%q ORDER BY id;`, uuidTSSQL, namespace, "USERS"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersMergeRecord(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "TRACKS"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksAppendRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "PRODUCT_TRACK"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackAppendRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "PAGES"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, _as FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "SCREENS"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensAppendRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "ALIASES"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "GROUPS"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsAppendRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
			{
				name:           "Undefined preferAppend",
				tables:         []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				cred:           credentials,
				database:       credentials.Database,
				eventFilePath1: "../testdata/upload-job.events-1.json",
				eventFilePath2: "../testdata/upload-job.events-1.json",
				useSameUserID:  true,
				configOverride: map[string]any{
					"password": credentials.Password,
				},
				verifySchema: func(t *testing.T, db *sql.DB, namespace string) {
					t.Helper()
					schema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
					require.Equal(t, expectedUploadJobSchema, whth.ConvertRecordsToSchema(schema))
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace, jobRunID, taskRunID string) {
					t.Helper()
					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "IDENTIFIES"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesMergeRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, substring(id, 1, 16), %s, received_at, name, original_timestamp FROM %q.%q ORDER BY id;`, uuidTSSQL, namespace, "USERS"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersMergeRecord(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "TRACKS"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksMergeRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "PRODUCT_TRACK"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackMergeRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "PAGES"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesMergeRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, _as FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "SCREENS"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensMergeRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %q.%q ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "ALIASES"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesMergeRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %q.%q ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "GROUPS"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsMergeRecords(userIDFormat, sourceID, destinationID, destType))
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
					WithConfigOption("account", tc.cred.Account).
					WithConfigOption("database", tc.database).
					WithConfigOption("warehouse", tc.cred.Warehouse).
					WithConfigOption("user", tc.cred.User).
					WithConfigOption("cloudProvider", "AWS").
					WithConfigOption("bucketName", tc.cred.BucketName).
					WithConfigOption("accessKeyID", tc.cred.AccessKeyID).
					WithConfigOption("accessKey", tc.cred.AccessKey).
					WithConfigOption("namespace", namespace).
					WithConfigOption("enableSSE", false).
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

				t.Setenv("RSERVER_WAREHOUSE_ENABLE_IDRESOLUTION", "true")
				t.Setenv("RSERVER_WAREHOUSE_SNOWFLAKE_MAX_PARALLEL_LOADS", "8")
				t.Setenv("RSERVER_WAREHOUSE_SNOWFLAKE_ENABLE_DELETE_BY_JOBS", "true")
				t.Setenv("RSERVER_WAREHOUSE_SNOWFLAKE_SLOW_QUERY_THRESHOLD", "0s")
				t.Setenv("RSERVER_WAREHOUSE_SNOWFLAKE_DEBUG_DUPLICATE_WORKSPACE_IDS", workspaceID)
				t.Setenv("RSERVER_WAREHOUSE_SNOWFLAKE_DEBUG_DUPLICATE_TABLES", strings.Join(
					[]string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
					" ",
				))

				whth.BootstrapSvc(t, workspaceConfig, httpPort, jobsDBPort)

				credentialsJSON, err := json.Marshal(sqlconnectconfig.Snowflake{
					Account:              tc.cred.Account,
					User:                 tc.cred.User,
					Role:                 tc.cred.Role,
					DBName:               tc.database,
					Warehouse:            tc.cred.Warehouse,
					Password:             tc.cred.Password,
					UseKeyPairAuth:       tc.cred.UseKeyPairAuth,
					PrivateKey:           tc.cred.PrivateKey,
					PrivateKeyPassphrase: tc.cred.PrivateKeyPassphrase,
				})
				require.NoError(t, err)

				sqlConnectDB, err := sqlconnect.NewDB("snowflake", credentialsJSON)
				require.NoError(t, err)

				db := sqlConnectDB.SqlDB()
				require.NoError(t, db.Ping())
				t.Cleanup(func() { _ = db.Close() })
				t.Cleanup(func() {
					dropSchema(t, db, namespace)
				})

				sqlClient := &client.Client{
					SQL:  db,
					Type: client.SQLClient,
				}

				conf := map[string]interface{}{
					"cloudProvider":      "AWS",
					"bucketName":         tc.cred.BucketName,
					"storageIntegration": "",
					"accessKeyID":        tc.cred.AccessKeyID,
					"accessKey":          tc.cred.AccessKey,
					"prefix":             "snowflake-prefix",
					"enableSSE":          false,
					"useRudderStorage":   false,
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

	t.Run("Validation", func(t *testing.T) {
		namespace := whth.RandSchema(destType)

		credentialsJSON, err := json.Marshal(sqlconnectconfig.Snowflake{
			Account:   credentials.Account,
			User:      credentials.User,
			Role:      credentials.Role,
			Password:  credentials.Password,
			DBName:    credentials.Database,
			Warehouse: credentials.Warehouse,
		})
		require.NoError(t, err)

		sqlConnectDB, err := sqlconnect.NewDB("snowflake", credentialsJSON)
		require.NoError(t, err)

		db := sqlConnectDB.SqlDB()
		require.NoError(t, db.Ping())
		t.Cleanup(func() { _ = db.Close() })
		t.Cleanup(func() {
			dropSchema(t, db, namespace)
		})

		dest := backendconfig.DestinationT{
			ID: "test_destination_id",
			Config: map[string]interface{}{
				"account":            credentials.Account,
				"database":           credentials.Database,
				"warehouse":          credentials.Warehouse,
				"user":               credentials.User,
				"password":           credentials.Password,
				"cloudProvider":      "AWS",
				"bucketName":         credentials.BucketName,
				"storageIntegration": "",
				"accessKeyID":        credentials.AccessKeyID,
				"accessKey":          credentials.AccessKey,
				"namespace":          namespace,
				"prefix":             "snowflake-prefix",
				"syncFrequency":      "30",
				"enableSSE":          false,
				"useRudderStorage":   false,
				"preferAppend":       false,
			},
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				ID:          "1XjvXnzw34UMAz1YOuKqL1kwzh6",
				Name:        "SNOWFLAKE",
				DisplayName: "Snowflake",
			},
			Name:       "snowflake-demo",
			Enabled:    true,
			RevisionID: "test_destination_id",
		}
		whth.VerifyConfigurationTest(t, dest)
	})

	t.Run("Load Table", func(t *testing.T) {
		ctx := context.Background()
		namespace := whth.RandSchema(destType)

		credentialsJSON, err := json.Marshal(sqlconnectconfig.Snowflake{
			Account:   credentials.Account,
			User:      credentials.User,
			Role:      credentials.Role,
			Password:  credentials.Password,
			DBName:    credentials.Database,
			Warehouse: credentials.Warehouse,
		})
		require.NoError(t, err)

		sqlConnectDB, err := sqlconnect.NewDB("snowflake", credentialsJSON)
		require.NoError(t, err)

		db := sqlConnectDB.SqlDB()
		require.NoError(t, db.Ping())
		t.Cleanup(func() { _ = db.Close() })
		t.Cleanup(func() {
			dropSchema(t, db, namespace)
		})

		schemaInUpload := model.TableSchema{
			"TEST_BOOL":     "boolean",
			"TEST_DATETIME": "datetime",
			"TEST_FLOAT":    "float",
			"TEST_INT":      "int",
			"TEST_STRING":   "string",
			"ID":            "string",
			"RECEIVED_AT":   "datetime",
		}
		schemaInWarehouse := model.TableSchema{
			"TEST_BOOL":           "boolean",
			"TEST_DATETIME":       "datetime",
			"TEST_FLOAT":          "float",
			"TEST_INT":            "int",
			"TEST_STRING":         "string",
			"ID":                  "string",
			"RECEIVED_AT":         "datetime",
			"EXTRA_TEST_BOOL":     "boolean",
			"EXTRA_TEST_DATETIME": "datetime",
			"EXTRA_TEST_FLOAT":    "float",
			"EXTRA_TEST_INT":      "int",
			"EXTRA_TEST_STRING":   "string",
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
					"account":            credentials.Account,
					"database":           credentials.Database,
					"warehouse":          credentials.Warehouse,
					"user":               credentials.User,
					"password":           credentials.Password,
					"cloudProvider":      "AWS",
					"bucketName":         credentials.BucketName,
					"storageIntegration": "",
					"accessKeyID":        credentials.AccessKeyID,
					"accessKey":          credentials.AccessKey,
					"namespace":          namespace,
				},
			},
			WorkspaceID: "test_workspace_id",
			Namespace:   namespace,
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
			tableName := whutils.ToProviderCase(destType, "schema_not_exists_test_table")

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, false, false)

			sf := snowflake.New(config.New(), logger.NOP, stats.NOP)
			err := sf.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			loadTableStat, err := sf.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("table does not exists", func(t *testing.T) {
			tableName := whutils.ToProviderCase(destType, "table_not_exists_test_table")

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, false, false)

			sf := snowflake.New(config.New(), logger.NOP, stats.NOP)
			err := sf.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = sf.CreateSchema(ctx)
			require.NoError(t, err)

			loadTableStat, err := sf.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("merge", func(t *testing.T) {
			tableName := whutils.ToProviderCase(destType, "merge_test_table")

			t.Run("without dedup", func(t *testing.T) {
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, true, false)

				appendWarehouse := th.Clone(t, warehouse)
				appendWarehouse.Destination.Config[model.PreferAppendSetting.String()] = true

				sf := snowflake.New(config.New(), logger.NOP, stats.NOP)
				err := sf.Setup(ctx, appendWarehouse, mockUploader)
				require.NoError(t, err)

				err = sf.CreateSchema(ctx)
				require.NoError(t, err)

				err = sf.CreateTable(ctx, tableName, schemaInWarehouse)
				require.NoError(t, err)

				loadTableStat, err := sf.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(14))
				require.Equal(t, loadTableStat.RowsUpdated, int64(0))

				loadTableStat, err = sf.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(0),
					"2nd copy on the same table with the same data should not have any 'rows_loaded'")
				require.Equal(t, loadTableStat.RowsUpdated, int64(0),
					"2nd copy on the same table with the same data should not have any 'rows_loaded'")

				records := whth.RetrieveRecordsFromWarehouse(t, sf.DB.DB,
					fmt.Sprintf(
						`SELECT
						  id,
						  received_at,
						  test_bool,
						  test_datetime,
						  test_float,
						  test_int,
						  test_string
						FROM %q.%q
						ORDER BY id;`,
						namespace,
						tableName,
					),
				)
				require.Equal(t, whth.SampleTestRecords(), records)
			})
			t.Run("with dedup use new record", func(t *testing.T) {
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/dedup.csv.gz", tableName)

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, false, true)

				sf := snowflake.New(config.New(), logger.NOP, stats.NOP)
				err := sf.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				err = sf.CreateSchema(ctx)
				require.NoError(t, err)

				err = sf.CreateTable(ctx, tableName, schemaInWarehouse)
				require.NoError(t, err)

				loadTableStat, err := sf.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(0))
				require.Equal(t, loadTableStat.RowsUpdated, int64(14))

				records := whth.RetrieveRecordsFromWarehouse(t, db,
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
			t.Run("dedup window", func(t *testing.T) {
				tableName := whutils.ToProviderCase(destType, "merge_test_window_table")

				schema := model.TableSchema{
					"ID":          "string",
					"RECEIVED_AT": "datetime",
				}

				now := time.Now()

				uploadOutput := whth.UploadLoad(t, fm, tableName, [][]string{
					// {"id", "received_at"},
					{"1", now.Format(time.RFC3339)},
					{"2", now.Add(-1 * time.Hour).Format(time.RFC3339)},
					{"3", now.Add(-25 * time.Hour).Format(time.RFC3339)},
					{"4", now.Add(-25 * time.Hour).Format(time.RFC3339)},
				})

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schema, schema, true, false)

				c := config.New()
				c.Set("Warehouse.snowflake.mergeWindow."+warehouse.Destination.ID+".tables", tableName)
				c.Set("Warehouse.snowflake.mergeWindow."+warehouse.Destination.ID+".column", "RECEIVED_AT")
				c.Set("Warehouse.snowflake.mergeWindow."+warehouse.Destination.ID+".duration", "24h")

				sf := snowflake.New(c, logger.NOP, stats.NOP)
				err := sf.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				err = sf.CreateSchema(ctx)
				require.NoError(t, err)

				err = sf.CreateTable(ctx, tableName, schema)
				require.NoError(t, err)

				loadTableStat, err := sf.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, int64(4), loadTableStat.RowsInserted)
				require.Equal(t, int64(0), loadTableStat.RowsUpdated)

				loadTableStat, err = sf.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, int64(2), loadTableStat.RowsInserted,
					"2nd copy on the same table with the same data should not have any 'rows_loaded'")
				require.Equal(t, int64(2), loadTableStat.RowsUpdated,
					"2nd copy on the same table with the same data should not have any 'rows_updated'")

				records := whth.RetrieveRecordsFromWarehouse(t, sf.DB.DB,
					fmt.Sprintf(
						`SELECT
						  id,
						  received_at,
						FROM %q.%q
						ORDER BY id;`,
						namespace,
						tableName,
					),
				)
				require.Equal(t, [][]string{
					{"1", now.Format(time.RFC3339)},
					{"2", now.Add(-1 * time.Hour).Format(time.RFC3339)},
					{"3", now.Add(-25 * time.Hour).Format(time.RFC3339)},
					{"3", now.Add(-25 * time.Hour).Format(time.RFC3339)},
					{"4", now.Add(-25 * time.Hour).Format(time.RFC3339)},
					{"4", now.Add(-25 * time.Hour).Format(time.RFC3339)},
				}, records)
			})
		})
		t.Run("append", func(t *testing.T) {
			tableName := whutils.ToProviderCase(destType, "append_test_table")

			run := func() {
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, true, false)

				appendWarehouse := th.Clone(t, warehouse)
				appendWarehouse.Destination.Config[model.PreferAppendSetting.String()] = true

				sf := snowflake.New(config.New(), logger.NOP, stats.NOP)
				err := sf.Setup(ctx, appendWarehouse, mockUploader)
				require.NoError(t, err)

				err = sf.CreateSchema(ctx)
				require.NoError(t, err)

				err = sf.CreateTable(ctx, tableName, schemaInWarehouse)
				require.NoError(t, err)

				t.Run("loading once should copy everything", func(t *testing.T) {
					loadTableStat, err := sf.LoadTable(ctx, tableName)
					require.NoError(t, err)
					require.Equal(t, loadTableStat.RowsInserted, int64(14))
					require.Equal(t, loadTableStat.RowsUpdated, int64(0))
				})
				t.Run("loading twice should not copy anything", func(t *testing.T) {
					loadTableStat, err := sf.LoadTable(ctx, tableName)
					require.NoError(t, err)
					require.Equal(t, loadTableStat.RowsInserted, int64(0))
					require.Equal(t, loadTableStat.RowsUpdated, int64(0))
				})
			}

			run()
			run()

			records := whth.RetrieveRecordsFromWarehouse(t, db,
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
			tableName := whutils.ToProviderCase(destType, "load_file_not_exists_test_table")

			loadFiles := []whutils.LoadFile{{
				Location: "https://bucket.s3.amazonaws.com/rudder-warehouse-load-objects/load_file_not_exists_test_table/test_source_id/0ef75cb0-3fd0-4408-98b9-2bea9e476916-load_file_not_exists_test_table/load.csv.gz",
			}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, false, false)

			sf := snowflake.New(config.New(), logger.NOP, stats.NOP)
			err := sf.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = sf.CreateSchema(ctx)
			require.NoError(t, err)

			err = sf.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := sf.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("mismatch in number of columns", func(t *testing.T) {
			tableName := whutils.ToProviderCase(destType, "mismatch_columns_test_table")

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/mismatch-columns.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, false, false)

			sf := snowflake.New(config.New(), logger.NOP, stats.NOP)
			err := sf.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = sf.CreateSchema(ctx)
			require.NoError(t, err)

			err = sf.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := sf.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := whth.RetrieveRecordsFromWarehouse(t, sf.DB.DB,
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
			require.Equal(t, records, whth.SampleTestRecords())
		})
		t.Run("mismatch in schema", func(t *testing.T) {
			tableName := whutils.ToProviderCase(destType, "mismatch_schema_test_table")

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/mismatch-schema.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, false, false)

			sf := snowflake.New(config.New(), logger.NOP, stats.NOP)
			err := sf.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = sf.CreateSchema(ctx)
			require.NoError(t, err)

			err = sf.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := sf.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("discards", func(t *testing.T) {
			tableName := whutils.ToProviderCase(destType, whutils.DiscardsTable)

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/discards.csv.gz", tableName)

			discardsSchema := lo.MapKeys(whutils.DiscardsSchema, func(_, key string) string {
				return whutils.ToProviderCase(destType, key)
			})

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, discardsSchema, discardsSchema, false, false)

			sf := snowflake.New(config.New(), logger.NOP, stats.NOP)
			err := sf.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = sf.CreateSchema(ctx)
			require.NoError(t, err)

			err = sf.CreateTable(ctx, tableName, discardsSchema)
			require.NoError(t, err)

			loadTableStat, err := sf.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(6))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := whth.RetrieveRecordsFromWarehouse(t, sf.DB.DB,
				fmt.Sprintf(`
					SELECT
					  COLUMN_NAME,
					  COLUMN_VALUE,
					  REASON,
					  RECEIVED_AT,
					  ROW_ID,
					  TABLE_NAME,
					  UUID_TS
					FROM
					  %q.%q
					ORDER BY ROW_ID ASC;
					`,
					namespace,
					tableName,
				),
			)
			require.Equal(t, records, whth.DiscardTestRecords())
		})
	})

	t.Run("Delete By", func(t *testing.T) {
		ctx := context.Background()
		namespace := whth.RandSchema(destType)

		credentialsJSON, err := json.Marshal(sqlconnectconfig.Snowflake{
			Account:   credentials.Account,
			User:      credentials.User,
			Role:      credentials.Role,
			Password:  credentials.Password,
			DBName:    credentials.Database,
			Warehouse: credentials.Warehouse,
		})
		require.NoError(t, err)

		sqlConnectDB, err := sqlconnect.NewDB("snowflake", credentialsJSON)
		require.NoError(t, err)

		db := sqlConnectDB.SqlDB()
		require.NoError(t, db.Ping())
		t.Cleanup(func() { _ = db.Close() })
		t.Cleanup(func() {
			dropSchema(t, db, namespace)
		})

		conf := config.New()
		conf.Set("Warehouse.snowflake.enableDeleteByJobs", true)

		sf := snowflake.New(conf, logger.NOP, stats.NOP)
		sf.DB = sqlquerywrapper.New(db)
		sf.Namespace = namespace

		now := time.Now()

		_, err = sf.DB.ExecContext(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, namespace))
		require.NoError(t, err, "should create schema")

		_, err = sf.DB.ExecContext(ctx, "CREATE TABLE "+namespace+".TEST_TABLE (id INT, context_sources_job_run_id STRING, context_sources_task_run_id STRING, context_source_id STRING, received_at DATETIME)")
		require.NoError(t, err, "should create table")

		_, err = sf.DB.ExecContext(ctx, "INSERT INTO "+namespace+".TEST_TABLE VALUES (1, 'job_run_id_2', 'task_run_id_1_2', 'source_id_1', ?)", now.Add(-time.Hour))
		require.NoError(t, err, "should insert records")
		_, err = sf.DB.ExecContext(ctx, "INSERT INTO "+namespace+".TEST_TABLE VALUES (2, 'job_run_id_2', 'task_run_id_1', 'source_id_2', ?)", now.Add(-time.Hour))
		require.NoError(t, err, "should insert records")

		require.NoError(t, sf.DeleteBy(ctx, []string{"TEST_TABLE"}, whutils.DeleteByParams{
			SourceId:  "source_id_1",
			JobRunId:  "new_job_run_id",
			TaskRunId: "new_task_job_run_id",
			StartTime: now,
		}), "should delete records")

		rows, err := sf.DB.QueryContext(ctx, "SELECT id FROM "+namespace+".TEST_TABLE")
		require.NoError(t, err, "should see a successful query for ids")

		var recordIDs []int
		for rows.Next() {
			var id int
			err := rows.Scan(&id)
			require.NoError(t, err, "should scan rows")

			recordIDs = append(recordIDs, id)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []int{2}, recordIDs, "got the correct set of ids after deletion")

		require.NoError(t, sf.DeleteBy(ctx, []string{"TEST_TABLE"}, whutils.DeleteByParams{
			SourceId:  "source_id_2",
			JobRunId:  "new_job_run_id",
			TaskRunId: "new_task_job_run_id",
			StartTime: time.Time{},
		}), "delete should succeed even if start time is zero value - no records must be deleted")

		rows, err = sf.DB.QueryContext(ctx, "SELECT id FROM "+namespace+".TEST_TABLE")
		require.NoError(t, err, "should see a successful query for ids")

		var ids1 []int
		for rows.Next() {
			var id int
			err := rows.Scan(&id)
			require.NoError(t, err, "should scan rows")

			ids1 = append(ids1, id)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []int{2}, ids1, "got the same set of ids after deletion")

		require.NoError(t, sf.DeleteBy(ctx, []string{"TEST_TABLE"}, whutils.DeleteByParams{
			SourceId:  "source_id_2",
			JobRunId:  "new_job_run_id",
			TaskRunId: "new_task_job_run_id",
			StartTime: now,
		}), "should delete records")

		rows, err = sf.DB.QueryContext(ctx, "SELECT id FROM "+namespace+".TEST_TABLE")
		require.NoError(t, err, "should see a successful query for ids")
		var ids2 []int
		for rows.Next() {
			var id int
			err := rows.Scan(&id)
			require.NoError(t, err, "should scan rows")

			ids2 = append(ids2, id)
		}
		require.NoError(t, rows.Err())
		require.Empty(t, ids2, "no more rows left")
	})
}

func TestSnowflake_ShouldMerge(t *testing.T) {
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
			expected:          true,
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
			conf := config.New()
			conf.Set("Warehouse.snowflake.appendOnlyTables", tc.appendOnlyTables)

			sf := snowflake.New(conf, logger.NOP, stats.NOP)
			sf.Warehouse = model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						model.PreferAppendSetting.String(): tc.preferAppend,
					},
				},
			}

			mockCtrl := gomock.NewController(t)
			uploader := mockuploader.NewMockUploader(mockCtrl)
			uploader.EXPECT().CanAppend().AnyTimes().Return(tc.uploaderCanAppend)

			sf.Uploader = uploader
			require.Equal(t, sf.ShouldMerge(tc.tableName), tc.expected)
		})
	}
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

func newMockUploader(
	t testing.TB,
	loadFiles []whutils.LoadFile,
	tableName string,
	schemaInUpload model.TableSchema,
	schemaInWarehouse model.TableSchema,
	canAppend bool,
	dedupUseNewRecord bool,
) whutils.Uploader {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockUploader := mockuploader.NewMockUploader(ctrl)
	mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()
	mockUploader.EXPECT().CanAppend().Return(canAppend).AnyTimes()
	mockUploader.EXPECT().ShouldOnDedupUseNewRecord().Return(dedupUseNewRecord).AnyTimes()
	mockUploader.EXPECT().GetLoadFilesMetadata(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, options whutils.GetLoadFilesOptions) ([]whutils.LoadFile, error) {
			return slices.Clone(loadFiles), nil
		},
	).AnyTimes()
	mockUploader.EXPECT().GetSampleLoadFileLocation(gomock.Any(), gomock.Any()).Return(loadFiles[0].Location, nil).AnyTimes()
	mockUploader.EXPECT().GetTableSchemaInUpload(tableName).Return(schemaInUpload).AnyTimes()
	mockUploader.EXPECT().GetTableSchemaInWarehouse(tableName).Return(schemaInWarehouse).AnyTimes()
	mockUploader.EXPECT().GetLoadFileType().Return(whutils.LoadFileTypeCsv).AnyTimes()

	return mockUploader
}
