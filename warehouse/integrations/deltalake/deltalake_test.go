package deltalake_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	"go.uber.org/mock/gomock"

	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	th "github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseclient "github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/deltalake"
	whth "github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

type testCredentials struct {
	Host          string `json:"host"`
	Port          string `json:"port"`
	Path          string `json:"path"`
	Token         string `json:"token"`
	AccountName   string `json:"accountName"`
	AccountKey    string `json:"accountKey"`
	ContainerName string `json:"containerName"`
}

const testKey = "DATABRICKS_INTEGRATION_TEST_CREDENTIALS"

func deltaLakeTestCredentials() (*testCredentials, error) {
	cred, exists := os.LookupEnv(testKey)
	if !exists {
		return nil, errors.New("deltaLake test credentials not found")
	}

	var credentials testCredentials
	err := json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal deltaLake test credentials: %w", err)
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

	destType := whutils.DELTALAKE

	credentials, err := deltaLakeTestCredentials()
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

		expectedSchema := model.Schema{
			"screens":       {"context_source_id": "string", "event_date": "date", "user_id": "string", "sent_at": "timestamp", "context_request_ip": "string", "original_timestamp": "timestamp", "url": "string", "context_source_type": "string", "_between": "string", "timestamp": "timestamp", "context_ip": "string", "context_destination_type": "string", "received_at": "timestamp", "title": "string", "uuid_ts": "timestamp", "context_destination_id": "string", "name": "string", "id": "string", "_as": "string"},
			"identifies":    {"context_ip": "string", "event_date": "date", "context_destination_id": "string", "email": "string", "context_request_ip": "string", "sent_at": "timestamp", "uuid_ts": "timestamp", "_as": "string", "logins": "bigint", "context_source_type": "string", "context_traits_logins": "bigint", "name": "string", "context_destination_type": "string", "_between": "string", "id": "string", "timestamp": "timestamp", "received_at": "timestamp", "user_id": "string", "context_traits_email": "string", "context_traits_as": "string", "context_traits_name": "string", "original_timestamp": "timestamp", "context_traits_between": "string", "context_source_id": "string"},
			"users":         {"context_traits_name": "string", "event_date": "date", "context_traits_between": "string", "context_request_ip": "string", "context_traits_logins": "bigint", "context_destination_id": "string", "email": "string", "logins": "bigint", "_as": "string", "context_source_id": "string", "uuid_ts": "timestamp", "context_source_type": "string", "context_traits_email": "string", "name": "string", "id": "string", "_between": "string", "context_ip": "string", "received_at": "timestamp", "sent_at": "timestamp", "context_traits_as": "string", "context_destination_type": "string", "timestamp": "timestamp", "original_timestamp": "timestamp"},
			"product_track": {"review_id": "string", "event_date": "date", "context_source_id": "string", "user_id": "string", "timestamp": "timestamp", "uuid_ts": "timestamp", "review_body": "string", "context_source_type": "string", "_as": "string", "_between": "string", "id": "string", "rating": "bigint", "event": "string", "original_timestamp": "timestamp", "context_destination_type": "string", "context_ip": "string", "context_destination_id": "string", "sent_at": "timestamp", "received_at": "timestamp", "event_text": "string", "product_id": "string", "context_request_ip": "string"},
			"tracks":        {"original_timestamp": "timestamp", "event_date": "date", "context_destination_id": "string", "event": "string", "context_request_ip": "string", "uuid_ts": "timestamp", "context_destination_type": "string", "user_id": "string", "sent_at": "timestamp", "context_source_type": "string", "context_ip": "string", "timestamp": "timestamp", "received_at": "timestamp", "context_source_id": "string", "event_text": "string", "id": "string"},
			"aliases":       {"context_request_ip": "string", "event_date": "date", "context_destination_type": "string", "context_destination_id": "string", "previous_id": "string", "context_ip": "string", "sent_at": "timestamp", "id": "string", "uuid_ts": "timestamp", "timestamp": "timestamp", "original_timestamp": "timestamp", "context_source_id": "string", "user_id": "string", "context_source_type": "string", "received_at": "timestamp"},
			"pages":         {"name": "string", "url": "string", "event_date": "date", "id": "string", "timestamp": "timestamp", "title": "string", "user_id": "string", "context_source_id": "string", "context_source_type": "string", "original_timestamp": "timestamp", "context_request_ip": "string", "received_at": "timestamp", "_between": "string", "context_destination_type": "string", "uuid_ts": "timestamp", "context_destination_id": "string", "sent_at": "timestamp", "context_ip": "string", "_as": "string"},
			"groups":        {"_as": "string", "user_id": "string", "event_date": "date", "context_destination_type": "string", "sent_at": "timestamp", "context_source_type": "string", "received_at": "timestamp", "context_ip": "string", "industry": "string", "timestamp": "timestamp", "group_id": "string", "uuid_ts": "timestamp", "context_source_id": "string", "context_request_ip": "string", "_between": "string", "original_timestamp": "timestamp", "name": "string", "plan": "string", "context_destination_id": "string", "employees": "bigint", "id": "string"},
		}
		schemaFor := func(db *sql.DB, namespace, tableName string) model.TableSchema {
			tableSchema := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`DESCRIBE QUERY TABLE %s.%s;`, namespace, tableName))
			return lo.SliceToMap(tableSchema, func(col []string) (string, string) {
				return col[0], col[1]
			})
		}
		verifySchema := func(t *testing.T, db *sql.DB, namespace string) {
			t.Helper()
			require.Equal(t, expectedSchema["screens"], schemaFor(db, namespace, "screens"))
			require.Equal(t, expectedSchema["identifies"], schemaFor(db, namespace, "identifies"))
			require.Equal(t, expectedSchema["users"], schemaFor(db, namespace, "users"))
			require.Equal(t, expectedSchema["product_track"], schemaFor(db, namespace, "product_track"))
			require.Equal(t, expectedSchema["tracks"], schemaFor(db, namespace, "tracks"))
			require.Equal(t, expectedSchema["aliases"], schemaFor(db, namespace, "aliases"))
			require.Equal(t, expectedSchema["pages"], schemaFor(db, namespace, "pages"))
			require.Equal(t, expectedSchema["groups"], schemaFor(db, namespace, "groups"))
		}
		userIDFormat := "userId_deltalake"
		userIDSQL := "SUBSTRING(user_id, 1, 16)"
		uuidTSSQL := "DATE_FORMAT(uuid_ts, 'yyyy-MM-dd')"

		testCases := []struct {
			name                           string
			warehouseEventsMap2            whth.EventsCountMap
			useParquetLoadFiles            bool
			configOverride                 map[string]any
			eventFilePath1, eventFilePath2 string
			useSameUserID                  bool
			verifyRecords                  func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace string)
		}{
			{
				name:           "Merge Mode",
				eventFilePath1: "../testdata/upload-job.events-1.json",
				eventFilePath2: "../testdata/upload-job.events-1.json",
				useSameUserID:  true,
				configOverride: map[string]any{
					"preferAppend": false,
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace string) {
					t.Helper()
					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesMergeRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, substring(id, 1, 16), %s, received_at, name, original_timestamp FROM %s.%s ORDER BY id;`, uuidTSSQL, namespace, "users"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersMergeRecord(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %s.%s ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksMergeRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "product_track"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackMergeRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "pages"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesMergeRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, _as FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "screens"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensMergeRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "aliases"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesMergeRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %s.%s ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "groups"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsMergeRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
			{
				name: "Append Mode",
				warehouseEventsMap2: whth.EventsCountMap{
					// For all tables we will be appending because of preferAppend config
					"identifies": 8, "users": 2, "tracks": 8, "product_track": 8, "pages": 8, "screens": 8, "aliases": 8, "groups": 8,
				},
				eventFilePath1: "../testdata/upload-job.events-1.json",
				eventFilePath2: "../testdata/upload-job.events-1.json",
				useSameUserID:  true,
				configOverride: map[string]any{
					"preferAppend": true,
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace string) {
					t.Helper()
					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, substring(id, 1, 16), %s, received_at, name, original_timestamp FROM %s.%s ORDER BY id;`, uuidTSSQL, namespace, "users"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersAppendRecords(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %s.%s ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksAppendRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "product_track"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackAppendRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "pages"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, _as FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "screens"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensAppendRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "aliases"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesAppendRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %s.%s ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "groups"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsAppendRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
			{
				name:           "Undefined preferAppend",
				eventFilePath1: "../testdata/upload-job.events-1.json",
				eventFilePath2: "../testdata/upload-job.events-1.json",
				useSameUserID:  true,
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace string) {
					t.Helper()
					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesMergeRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, substring(id, 1, 16), %s, received_at, name, original_timestamp FROM %s.%s ORDER BY id;`, uuidTSSQL, namespace, "users"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersMergeRecord(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %s.%s ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksMergeRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "product_track"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackMergeRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "pages"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesMergeRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, _as FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "screens"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensMergeRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "aliases"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesMergeRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %s.%s ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "groups"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsMergeRecords(userIDFormat, sourceID, destinationID, destType))
				},
			},
			{
				name:                "Parquet load files(merge)",
				eventFilePath1:      "../testdata/upload-job.events-1.json",
				eventFilePath2:      "../testdata/upload-job.events-1.json",
				useSameUserID:       true,
				useParquetLoadFiles: true,
				configOverride: map[string]any{
					"preferAppend": false,
				},
				verifyRecords: func(t *testing.T, db *sql.DB, sourceID, destinationID, namespace string) {
					t.Helper()
					identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, %s, context_traits_logins, _as, name, logins, email, original_timestamp, context_ip, context_traits_as, timestamp, received_at, context_destination_type, sent_at, context_source_type, context_traits_between, context_source_id, context_traits_name, context_request_ip, _between, context_traits_email, context_destination_id, id FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "identifies"))
					require.ElementsMatch(t, identifiesRecords, whth.UploadJobIdentifiesMergeRecords(userIDFormat, sourceID, destinationID, destType))
					usersRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_type, context_request_ip, context_traits_name, context_traits_between, _as, logins, sent_at, context_traits_logins, context_ip, _between, context_traits_email, timestamp, context_destination_id, email, context_traits_as, context_source_type, substring(id, 1, 16), %s, received_at, name, original_timestamp FROM %s.%s ORDER BY id;`, uuidTSSQL, namespace, "users"))
					require.ElementsMatch(t, usersRecords, whth.UploadJobUsersMergeRecord(userIDFormat, sourceID, destinationID, destType))
					tracksRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT original_timestamp, context_destination_id, context_destination_type, %s, context_source_type, timestamp, id, event, sent_at, context_ip, event_text, context_source_id, context_request_ip, received_at, %s FROM %s.%s ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "tracks"))
					require.ElementsMatch(t, tracksRecords, whth.UploadJobTracksMergeRecords(userIDFormat, sourceID, destinationID, destType))
					productTrackRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT timestamp, %s, product_id, received_at, context_source_id, sent_at, context_source_type, context_ip, context_destination_type, original_timestamp, context_request_ip, context_destination_id, %s, _as, review_body, _between, review_id, event_text, id, event, rating FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "product_track"))
					require.ElementsMatch(t, productTrackRecords, whth.UploadJobProductTrackMergeRecords(userIDFormat, sourceID, destinationID, destType))
					pagesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT %s, context_source_id, id, title, timestamp, context_source_type, _as, received_at, context_destination_id, context_ip, context_destination_type, name, original_timestamp, _between, context_request_ip, sent_at, url, %s FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "pages"))
					require.ElementsMatch(t, pagesRecords, whth.UploadJobPagesMergeRecords(userIDFormat, sourceID, destinationID, destType))
					screensRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, url, context_source_type, title, original_timestamp, %s, _between, context_ip, name, context_request_ip, %s, context_source_id, id, received_at, context_destination_id, timestamp, sent_at, _as FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "screens"))
					require.ElementsMatch(t, screensRecords, whth.UploadJobScreensMergeRecords(userIDFormat, sourceID, destinationID, destType))
					aliasesRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_source_id, context_destination_id, context_ip, sent_at, id, %s, %s, previous_id, original_timestamp, context_source_type, received_at, context_destination_type, context_request_ip, timestamp FROM %s.%s ORDER BY id;`, userIDSQL, uuidTSSQL, namespace, "aliases"))
					require.ElementsMatch(t, aliasesRecords, whth.UploadJobAliasesMergeRecords(userIDFormat, sourceID, destinationID, destType))
					groupsRecords := whth.RetrieveRecordsFromWarehouse(t, db, fmt.Sprintf(`SELECT context_destination_type, id, _between, plan, original_timestamp, %s, context_source_id, sent_at, %s, group_id, industry, context_request_ip, context_source_type, timestamp, employees, _as, context_destination_id, received_at, name, context_ip FROM %s.%s ORDER BY id;`, uuidTSSQL, userIDSQL, namespace, "groups"))
					require.ElementsMatch(t, groupsRecords, whth.UploadJobGroupsMergeRecords(userIDFormat, sourceID, destinationID, destType))
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
					WithConfigOption("host", credentials.Host).
					WithConfigOption("port", credentials.Port).
					WithConfigOption("path", credentials.Path).
					WithConfigOption("token", credentials.Token).
					WithConfigOption("namespace", namespace).
					WithConfigOption("bucketProvider", "AZURE_BLOB").
					WithConfigOption("containerName", credentials.ContainerName).
					WithConfigOption("useSTSTokens", false).
					WithConfigOption("enableSSE", false).
					WithConfigOption("accountName", credentials.AccountName).
					WithConfigOption("accountKey", credentials.AccountKey).
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

				t.Setenv("RSERVER_WAREHOUSE_DELTALAKE_MAX_PARALLEL_LOADS", "8")
				t.Setenv("RSERVER_WAREHOUSE_DELTALAKE_SLOW_QUERY_THRESHOLD", "0s")
				t.Setenv("RSERVER_WAREHOUSE_DELTALAKE_USE_PARQUET_LOAD_FILES", strconv.FormatBool(tc.useParquetLoadFiles))

				whth.BootstrapSvc(t, workspaceConfig, httpPort, jobsDBPort)

				port, err := strconv.Atoi(credentials.Port)
				require.NoError(t, err)

				connector, err := dbsql.NewConnector(
					dbsql.WithServerHostname(credentials.Host),
					dbsql.WithPort(port),
					dbsql.WithHTTPPath(credentials.Path),
					dbsql.WithAccessToken(credentials.Token),
					dbsql.WithSessionParams(map[string]string{
						"ansi_mode": "false",
					}),
				)
				require.NoError(t, err)

				db := sql.OpenDB(connector)
				require.NoError(t, db.Ping())
				t.Cleanup(func() { _ = db.Close() })
				t.Cleanup(func() {
					dropSchema(t, db, namespace)
				})

				sqlClient := &warehouseclient.Client{
					SQL:  db,
					Type: warehouseclient.SQLClient,
				}

				conf := map[string]interface{}{
					"bucketProvider": "AZURE_BLOB",
					"containerName":  credentials.ContainerName,
					"prefix":         "",
					"useSTSTokens":   false,
					"enableSSE":      false,
					"accountName":    credentials.AccountName,
					"accountKey":     credentials.AccountKey,
				}
				tables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}

				t.Log("verifying test case 1")
				ts1 := whth.TestConfig{
					WriteKey:        writeKey,
					Schema:          namespace,
					Tables:          tables,
					SourceID:        sourceID,
					DestinationID:   destinationID,
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
					WriteKey:           writeKey,
					Schema:             namespace,
					Tables:             tables,
					SourceID:           sourceID,
					DestinationID:      destinationID,
					WarehouseEventsMap: tc.warehouseEventsMap2,
					Config:             conf,
					WorkspaceID:        workspaceID,
					DestinationType:    destType,
					JobsDB:             jobsDB,
					HTTPPort:           httpPort,
					Client:             sqlClient,
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
				verifySchema(t, db, namespace)

				t.Log("verifying records")
				tc.verifyRecords(t, db, sourceID, destinationID, namespace)
			})
		}
	})

	t.Run("Validation", func(t *testing.T) {
		namespace := whth.RandSchema(destType)

		port, err := strconv.Atoi(credentials.Port)
		require.NoError(t, err)

		connector, err := dbsql.NewConnector(
			dbsql.WithServerHostname(credentials.Host),
			dbsql.WithPort(port),
			dbsql.WithHTTPPath(credentials.Path),
			dbsql.WithAccessToken(credentials.Token),
			dbsql.WithSessionParams(map[string]string{
				"ansi_mode": "false",
			}),
		)
		require.NoError(t, err)

		db := sql.OpenDB(connector)
		require.NoError(t, db.Ping())
		t.Cleanup(func() { _ = db.Close() })
		t.Cleanup(func() {
			dropSchema(t, db, namespace)
		})

		dest := backendconfig.DestinationT{
			ID: "test_destination_id",
			Config: map[string]interface{}{
				"host":            credentials.Host,
				"port":            credentials.Port,
				"path":            credentials.Path,
				"token":           credentials.Token,
				"namespace":       namespace,
				"bucketProvider":  "AZURE_BLOB",
				"containerName":   credentials.ContainerName,
				"prefix":          "",
				"useSTSTokens":    false,
				"enableSSE":       false,
				"accountName":     credentials.AccountName,
				"accountKey":      credentials.AccountKey,
				"syncFrequency":   "30",
				"eventDelivery":   false,
				"eventDeliveryTS": 1648195480174,
			},
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				ID:          "23HLpnDJnIg7DsBvDWGU6DQzFEo",
				Name:        "DELTALAKE",
				DisplayName: "Databricks (Delta Lake)",
			},
			Name:       "deltalake-demo",
			Enabled:    true,
			RevisionID: "39eClxJQQlaWzMWyqnQctFDP5T2",
		}

		testCases := []struct {
			name                string
			useParquetLoadFiles bool
			conf                map[string]interface{}
		}{
			{
				name:                "Parquet load files",
				useParquetLoadFiles: true,
			},
			{
				name:                "CSV load files",
				useParquetLoadFiles: false,
			},
			{
				name:                "External location",
				useParquetLoadFiles: true,
				conf: map[string]interface{}{
					"enableExternalLocation": true,
					"externalLocation":       "/path/to/delta/table",
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Setenv(
					"RSERVER_WAREHOUSE_DELTALAKE_USE_PARQUET_LOAD_FILES",
					strconv.FormatBool(tc.useParquetLoadFiles),
				)

				for k, v := range tc.conf {
					dest.Config[k] = v
				}

				whth.VerifyConfigurationTest(t, dest)
			})
		}
	})

	t.Run("Load Table", func(t *testing.T) {
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
					"host":           credentials.Host,
					"port":           credentials.Port,
					"path":           credentials.Path,
					"token":          credentials.Token,
					"namespace":      namespace,
					"bucketProvider": whutils.AzureBlob,
					"containerName":  credentials.ContainerName,
					"accountName":    credentials.AccountName,
					"accountKey":     credentials.AccountKey,
				},
			},
			WorkspaceID: "test_workspace_id",
			Namespace:   namespace,
		}

		fm, err := filemanager.New(&filemanager.Settings{
			Provider: whutils.AzureBlob,
			Config: map[string]any{
				"containerName":  credentials.ContainerName,
				"accountName":    credentials.AccountName,
				"accountKey":     credentials.AccountKey,
				"bucketProvider": whutils.AzureBlob,
			},
		})
		require.NoError(t, err)

		t.Run("schema does not exists", func(t *testing.T) {
			tableName := "schema_not_exists_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, false, false)

			d := deltalake.New(config.New(), logger.NOP, stats.NOP)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("table does not exists", func(t *testing.T) {
			tableName := "table_not_exists_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, false, false)

			d := deltalake.New(config.New(), logger.NOP, stats.NOP)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)
			t.Cleanup(func() {
				dropSchema(t, d.DB.DB, namespace)
			})

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("merge", func(t *testing.T) {
			t.Run("without dedup", func(t *testing.T) {
				tableName := "merge_without_dedup_test_table"
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, false, false)

				d := deltalake.New(config.New(), logger.NOP, stats.NOP)
				err := d.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				err = d.CreateSchema(ctx)
				require.NoError(t, err)
				t.Cleanup(func() {
					dropSchema(t, d.DB.DB, namespace)
				})

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

				records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
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
						ORDER BY id;`,
						namespace,
						tableName,
					),
				)
				require.Equal(t, records, whth.SampleTestRecords())
			})
			t.Run("with dedup use new record", func(t *testing.T) {
				tableName := "merge_with_dedup_use_new_record_test_table"
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/dedup.csv.gz", tableName)

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, true, true)

				d := deltalake.New(config.New(), logger.NOP, stats.NOP)
				err := d.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				err = d.CreateSchema(ctx)
				require.NoError(t, err)
				t.Cleanup(func() {
					dropSchema(t, d.DB.DB, namespace)
				})

				err = d.CreateTable(ctx, tableName, schemaInWarehouse)
				require.NoError(t, err)

				loadTableStat, err := d.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(14))
				require.Equal(t, loadTableStat.RowsUpdated, int64(0))

				retrieveRecordsSQL := fmt.Sprintf(`
						SELECT
						  id,
						  received_at,
						  test_bool,
						  test_datetime,
						  test_float,
						  test_int,
						  test_string
						FROM %s.%s
						ORDER BY id;`,
					namespace,
					tableName,
				)
				records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB, retrieveRecordsSQL)
				require.Equal(t, records, whth.DedupTestRecords())

				loadTableStat, err = d.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(0))
				require.Equal(t, loadTableStat.RowsUpdated, int64(14))

				records = whth.RetrieveRecordsFromWarehouse(t, d.DB.DB, retrieveRecordsSQL)
				require.Equal(t, records, whth.DedupTestRecords())
			})
			t.Run("with no overlapping partition with preferAppend false", func(t *testing.T) {
				tableName := "merge_with_no_overlapping_partition_test_table"
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/dedup.csv.gz", tableName)

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, true, false)

				appendWarehouse := th.Clone(t, warehouse)
				appendWarehouse.Destination.Config["preferAppend"] = false

				d := deltalake.New(config.New(), logger.NOP, stats.NOP)
				err := d.Setup(ctx, appendWarehouse, mockUploader)
				require.NoError(t, err)

				err = d.CreateSchema(ctx)
				require.NoError(t, err)
				t.Cleanup(func() {
					dropSchema(t, d.DB.DB, namespace)
				})

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

				records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
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
						ORDER BY id;`,
						namespace,
						tableName,
					),
				)
				require.Equal(t, records, whth.DedupTestRecords())
			})
			t.Run("with no overlapping partition with preferAppend true", func(t *testing.T) {
				tableName := "merge_with_no_overlapping_partition_test_table"
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/dedup.csv.gz", tableName)

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, true, false)

				appendWarehouse := th.Clone(t, warehouse)
				appendWarehouse.Destination.Config["preferAppend"] = true

				d := deltalake.New(config.New(), logger.NOP, stats.NOP)
				err := d.Setup(ctx, appendWarehouse, mockUploader)
				require.NoError(t, err)

				err = d.CreateSchema(ctx)
				require.NoError(t, err)
				t.Cleanup(func() {
					dropSchema(t, d.DB.DB, namespace)
				})

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

				records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
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
						ORDER BY id;`,
						namespace,
						tableName,
					),
				)
				require.Equal(t, records, whth.DedupTwiceTestRecords())
			})
		})
		t.Run("append", func(t *testing.T) {
			tableName := "append_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, true, false)

			appendWarehouse := th.Clone(t, warehouse)
			appendWarehouse.Destination.Config[model.PreferAppendSetting.String()] = true

			d := deltalake.New(config.New(), logger.NOP, stats.NOP)
			err := d.Setup(ctx, appendWarehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)
			t.Cleanup(func() {
				dropSchema(t, d.DB.DB, namespace)
			})

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

			records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
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
					ORDER BY id;`,
					namespace,
					tableName,
				),
			)
			require.Equal(t, records, whth.AppendTestRecords())
		})
		t.Run("load file does not exists", func(t *testing.T) {
			tableName := "load_file_not_exists_test_table"

			loadFiles := []whutils.LoadFile{{
				Location: fmt.Sprintf("https://%s.blob.core.windows.net/%s/rudder-warehouse-load-objects/load_file_not_exists_test_table/test_source_id/a01af26e-4548-49ff-a895-258829cc1a83-load_file_not_exists_test_table/load.csv.gz",
					credentials.AccountName,
					credentials.ContainerName,
				),
			}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, false, false)

			d := deltalake.New(config.New(), logger.NOP, stats.NOP)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)
			t.Cleanup(func() {
				dropSchema(t, d.DB.DB, namespace)
			})

			err = d.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("mismatch in number of columns", func(t *testing.T) {
			tableName := "mismatch_columns_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/mismatch-columns.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, false, false)

			d := deltalake.New(config.New(), logger.NOP, stats.NOP)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)
			t.Cleanup(func() {
				dropSchema(t, d.DB.DB, namespace)
			})

			err = d.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
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
					ORDER BY id;`,
					namespace,
					tableName,
				),
			)
			require.Equal(t, records, whth.SampleTestRecords())
		})
		t.Run("mismatch in schema", func(t *testing.T) {
			tableName := "mismatch_schema_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/mismatch-schema.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, false, false)

			d := deltalake.New(config.New(), logger.NOP, stats.NOP)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)
			t.Cleanup(func() {
				dropSchema(t, d.DB.DB, namespace)
			})

			err = d.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
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
					ORDER BY id;`,
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
			mockUploader := newMockUploader(t, loadFiles, tableName, whutils.DiscardsSchema, whutils.DiscardsSchema, whutils.LoadFileTypeCsv, false, false)

			d := deltalake.New(config.New(), logger.NOP, stats.NOP)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)
			t.Cleanup(func() {
				dropSchema(t, d.DB.DB, namespace)
			})

			err = d.CreateTable(ctx, tableName, whutils.DiscardsSchema)
			require.NoError(t, err)

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(6))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
				fmt.Sprintf(
					`SELECT
						column_name,
						column_value,
						reason,
						received_at,
						row_id,
						table_name,
						uuid_ts
					FROM %s.%s
					ORDER BY row_id ASC;`,
					namespace,
					tableName,
				),
			)
			require.Equal(t, records, whth.DiscardTestRecords())
		})
		t.Run("parquet", func(t *testing.T) {
			tableName := "parquet_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.parquet", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeParquet, false, false)

			d := deltalake.New(config.New(), logger.NOP, stats.NOP)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)
			t.Cleanup(func() {
				dropSchema(t, d.DB.DB, namespace)
			})

			err = d.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)
			metadata := tableMetadata(t, d.DB.DB, namespace, tableName)
			require.Equal(t, "MANAGED", metadata["Type"])

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
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
					ORDER BY id;`,
					namespace,
					tableName,
				),
			)
			require.Equal(t, records, whth.SampleTestRecords())
		})
		t.Run("partition pruning", func(t *testing.T) {
			t.Run("not partitioned", func(t *testing.T) {
				tableName := "not_partitioned_test_table"

				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, false, false)

				d := deltalake.New(config.New(), logger.NOP, stats.NOP)
				err := d.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				err = d.CreateSchema(ctx)
				require.NoError(t, err)
				t.Cleanup(func() {
					dropSchema(t, d.DB.DB, namespace)
				})

				_, err = d.DB.QueryContext(ctx,
					`CREATE TABLE IF NOT EXISTS `+namespace+`.`+tableName+` (
						extra_test_bool BOOLEAN,
						extra_test_datetime TIMESTAMP,
						extra_test_float DOUBLE,
						extra_test_int BIGINT,
						extra_test_string STRING,
						id STRING,
						received_at TIMESTAMP,
						event_date DATE GENERATED ALWAYS AS (
							CAST(received_at AS DATE)
						),
						test_bool BOOLEAN,
						test_datetime TIMESTAMP,
						test_float DOUBLE,
						test_int BIGINT,
						test_string STRING
					) USING DELTA;`)
				require.NoError(t, err)

				loadTableStat, err := d.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(14))
				require.Equal(t, loadTableStat.RowsUpdated, int64(0))

				records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
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
						ORDER BY id;`,
						namespace,
						tableName,
					),
				)
				require.Equal(t, records, whth.SampleTestRecords())
			})
			t.Run("event_date is not in partition", func(t *testing.T) {
				tableName := "not_event_date_partition_test_table"

				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

				loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, false, false)

				d := deltalake.New(config.New(), logger.NOP, stats.NOP)
				err := d.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				err = d.CreateSchema(ctx)
				require.NoError(t, err)
				t.Cleanup(func() {
					dropSchema(t, d.DB.DB, namespace)
				})

				_, err = d.DB.QueryContext(ctx,
					`CREATE TABLE IF NOT EXISTS `+namespace+`.`+tableName+` (
						extra_test_bool BOOLEAN,
						extra_test_datetime TIMESTAMP,
						extra_test_float DOUBLE,
						extra_test_int BIGINT,
						extra_test_string STRING,
						id STRING,
						received_at TIMESTAMP,
						event_date DATE GENERATED ALWAYS AS (
							CAST(received_at AS DATE)
						),
						test_bool BOOLEAN,
						test_datetime TIMESTAMP,
						test_float DOUBLE,
						test_int BIGINT,
						test_string STRING
					) USING DELTA PARTITIONED BY(id);`)
				require.NoError(t, err)

				loadTableStat, err := d.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(14))
				require.Equal(t, loadTableStat.RowsUpdated, int64(0))

				records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
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
						ORDER BY id;`,
						namespace,
						tableName,
					),
				)
				require.Equal(t, records, whth.SampleTestRecords())
			})
		})
		t.Run("external tables", func(t *testing.T) {
			tableName := "external_tables"
			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, whutils.LoadFileTypeCsv, false, false)

			externalWarehouse := th.Clone(t, warehouse)
			externalWarehouse.Destination.Config["enableExternalLocation"] = true
			externalWarehouse.Destination.Config["externalLocation"] = "/path/to/delta/table"

			d := deltalake.New(config.New(), logger.NOP, stats.NOP)
			require.NoError(t, d.Setup(ctx, externalWarehouse, mockUploader))
			require.NoError(t, d.CreateSchema(ctx))
			t.Cleanup(func() {
				dropSchema(t, d.DB.DB, namespace)
			})

			require.NoError(t, d.CreateTable(ctx, tableName, schemaInWarehouse))
			metadata := tableMetadata(t, d.DB.DB, namespace, tableName)
			require.Equal(t, "EXTERNAL", metadata["Type"])
			require.Equal(t, "dbfs:/path/to/delta/table/"+namespace+"/"+tableName, metadata["Location"])

			loadTableStat, err := d.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))
		})
	})

	t.Run("Fetch Schema", func(t *testing.T) {
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
					"host":           credentials.Host,
					"port":           credentials.Port,
					"path":           credentials.Path,
					"token":          credentials.Token,
					"namespace":      namespace,
					"bucketProvider": whutils.AzureBlob,
					"containerName":  credentials.ContainerName,
					"accountName":    credentials.AccountName,
					"accountKey":     credentials.AccountKey,
				},
			},
			WorkspaceID: "test_workspace_id",
			Namespace:   namespace,
		}

		t.Run("create schema if not exists", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockUploader := mockuploader.NewMockUploader(ctrl)
			mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()

			d := deltalake.New(config.New(), logger.NOP, stats.NOP)
			err := d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			var schema string
			err = d.DB.QueryRowContext(ctx, fmt.Sprintf(`SHOW SCHEMAS LIKE '%s';`, d.Namespace)).Scan(&schema)
			require.ErrorIs(t, err, sql.ErrNoRows)
			require.Empty(t, schema)

			warehouseSchema, err := d.FetchSchema(ctx)
			require.NoError(t, err)
			require.Empty(t, warehouseSchema)
			t.Cleanup(func() {
				dropSchema(t, d.DB.DB, namespace)
			})

			err = d.DB.QueryRowContext(ctx, fmt.Sprintf(`SHOW SCHEMAS LIKE '%s';`, d.Namespace)).Scan(&schema)
			require.NoError(t, err)
			require.Equal(t, schema, d.Namespace)
		})

		t.Run("schema already exists with some missing datatype", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockUploader := mockuploader.NewMockUploader(ctrl)
			mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()

			statsStore, err := memstats.New()
			require.NoError(t, err)

			d := deltalake.New(config.New(), logger.NOP, statsStore)
			err = d.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = d.CreateSchema(ctx)
			require.NoError(t, err)
			t.Cleanup(func() {
				dropSchema(t, d.DB.DB, namespace)
			})

			_, err = d.DB.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s.test_table (c1 bigint, c2 binary, c3 boolean, c4 date, c5 decimal(10,2), c6 double, c7 float, c8 int, c9 void, c10 smallint, c11 string, c12 timestamp, c13 timestamp_ntz, c14 tinyint, c15 array<int>, c16 map<timestamp, int>, c17 struct<Field1:timestamp,Field2:int>, received_at timestamp, event_date date GENERATED ALWAYS AS (CAST(received_at AS DATE)));`, d.Namespace))
			require.NoError(t, err)
			_, err = d.DB.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s.rudder_staging_123 (c1 string, c2 int);`, d.Namespace))
			require.NoError(t, err)

			warehouseSchema, err := d.FetchSchema(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, warehouseSchema)
			require.Contains(t, warehouseSchema, "test_table")
			require.NotContains(t, warehouseSchema["test_table"], "event_date")
			require.NotContains(t, warehouseSchema, "rudder_staging_123")

			require.Equal(t, model.TableSchema{
				"c1":          "int",
				"c11":         "string",
				"c4":          "date",
				"c14":         "int",
				"c3":          "boolean",
				"received_at": "datetime",
				"c7":          "float",
				"c12":         "datetime",
				"c10":         "int",
				"c6":          "float",
				"c8":          "int",
			},
				warehouseSchema["test_table"],
			)

			missingDatatypeStats := []string{"void", "timestamp_ntz", "struct", "array", "binary", "map", "decimal(10,2)"}
			for _, missingDatatype := range missingDatatypeStats {
				require.EqualValues(t, 1, statsStore.Get(whutils.RudderMissingDatatype, stats.Tags{
					"module":      "warehouse",
					"destType":    warehouse.Type,
					"workspaceId": warehouse.WorkspaceID,
					"destID":      warehouse.Destination.ID,
					"sourceID":    warehouse.Source.ID,
					"datatype":    missingDatatype,
				}).LastValue())
			}
		})
	})

	t.Run("Add columns to table", func(t *testing.T) {
		tableName := "test_add_existing_columns"
		namespace := whth.RandSchema(destType)

		loadFiles := []whutils.LoadFile{{Location: "dummy_location"}}
		mockUploader := newMockUploader(t, loadFiles, tableName, nil, nil, whutils.LoadFileTypeCsv, false, false)
		ctx := context.Background()

		d := deltalake.New(config.New(), logger.NOP, stats.NOP)
		warehouse := model.Warehouse{
			Destination: backendconfig.DestinationT{
				ID: "test_destination_id",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: destType,
				},
				Config: map[string]any{
					"host":           credentials.Host,
					"port":           credentials.Port,
					"path":           credentials.Path,
					"token":          credentials.Token,
					"namespace":      namespace,
					"bucketProvider": whutils.AzureBlob,
					"containerName":  credentials.ContainerName,
					"accountName":    credentials.AccountName,
					"accountKey":     credentials.AccountKey,
				},
			},
			Namespace: namespace,
		}
		err := d.Setup(ctx, warehouse, mockUploader)
		require.NoError(t, err)

		err = d.CreateSchema(ctx)
		require.NoError(t, err)
		t.Cleanup(func() {
			dropSchema(t, d.DB.DB, d.Namespace)
		})
		columns := map[string]string{
			"col1": "int",
			"col2": "string",
		}

		err = d.CreateTable(ctx, tableName, columns)
		require.NoError(t, err)

		columnsToAdd := []whutils.ColumnInfo{
			{
				Name: "col1",
				Type: "int",
			},
			{
				Name: "col3",
				Type: "int",
			},
		}

		require.NoError(t, d.AddColumns(ctx, tableName, columnsToAdd))
		require.NoError(t, d.AddColumns(ctx, tableName, columnsToAdd))

		schema, err := d.FetchSchema(ctx)
		require.NoError(t, err)
		require.Contains(t, schema, tableName)
		require.Equal(t, model.TableSchema{
			"col1": "int",
			"col2": "string",
			"col3": "int",
		}, schema[tableName])
	})
}

func tableMetadata(t *testing.T, db *sql.DB, namespace, tableName string) map[string]string {
	t.Helper()
	t.Log("table metadata for", namespace, tableName)

	var database, table, isTemporary, information string
	err := db.QueryRowContext(context.Background(), fmt.Sprintf("SHOW TABLE EXTENDED IN %s LIKE '%s'", namespace, tableName)).Scan(&database, &table, &isTemporary, &information)
	require.NoError(t, err)

	metadataMap := make(map[string]string)
	for _, line := range strings.Split(information, "\n") {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) >= 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			metadataMap[key] = value
		}
	}
	return metadataMap
}

func dropSchema(t *testing.T, db *sql.DB, namespace string) {
	t.Helper()
	t.Log("dropping schema", namespace)

	require.Eventually(t,
		func() bool {
			_, err := db.ExecContext(context.Background(), fmt.Sprintf(`DROP SCHEMA %s CASCADE;`, namespace))
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

func TestDeltalake_TrimErrorMessage(t *testing.T) {
	tempError := errors.New("temp error")

	testCases := []struct {
		name          string
		inputError    error
		expectedError error
	}{
		{
			name:          "error message is above max length",
			inputError:    errors.New(strings.Repeat(tempError.Error(), 100)),
			expectedError: errors.New(strings.Repeat(tempError.Error(), 25)),
		},
		{
			name:          "error message is below max length",
			inputError:    errors.New(strings.Repeat(tempError.Error(), 25)),
			expectedError: errors.New(strings.Repeat(tempError.Error(), 25)),
		},
		{
			name:          "error message is equal to max length",
			inputError:    errors.New(strings.Repeat(tempError.Error(), 10)),
			expectedError: errors.New(strings.Repeat(tempError.Error(), 10)),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := config.New()
			c.Set("Warehouse.deltalake.maxErrorLength", len(tempError.Error())*25)

			d := deltalake.New(c, logger.NOP, stats.NOP)
			require.Equal(t, tc.expectedError, d.TrimErrorMessage(tc.inputError))
		})
	}
}

func TestDeltalake_ShouldMerge(t *testing.T) {
	testCases := []struct {
		name                  string
		preferAppend          bool
		uploaderCanAppend     bool
		uploaderExpectedCalls int
		expected              bool
	}{
		{
			name:                  "uploader says we can append and user prefers to append",
			preferAppend:          true,
			uploaderCanAppend:     true,
			uploaderExpectedCalls: 1,
			expected:              false,
		},
		{
			name:                  "uploader says we can append and users prefers not to append",
			preferAppend:          false,
			uploaderCanAppend:     true,
			uploaderExpectedCalls: 1,
			expected:              true,
		},
		{
			name:                  "uploader says we cannot append and user prefers to append",
			preferAppend:          true,
			uploaderCanAppend:     false,
			uploaderExpectedCalls: 1,
			expected:              true,
		},
		{
			name:                  "uploader says we cannot append and users prefers not to append",
			preferAppend:          false,
			uploaderCanAppend:     false,
			uploaderExpectedCalls: 1,
			expected:              true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			d := deltalake.New(config.New(), logger.NOP, stats.NOP)
			d.Warehouse = model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						model.PreferAppendSetting.String(): tc.preferAppend,
					},
				},
			}

			mockCtrl := gomock.NewController(t)
			uploader := mockuploader.NewMockUploader(mockCtrl)
			uploader.EXPECT().CanAppend().Times(tc.uploaderExpectedCalls).Return(tc.uploaderCanAppend)

			d.Uploader = uploader
			require.Equal(t, d.ShouldMerge(), tc.expected)
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
	canAppend bool,
	onDedupUseNewRecords bool,
) whutils.Uploader {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockUploader := mockuploader.NewMockUploader(ctrl)
	mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()
	mockUploader.EXPECT().ShouldOnDedupUseNewRecord().Return(onDedupUseNewRecords).AnyTimes()
	mockUploader.EXPECT().CanAppend().Return(canAppend).AnyTimes()
	mockUploader.EXPECT().GetLoadFilesMetadata(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, options whutils.GetLoadFilesOptions) ([]whutils.LoadFile, error) {
			return slices.Clone(loadFiles), nil
		},
	).AnyTimes()
	mockUploader.EXPECT().GetSampleLoadFileLocation(gomock.Any(), gomock.Any()).Return(loadFiles[0].Location, nil).AnyTimes()
	mockUploader.EXPECT().GetTableSchemaInUpload(tableName).Return(schemaInUpload).AnyTimes()
	mockUploader.EXPECT().GetTableSchemaInWarehouse(tableName).Return(schemaInWarehouse).AnyTimes()
	mockUploader.EXPECT().GetLoadFileType().Return(loadFileType).AnyTimes()

	return mockUploader
}
