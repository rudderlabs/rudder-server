package postgres_test

import (
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/timeutil"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var statsToVerify = []string{"pg_rollback_timeout"}

func TestPostgresIntegration(t *testing.T) {
	if os.Getenv("SLOW") == "0" {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}

	t.Parallel()

	postgres.Init()

	db, err := postgres.Connect(postgres.CredentialsT{
		DBName:   "rudderdb",
		Password: "rudder-password",
		User:     "rudder",
		Host:     "wh-postgres",
		SSLMode:  "disable",
		Port:     "5432",
	})
	require.NoError(t, err)

	err = db.Ping()
	require.NoError(t, err)

	jobsDB := testhelper.SetUpJobsDB(t)

	testcase := []struct {
		name                  string
		writeKey              string
		schema                string
		tables                []string
		sourceID              string
		destinationID         string
		skipUserCreation      bool
		eventsMap             testhelper.EventsCountMap
		stagingFilesEventsMap testhelper.EventsCountMap
		loadFilesEventsMap    testhelper.EventsCountMap
		tableUploadsEventsMap testhelper.EventsCountMap
		warehouseEventsMap    testhelper.EventsCountMap
		asyncJob              func(t testing.TB, wareHouseTest *testhelper.WareHouseTest)
	}{
		{
			name:                  "Upload Job",
			writeKey:              "kwzDkh9h2fhfUVuS9jZ8uVbhV3v",
			schema:                "postgres_wh_integration",
			tables:                []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
			sourceID:              "1wRvLmEnMOOxSQD9pwaZhyCqXRE",
			destinationID:         "216ZvbavR21Um6eGKQCagZHqLGZ",
			eventsMap:             testhelper.SendEventsMap(),
			stagingFilesEventsMap: testhelper.DefaultStagingFilesEventsMap(),
			loadFilesEventsMap:    testhelper.DefaultLoadFilesEventsMap(),
			tableUploadsEventsMap: testhelper.DefaultTableUploadsEventsMap(),
			warehouseEventsMap:    testhelper.DefaultWarehouseEventsMap(),
		},
		{
			name:                  "Async Job",
			writeKey:              "2DkCpXZcEvJK2fcpUD3LmjPI7J6",
			schema:                "postgres_wh_sources_integration",
			tables:                []string{"tracks", "google_sheet"},
			sourceID:              "2DkCpUr0xfiGBPJxIwqyqfyHdq4",
			destinationID:         "308ZvbavR21Um6eGKQCagZHqLGZ",
			eventsMap:             testhelper.SourcesSendEventMap(),
			stagingFilesEventsMap: testhelper.SourcesStagingFilesEventsMap(),
			loadFilesEventsMap:    testhelper.SourcesLoadFilesEventsMap(),
			tableUploadsEventsMap: testhelper.SourcesTableUploadsEventsMap(),
			warehouseEventsMap:    testhelper.SourcesWarehouseEventsMap(),
			asyncJob:              testhelper.VerifyAsyncJob,
			skipUserCreation:      true,
		},
	}

	for _, tc := range testcase {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			warehouseTest := &testhelper.WareHouseTest{
				Client: &client.Client{
					SQL:  db,
					Type: client.SQLClient,
				},
				WriteKey:      tc.writeKey,
				Schema:        tc.schema,
				Tables:        tc.tables,
				Provider:      warehouseutils.POSTGRES,
				SourceID:      tc.sourceID,
				DestinationID: tc.destinationID,
			}

			// Scenario 1
			warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
			warehouseTest.UserId = testhelper.GetUserId(warehouseutils.POSTGRES)
			warehouseTest.JobRunID = misc.FastUUID().String()
			warehouseTest.TaskRunID = misc.FastUUID().String()

			testhelper.SendEvents(t, warehouseTest, tc.eventsMap)
			testhelper.SendEvents(t, warehouseTest, tc.eventsMap)
			testhelper.SendEvents(t, warehouseTest, tc.eventsMap)
			testhelper.SendIntegratedEvents(t, warehouseTest, tc.eventsMap)

			testhelper.VerifyEventsInStagingFiles(t, jobsDB, warehouseTest, tc.stagingFilesEventsMap)
			testhelper.VerifyEventsInLoadFiles(t, jobsDB, warehouseTest, tc.loadFilesEventsMap)
			testhelper.VerifyEventsInTableUploads(t, jobsDB, warehouseTest, tc.tableUploadsEventsMap)
			testhelper.VerifyEventsInWareHouse(t, warehouseTest, tc.warehouseEventsMap)

			// Scenario 2
			warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
			warehouseTest.JobRunID = misc.FastUUID().String()
			warehouseTest.TaskRunID = misc.FastUUID().String()
			if !tc.skipUserCreation {
				warehouseTest.UserId = testhelper.GetUserId(warehouseutils.POSTGRES)
			}

			testhelper.SendModifiedEvents(t, warehouseTest, tc.eventsMap)
			testhelper.SendModifiedEvents(t, warehouseTest, tc.eventsMap)
			testhelper.SendModifiedEvents(t, warehouseTest, tc.eventsMap)
			testhelper.SendIntegratedEvents(t, warehouseTest, tc.eventsMap)

			testhelper.VerifyEventsInStagingFiles(t, jobsDB, warehouseTest, tc.stagingFilesEventsMap)
			testhelper.VerifyEventsInLoadFiles(t, jobsDB, warehouseTest, tc.loadFilesEventsMap)
			testhelper.VerifyEventsInTableUploads(t, jobsDB, warehouseTest, tc.tableUploadsEventsMap)
			if tc.asyncJob != nil {
				tc.asyncJob(t, warehouseTest)
			}
			testhelper.VerifyEventsInWareHouse(t, warehouseTest, tc.warehouseEventsMap)

			testhelper.VerifyWorkspaceIDInStats(t, statsToVerify...)
		})
	}
}

func TestPostgresConfigurationValidation(t *testing.T) {
	if os.Getenv("SLOW") == "0" {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}

	t.Parallel()

	misc.Init()
	validations.Init()
	warehouseutils.Init()
	postgres.Init()

	configurations := testhelper.PopulateTemplateConfigurations()
	destination := backendconfig.DestinationT{
		ID: "216ZvbavR21Um6eGKQCagZHqLGZ",
		Config: map[string]interface{}{
			"host":             configurations["postgresHost"],
			"database":         configurations["postgresDatabase"],
			"user":             configurations["postgresUser"],
			"password":         configurations["postgresPassword"],
			"port":             configurations["postgresPort"],
			"sslMode":          "disable",
			"namespace":        "",
			"bucketProvider":   "MINIO",
			"bucketName":       configurations["minioBucketName"],
			"accessKeyID":      configurations["minioAccesskeyID"],
			"secretAccessKey":  configurations["minioSecretAccessKey"],
			"useSSL":           false,
			"endPoint":         configurations["minioEndpoint"],
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
	testhelper.VerifyConfigurationTest(t, destination)
}
