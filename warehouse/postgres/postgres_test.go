package postgres_test

import (
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

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

	var (
		provider = warehouseutils.POSTGRES
		jobsDB   = testhelper.SetUpJobsDB(t)
	)

	testcase := []struct {
		name                  string
		writeKey              string
		schema                string
		sourceID              string
		destinationID         string
		eventsMap             testhelper.EventsCountMap
		stagingFilesEventsMap testhelper.EventsCountMap
		loadFilesEventsMap    testhelper.EventsCountMap
		tableUploadsEventsMap testhelper.EventsCountMap
		warehouseEventsMap    testhelper.EventsCountMap
		asyncJob              bool
		tables                []string
	}{
		{
			name:          "Upload Job",
			writeKey:      "kwzDkh9h2fhfUVuS9jZ8uVbhV3v",
			schema:        "postgres_wh_integration",
			tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
			sourceID:      "1wRvLmEnMOOxSQD9pwaZhyCqXRE",
			destinationID: "216ZvbavR21Um6eGKQCagZHqLGZ",
		},
		{
			name:                  "Async Job",
			writeKey:              "2DkCpXZcEvJK2fcpUD3LmjPI7J6",
			schema:                "postgres_wh_sources_integration",
			tables:                []string{"tracks", "google_sheet"},
			sourceID:              "2DkCpUr0xfiGBPJxIwqyqfyHdq4",
			destinationID:         "308ZvbavR21Um6eGKQCagZHqLGZ",
			eventsMap:             testhelper.SourcesSendEventsMap(),
			stagingFilesEventsMap: testhelper.SourcesStagingFilesEventsMap(),
			loadFilesEventsMap:    testhelper.SourcesLoadFilesEventsMap(),
			tableUploadsEventsMap: testhelper.SourcesTableUploadsEventsMap(),
			warehouseEventsMap:    testhelper.SourcesWarehouseEventsMap(),
			asyncJob:              true,
		},
	}

	for _, tc := range testcase {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ts := testhelper.WareHouseTest{
				Schema:                tc.schema,
				WriteKey:              tc.writeKey,
				SourceID:              tc.sourceID,
				DestinationID:         tc.destinationID,
				Tables:                tc.tables,
				EventsMap:             tc.eventsMap,
				StagingFilesEventsMap: tc.stagingFilesEventsMap,
				LoadFilesEventsMap:    tc.loadFilesEventsMap,
				TableUploadsEventsMap: tc.tableUploadsEventsMap,
				WarehouseEventsMap:    tc.warehouseEventsMap,
				AsyncJob:              tc.asyncJob,
				UserID:                testhelper.GetUserId(provider),
				Provider:              provider,
				JobsDB:                jobsDB,
				JobRunID:              misc.FastUUID().String(),
				TaskRunID:             misc.FastUUID().String(),
				StatsToVerify:         []string{"pg_rollback_timeout"},
				Client: &client.Client{
					SQL:  db,
					Type: client.SQLClient,
				},
			}
			ts.TestScenarioOne(t)

			ts.UserID = testhelper.GetUserId(provider)
			ts.TestScenarioTwo(t)
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
