package mssql_test

import (
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/mssql"

	"github.com/rudderlabs/rudder-server/warehouse/client"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestIntegrationMSSQL(t *testing.T) {
	if os.Getenv("SLOW") == "0" {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}

	t.Parallel()

	mssql.Init()

	db, err := mssql.Connect(mssql.CredentialsT{
		DBName:   "master",
		Password: "reallyStrongPwd123",
		User:     "SA",
		Host:     "wh-mssql",
		SSLMode:  "disable",
		Port:     "1433",
	})
	require.NoError(t, err)

	err = db.Ping()
	require.NoError(t, err)

	var (
		provider = warehouseutils.MSSQL
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
			writeKey:      "YSQ3n267l1VQKGNbSuJE9fQbzON",
			schema:        "mssql_wh_integration",
			tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
			sourceID:      "1wRvLmEnMOONMbdspwaZhyCqXRE",
			destinationID: "21Ezdq58khNMj07VJB0VJmxLvgu",
		},
		{
			name:                  "Async Job",
			writeKey:              "2DkCpXZcEvPG2fcpUD3LmjPI7J6",
			schema:                "mssql_wh_sources_integration",
			tables:                []string{"tracks", "google_sheet"},
			sourceID:              "2DkCpUr0xfiINRJxIwqyqfyHdq4",
			destinationID:         "21Ezdq58kMNMj07VJB0VJmxLvgu",
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
				Client: &client.Client{
					SQL:  db,
					Type: client.SQLClient,
				},
			}
			ts.VerifyEvents(t)

			if !tc.asyncJob {
				ts.UserID = testhelper.GetUserId(provider)
			}
			ts.JobRunID = misc.FastUUID().String()
			ts.TaskRunID = misc.FastUUID().String()
			ts.VerifyModifiedEvents(t)
		})
	}
}

func TestConfigurationValidationMSSQL(t *testing.T) {
	if os.Getenv("SLOW") == "0" {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}

	t.Parallel()

	misc.Init()
	validations.Init()
	warehouseutils.Init()
	mssql.Init()

	configurations := testhelper.PopulateTemplateConfigurations()
	destination := backendconfig.DestinationT{
		ID: "21Ezdq58khNMj07VJB0VJmxLvgu",
		Config: map[string]interface{}{
			"host":             configurations["mssqlHost"],
			"database":         configurations["mssqlDatabase"],
			"user":             configurations["mssqlUser"],
			"password":         configurations["mssqlPassword"],
			"port":             configurations["mssqlPort"],
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
			ID:          "1qvbUYC2xVQ7lvI9UUYkkM4IBt9",
			Name:        "MSSQL",
			DisplayName: "Microsoft SQL Server",
		},
		Name:       "mssql-demo",
		Enabled:    true,
		RevisionID: "29eeuUb21cuDBeFKPTUA9GaQ9Aq",
	}
	testhelper.VerifyConfigurationTest(t, destination)
}
