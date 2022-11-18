package mssql_test

import (
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	"github.com/rudderlabs/rudder-server/warehouse/mssql"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestMSSQLIntegration(t *testing.T) {
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
			writeKey:              "YSQ3n267l1VQKGNbSuJE9fQbzON",
			schema:                "mssql_wh_integration",
			tables:                []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
			sourceID:              "1wRvLmEnMOONMbdspwaZhyCqXRE",
			destinationID:         "21Ezdq58khNMj07VJB0VJmxLvgu",
			eventsMap:             testhelper.SendEventsMap(),
			stagingFilesEventsMap: testhelper.DefaultStagingFilesEventsMap(),
			loadFilesEventsMap:    testhelper.DefaultLoadFilesEventsMap(),
			tableUploadsEventsMap: testhelper.DefaultTableUploadsEventsMap(),
			warehouseEventsMap:    testhelper.DefaultWarehouseEventsMap(),
		},
		{
			name:                  "Async Job",
			writeKey:              "2DkCpXZcEvPG2fcpUD3LmjPI7J6",
			schema:                "mssql_wh_sources_integration",
			tables:                []string{"tracks", "google_sheet"},
			sourceID:              "2DkCpUr0xfiINRJxIwqyqfyHdq4",
			destinationID:         "21Ezdq58kMNMj07VJB0VJmxLvgu",
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
				Provider:      warehouseutils.MSSQL,
				SourceID:      tc.sourceID,
				DestinationID: tc.destinationID,
			}

			// Scenario 1
			warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
			warehouseTest.UserId = testhelper.GetUserId(warehouseutils.MSSQL)
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
				warehouseTest.UserId = testhelper.GetUserId(warehouseutils.MSSQL)
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

			testhelper.VerifyWorkspaceIDInStats(t)
		})
	}
}

func TestMSSQLConfigurationValidation(t *testing.T) {
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
