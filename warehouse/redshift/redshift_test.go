//go:build warehouse_integration

package redshift_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	"github.com/rudderlabs/rudder-server/utils/timeutil"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/redshift"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestRedshiftIntegration(t *testing.T) {
	t.Parallel()

	if _, exists := os.LookupEnv(testhelper.RedshiftIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.RedshiftIntegrationTestCredentials)
	}

	redshift.Init()

	credentials, err := testhelper.RedshiftCredentials()
	require.NoError(t, err)

	require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
		{
			Key:   "Warehouse.redshift.dedupWindow",
			Value: true,
		},
		{
			Key:   "Warehouse.redshift.dedupWindowInHours",
			Value: 5,
		},
	}))

	testcase := []struct {
		name                  string
		schema                string
		writeKey              string
		sourceID              string
		destinationID         string
		tables                []string
		skipUserCreation      bool
		eventsMap             testhelper.EventsCountMap
		stagingFilesEventsMap testhelper.EventsCountMap
		loadFilesEventsMap    testhelper.EventsCountMap
		tableUploadsEventsMap testhelper.EventsCountMap
		warehouseEventsMap    testhelper.EventsCountMap
		asyncJob              func(t testing.TB, wareHouseTest *testhelper.WareHouseTest)
	}{
		{
			name:                  "Upload JOB",
			schema:                testhelper.Schema(warehouseutils.RS, testhelper.RedshiftIntegrationTestSchema),
			tables:                []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
			writeKey:              "JAAwdCxmM8BIabKERsUhPNmMmdf",
			sourceID:              "279L3gEKqwruBoKGsXZtSVX7vIy",
			destinationID:         "27SthahyhhqZE74HT4NTtNPl06V",
			eventsMap:             testhelper.SendEventsMap(),
			stagingFilesEventsMap: testhelper.DefaultStagingFilesEventsMap(),
			loadFilesEventsMap:    testhelper.DefaultLoadFilesEventsMap(),
			tableUploadsEventsMap: testhelper.DefaultTableUploadsEventsMap(),
			warehouseEventsMap:    testhelper.DefaultWarehouseEventsMap(),
		},
		{
			name:                  "Async JOB",
			schema:                fmt.Sprintf("%s_%s", testhelper.Schema(warehouseutils.RS, testhelper.RedshiftIntegrationTestSchema), "sources"),
			tables:                []string{"tracks", "google_sheet"},
			writeKey:              "BNAwdCxmM8BIabKERsUhPNmMmdf",
			sourceID:              "2DkCpUr0xgjfsdJxIwqyqfyHdq4",
			destinationID:         "27Sthahyhhsdas4HT4NTtNPl06V",
			eventsMap:             testhelper.SourcesSendEventMap(),
			stagingFilesEventsMap: testhelper.SourcesStagingFilesEventsMap(),
			loadFilesEventsMap:    testhelper.SourcesLoadFilesEventsMap(),
			tableUploadsEventsMap: testhelper.SourcesTableUploadsEventsMap(),
			warehouseEventsMap:    testhelper.SourcesWarehouseEventsMap(),
			asyncJob:              testhelper.VerifyAsyncJob,
		},
	}

	jobsDB := testhelper.SetUpJobsDB(t)

	for _, tc := range testcase {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db, err := redshift.Connect(credentials)
			require.NoError(t, err)

			t.Cleanup(func() {
				require.NoError(t, testhelper.WithConstantBackoff(func() (err error) {
					_, err = db.Exec(fmt.Sprintf(`DROP SCHEMA %q CASCADE;`, tc.schema))
					return
				}), fmt.Sprintf("Failed dropping schema %s for Redshift", tc.schema))
			})

			warehouseTest := &testhelper.WareHouseTest{
				Client: &client.Client{
					SQL:  db,
					Type: client.SQLClient,
				},
				WriteKey:      tc.writeKey,
				Schema:        tc.schema,
				SourceID:      tc.sourceID,
				DestinationID: tc.destinationID,
				Tables:        tc.tables,
				Provider:      warehouseutils.RS,
			}

			// Scenario 1
			warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
			warehouseTest.UserId = testhelper.GetUserId(warehouseutils.RS)
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
				warehouseTest.UserId = testhelper.GetUserId(warehouseutils.RS)
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

func TestRedshiftConfigurationValidation(t *testing.T) {
	t.Parallel()

	if _, exists := os.LookupEnv(testhelper.RedshiftIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.RedshiftIntegrationTestCredentials)
	}

	misc.Init()
	validations.Init()
	warehouseutils.Init()
	redshift.Init()

	configurations := testhelper.PopulateTemplateConfigurations()
	destination := backendconfig.DestinationT{
		ID: "27SthahyhhqZE74HT4NTtNPl06V",
		Config: map[string]interface{}{
			"host":             configurations["redshiftHost"],
			"port":             configurations["redshiftPort"],
			"database":         configurations["redshiftDbName"],
			"user":             configurations["redshiftUsername"],
			"password":         configurations["redshiftPassword"],
			"bucketName":       configurations["redshiftBucketName"],
			"accessKeyID":      configurations["redshiftAccessKeyID"],
			"accessKey":        configurations["redshiftAccessKey"],
			"prefix":           "",
			"namespace":        configurations["redshiftNamespace"],
			"syncFrequency":    "30",
			"enableSSE":        false,
			"useRudderStorage": false,
		},
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			ID:          "1UVZiJF7OgLaiIY2Jts8XOQE3M6",
			Name:        "RS",
			DisplayName: "Redshift",
		},
		Name:       "redshift-demo",
		Enabled:    true,
		RevisionID: "29HgOWobrn0RYZLpaSwPIbN2987",
	}
	testhelper.VerifyConfigurationTest(t, destination)
}
