package redshift_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/redshift"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestIntegrationRedshift(t *testing.T) {
	if os.Getenv("SLOW") == "0" {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}
	if _, exists := os.LookupEnv(testhelper.RedshiftIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.RedshiftIntegrationTestCredentials)
	}

	t.Parallel()

	redshift.Init()

	var (
		jobsDB        = testhelper.SetUpJobsDB(t)
		provider      = warehouseutils.RS
		schema        = testhelper.Schema(provider, testhelper.RedshiftIntegrationTestSchema)
		sourcesSchema = fmt.Sprintf("%s_%s", schema, "sources")
	)

	testcase := []struct {
		name                  string
		schema                string
		writeKey              string
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
			schema:        schema,
			tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
			writeKey:      "JAAwdCxmM8BIabKERsUhPNmMmdf",
			sourceID:      "279L3gEKqwruBoKGsXZtSVX7vIy",
			destinationID: "27SthahyhhqZE74HT4NTtNPl06V",
		},
		{
			name:                  "Async Job",
			schema:                sourcesSchema,
			tables:                []string{"tracks", "google_sheet"},
			writeKey:              "BNAwdCxmM8BIabKERsUhPNmMmdf",
			sourceID:              "2DkCpUr0xgjfsdJxIwqyqfyHdq4",
			destinationID:         "27Sthahyhhsdas4HT4NTtNPl06V",
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

			credentials, err := testhelper.RedshiftCredentials()
			require.NoError(t, err)

			db, err := redshift.Connect(credentials)
			require.NoError(t, err)

			t.Cleanup(func() {
				require.NoError(
					t,
					testhelper.WithConstantBackoff(func() (err error) {
						_, err = db.Exec(fmt.Sprintf(`DROP SCHEMA %q CASCADE;`, tc.schema))
						return
					}),
					fmt.Sprintf("Failed dropping schema %s for Redshift", tc.schema),
				)
			})

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
				Provider:              provider,
				JobsDB:                jobsDB,
				JobRunID:              misc.FastUUID().String(),
				TaskRunID:             misc.FastUUID().String(),
				UserID:                testhelper.GetUserId(provider),
				Client: &client.Client{
					SQL:  db,
					Type: client.SQLClient,
				},
			}
			ts.TestScenarioOne(t)

			if !tc.asyncJob {
				ts.UserID = testhelper.GetUserId(provider)
			}
			ts.JobRunID = misc.FastUUID().String()
			ts.TaskRunID = misc.FastUUID().String()
			ts.TestScenarioTwo(t)
		})
	}
}

func TestConfigurationValidationRedshift(t *testing.T) {
	if os.Getenv("SLOW") == "0" {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}
	if _, exists := os.LookupEnv(testhelper.RedshiftIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.RedshiftIntegrationTestCredentials)
	}

	t.Parallel()

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
