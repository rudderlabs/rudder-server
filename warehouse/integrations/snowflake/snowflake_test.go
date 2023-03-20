package snowflake_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/snowflake"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

	"github.com/rudderlabs/rudder-server/warehouse/client"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

func TestIntegrationSnowflake(t *testing.T) {
	if os.Getenv("SLOW") == "0" {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}
	if _, exists := os.LookupEnv(testhelper.SnowflakeIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.SnowflakeIntegrationTestCredentials)
	}
	if _, exists := os.LookupEnv(testhelper.SnowflakeRBACIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.SnowflakeRBACIntegrationTestCredentials)
	}

	t.Parallel()

	snowflake.Init()

	credentials, err := testhelper.SnowflakeCredentials(testhelper.SnowflakeIntegrationTestCredentials)
	require.NoError(t, err)

	rbacCrecentials, err := testhelper.SnowflakeCredentials(testhelper.SnowflakeRBACIntegrationTestCredentials)
	require.NoError(t, err)

	var (
		provider            = warehouseutils.SNOWFLAKE
		jobsDB              = testhelper.SetUpJobsDB(t)
		schema              = testhelper.Schema(provider, testhelper.SnowflakeIntegrationTestSchema)
		roleSchema          = fmt.Sprintf("%s_%s", schema, "ROLE")
		sourcesSchema       = fmt.Sprintf("%s_%s", schema, "SOURCES")
		caseSensitiveSchema = fmt.Sprintf("%s_%s", schema, "CS")
		database            = credentials.Database
	)

	testcase := []struct {
		name                          string
		credentials                   snowflake.Credentials
		database                      string
		schema                        string
		writeKey                      string
		sourceID                      string
		destinationID                 string
		eventsMap                     testhelper.EventsCountMap
		stagingFilesEventsMap         testhelper.EventsCountMap
		stagingFilesModifiedEventsMap testhelper.EventsCountMap
		loadFilesEventsMap            testhelper.EventsCountMap
		tableUploadsEventsMap         testhelper.EventsCountMap
		warehouseEventsMap            testhelper.EventsCountMap
		asyncJob                      bool
		tables                        []string
	}{
		{
			name:          "Upload Job with Normal Database",
			credentials:   credentials,
			database:      database,
			schema:        schema,
			tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
			writeKey:      "2eSJyYtqwcFiUILzXv2fcNIrWO7",
			sourceID:      "24p1HhPk09FW25Kuzvx7GshCLKR",
			destinationID: "24qeADObp6eIhjjDnEppO6P1SNc",
			stagingFilesEventsMap: testhelper.EventsCountMap{
				"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
			},
			stagingFilesModifiedEventsMap: testhelper.EventsCountMap{
				"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
			},
		},
		{
			name:          "Upload Job with Role",
			credentials:   rbacCrecentials,
			database:      database,
			schema:        roleSchema,
			tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
			writeKey:      "2eSafstqwcFYUILzXv2fcNIrWO7",
			sourceID:      "24p1HhPsafaFBMKuzvx7GshCLKR",
			destinationID: "24qeADObsdsJhijDnEppO6P1SNc",
			stagingFilesEventsMap: testhelper.EventsCountMap{
				"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
			},
			stagingFilesModifiedEventsMap: testhelper.EventsCountMap{
				"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
			},
		},
		{
			name:          "Upload Job with Case Sensitive Database",
			credentials:   credentials,
			database:      strings.ToLower(database),
			schema:        caseSensitiveSchema,
			tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
			writeKey:      "2eSJyYtqwcFYUILzXv2fcNIrWO7",
			sourceID:      "24p1HhPk09FBMKuzvx7GshCLKR",
			destinationID: "24qeADObp6eJhijDnEppO6P1SNc",
			stagingFilesEventsMap: testhelper.EventsCountMap{
				"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
			},
			stagingFilesModifiedEventsMap: testhelper.EventsCountMap{
				"wh_staging_files": 34, // 32 + 2 (merge events because of ID resolution)
			},
		},
		{
			name:          "Async Job with Sources",
			credentials:   credentials,
			database:      database,
			schema:        sourcesSchema,
			tables:        []string{"tracks", "google_sheet"},
			writeKey:      "2eSJyYtqwcFYerwzXv2fcNIrWO7",
			sourceID:      "2DkCpUr0xgjaNRJxIwqyqfyHdq4",
			destinationID: "24qeADObp6eIsfjDnEppO6P1SNc",
			eventsMap:     testhelper.SourcesSendEventsMap(),
			stagingFilesEventsMap: testhelper.EventsCountMap{
				"wh_staging_files": 9, // 8 + 1 (merge events because of ID resolution)
			},
			stagingFilesModifiedEventsMap: testhelper.EventsCountMap{
				"wh_staging_files": 8, // 8 (de-duped by encounteredMergeRuleMap)
			},
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

			credentialsCopy := tc.credentials
			credentialsCopy.Database = tc.database

			db, err := snowflake.Connect(credentialsCopy)
			require.NoError(t, err)

			t.Cleanup(func() {
				require.NoError(
					t,
					testhelper.WithConstantBackoff(func() (err error) {
						_, err = db.Exec(fmt.Sprintf(`DROP SCHEMA %q CASCADE;`, tc.schema))
						return
					}),
					fmt.Sprintf("Failed dropping schema %s for Snowflake", tc.schema),
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
			ts.VerifyEvents(t)

			if !tc.asyncJob {
				ts.UserID = testhelper.GetUserId(provider)
			}
			ts.StagingFilesEventsMap = tc.stagingFilesModifiedEventsMap
			ts.JobRunID = misc.FastUUID().String()
			ts.TaskRunID = misc.FastUUID().String()
			ts.VerifyModifiedEvents(t)
		})
	}
}

func TestConfigurationValidationSnowflake(t *testing.T) {
	if os.Getenv("SLOW") == "0" {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}
	if _, exists := os.LookupEnv(testhelper.SnowflakeIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.SnowflakeIntegrationTestCredentials)
	}

	t.Parallel()

	misc.Init()
	validations.Init()
	warehouseutils.Init()
	encoding.Init()
	snowflake.Init()

	configurations := testhelper.PopulateTemplateConfigurations()
	destination := backendconfig.DestinationT{
		ID: "24qeADObp6eIhjjDnEppO6P1SNc",
		Config: map[string]interface{}{
			"account":            configurations["snowflakeAccount"],
			"database":           configurations["snowflakeDBName"],
			"warehouse":          configurations["snowflakeWHName"],
			"user":               configurations["snowflakeUsername"],
			"password":           configurations["snowflakePassword"],
			"cloudProvider":      "AWS",
			"bucketName":         configurations["snowflakeBucketName"],
			"storageIntegration": "",
			"accessKeyID":        configurations["snowflakeAccessKeyID"],
			"accessKey":          configurations["snowflakeAccessKey"],
			"namespace":          configurations["snowflakeNamespace"],
			"prefix":             "snowflake-prefix",
			"syncFrequency":      "30",
			"enableSSE":          false,
			"useRudderStorage":   false,
		},
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			ID:          "1XjvXnzw34UMAz1YOuKqL1kwzh6",
			Name:        "SNOWFLAKE",
			DisplayName: "Snowflake",
		},
		Name:       "snowflake-demo",
		Enabled:    true,
		RevisionID: "29HgdgvNPwqFDMONSgmIZ3YSehV",
	}
	testhelper.VerifyConfigurationTest(t, destination)
}
