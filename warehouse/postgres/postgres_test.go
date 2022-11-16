//go:build warehouse_integration && !sources_integration

package postgres_test

import (
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

	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  db,
			Type: client.SQLClient,
		},
		WriteKey:      "kwzDkh9h2fhfUVuS9jZ8uVbhV3v",
		Schema:        "postgres_wh_integration",
		Tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
		Provider:      warehouseutils.POSTGRES,
		SourceID:      "1wRvLmEnMOOxSQD9pwaZhyCqXRE",
		DestinationID: "216ZvbavR21Um6eGKQCagZHqLGZ",
	}

	// Scenario 1
	warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.POSTGRES)

	sendEventsMap := testhelper.SendEventsMap()
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

	testhelper.VerifyEventsInStagingFiles(t, jobsDB, warehouseTest, testhelper.DefaultStagingFilesEventsMap())
	testhelper.VerifyEventsInLoadFiles(t, jobsDB, warehouseTest, testhelper.DefaultLoadFilesEventsMap())
	testhelper.VerifyEventsInTableUploads(t, jobsDB, warehouseTest, testhelper.DefaultTableUploadsEventsMap())
	testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.DefaultWarehouseEventsMap())

	// Scenario 2
	warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.POSTGRES)

	sendEventsMap = testhelper.SendEventsMap()
	testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

	testhelper.VerifyEventsInStagingFiles(t, jobsDB, warehouseTest, testhelper.DefaultStagingFilesEventsMap())
	testhelper.VerifyEventsInLoadFiles(t, jobsDB, warehouseTest, testhelper.DefaultLoadFilesEventsMap())
	testhelper.VerifyEventsInTableUploads(t, jobsDB, warehouseTest, testhelper.DefaultTableUploadsEventsMap())
	testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.DefaultWarehouseEventsMap())

	testhelper.VerifyWorkspaceIDInStats(t, statsToVerify...)
}

func TestPostgresConfigurationValidation(t *testing.T) {
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
