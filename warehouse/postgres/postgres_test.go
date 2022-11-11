//go:build warehouse_integration && !sources_integration

package postgres_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/timeutil"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type TestHandle struct {
	DB       *sql.DB
	WriteKey string
	Schema   string
	Tables   []string
}

var handle *TestHandle

var statsToVerify = []string{"pg_rollback_timeout"}

func (*TestHandle) VerifyConnection() error {
	return testhelper.WithConstantBackoff(func() (err error) {
		credentials := postgres.CredentialsT{
			DBName:   "rudderdb",
			Password: "rudder-password",
			User:     "rudder",
			Host:     "wh-postgres",
			SSLMode:  "disable",
			Port:     "5432",
		}
		if handle.DB, err = postgres.Connect(credentials); err != nil {
			err = fmt.Errorf("could not connect to warehouse postgres with error: %w", err)
			return
		}
		if err = handle.DB.Ping(); err != nil {
			err = fmt.Errorf("could not connect to warehouse postgres while pinging with error: %w", err)
			return
		}
		return
	})
}

func TestPostgresIntegration(t *testing.T) {
	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  handle.DB,
			Type: client.SQLClient,
		},
		WriteKey:      handle.WriteKey,
		Schema:        handle.Schema,
		Tables:        handle.Tables,
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

	testhelper.VerifyEventsInStagingFiles(t, warehouseTest, testhelper.StagingFilesEventsMap())
	testhelper.VerifyEventsInLoadFiles(t, warehouseTest, testhelper.LoadFilesEventsMap())
	testhelper.VerifyEventsInTableUploads(t, warehouseTest, testhelper.TableUploadsEventsMap())
	testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.WarehouseEventsMap())

	// Scenario 2
	warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.POSTGRES)

	sendEventsMap = testhelper.SendEventsMap()
	testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

	testhelper.VerifyEventsInStagingFiles(t, warehouseTest, testhelper.StagingFilesEventsMap())
	testhelper.VerifyEventsInLoadFiles(t, warehouseTest, testhelper.LoadFilesEventsMap())
	testhelper.VerifyEventsInTableUploads(t, warehouseTest, testhelper.TableUploadsEventsMap())
	testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.WarehouseEventsMap())

	testhelper.VerifyWorkspaceIDInStats(t, statsToVerify...)
}

func TestPostgresConfigurationValidation(t *testing.T) {
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
	testhelper.VerifyingConfigurationTest(t, destination)
}

func TestMain(m *testing.M) {
	handle = &TestHandle{
		WriteKey: "kwzDkh9h2fhfUVuS9jZ8uVbhV3v",
		Schema:   "postgres_wh_integration",
		Tables:   []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
	}
	os.Exit(testhelper.Run(m, handle))
}
