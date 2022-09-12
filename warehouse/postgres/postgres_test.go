//go:build warehouse_integration

package postgres_test

import (
	"database/sql"
	"fmt"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"os"
	"testing"

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

// VerifyConnection test connection for postgres
func (*TestHandle) VerifyConnection() error {
	err := testhelper.WithConstantBackoff(func() (err error) {
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
	if err != nil {
		return fmt.Errorf("error while running test connection for postgres with err: %s", err.Error())
	}
	return nil
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

	warehouseTest.EventsCountMap = testhelper.SendEventsMap()
	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendIntegratedEvents(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.StagingFilesEventsMap()
	testhelper.VerifyEventsInStagingFiles(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.LoadFilesEventsMap()
	testhelper.VerifyEventsInLoadFiles(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.TableUploadsEventsMap()
	testhelper.VerifyEventsInTableUploads(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.WarehouseEventsMap()
	testhelper.VerifyEventsInWareHouse(t, warehouseTest)

	// Scenario 2
	warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.POSTGRES)

	warehouseTest.EventsCountMap = testhelper.SendEventsMap()
	testhelper.SendModifiedEvents(t, warehouseTest)
	testhelper.SendModifiedEvents(t, warehouseTest)
	testhelper.SendModifiedEvents(t, warehouseTest)
	testhelper.SendIntegratedEvents(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.StagingFilesEventsMap()
	testhelper.VerifyEventsInStagingFiles(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.LoadFilesEventsMap()
	testhelper.VerifyEventsInLoadFiles(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.TableUploadsEventsMap()
	testhelper.VerifyEventsInTableUploads(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.WarehouseEventsMap()
	testhelper.VerifyEventsInWareHouse(t, warehouseTest)
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
