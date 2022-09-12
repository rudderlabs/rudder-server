//go:build warehouse_integration

package redshift_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/utils/timeutil"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/redshift"
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

const (
	TestCredentialsKey = testhelper.RedshiftIntegrationTestCredentials
	TestSchemaKey      = testhelper.RedshiftIntegrationTestSchema
)

// redshiftCredentials extracting redshift test credentials
func redshiftCredentials() (rsCredentials redshift.RedshiftCredentialsT, err error) {
	cred, exists := os.LookupEnv(TestCredentialsKey)
	if !exists {
		err = fmt.Errorf("following %s does not exists while running the Redshift test", TestCredentialsKey)
		return
	}

	err = json.Unmarshal([]byte(cred), &rsCredentials)
	if err != nil {
		err = fmt.Errorf("error occurred while unmarshalling redshift test credentials with err: %s", err.Error())
	}
	return
}

// VerifyConnection test connection for redshift
func (*TestHandle) VerifyConnection() error {
	credentials, err := redshiftCredentials()
	if err != nil {
		return err
	}

	err = testhelper.WithConstantBackoff(func() (err error) {
		handle.DB, err = redshift.Connect(credentials)
		if err != nil {
			err = fmt.Errorf("could not connect to warehouse redshift with error: %w", err)
			return
		}
		return
	})
	if err != nil {
		return fmt.Errorf("error while running test connection for redshift with err: %s", err.Error())
	}
	return nil
}

func TestRedshiftIntegration(t *testing.T) {
	// Cleanup resources
	t.Cleanup(func() {
		require.NoError(t, testhelper.WithConstantBackoff(func() (err error) {
			_, err = handle.DB.Exec(fmt.Sprintf(`DROP SCHEMA %q CASCADE;`, handle.Schema))
			return
		}), fmt.Sprintf("Failed dropping schema %s for Redshift", handle.Schema))
	})

	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  handle.DB,
			Type: client.SQLClient,
		},
		WriteKey:      handle.WriteKey,
		Schema:        handle.Schema,
		Tables:        handle.Tables,
		Provider:      warehouseutils.RS,
		SourceID:      "279L3gEKqwruBoKGsXZtSVX7vIy",
		DestinationID: "27SthahyhhqZE74HT4NTtNPl06V",
	}

	// Scenario 1
	warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.RS)

	warehouseTest.EventsCountMap = testhelper.SendEventsMap()
	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendEvents(t, warehouseTest)
	testhelper.SendIntegratedEvents(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.StagingFilesEventsMap()
	testhelper.VerifyingEventsInStagingFiles(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.LoadFilesEventsMap()
	testhelper.VerifyingEventsInLoadFiles(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.TableUploadsEventsMap()
	testhelper.VerifyingEventsInTableUploads(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.WarehouseEventsMap()
	testhelper.VerifyingEventsInWareHouse(t, warehouseTest)

	// Scenario 2
	warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.RS)

	warehouseTest.EventsCountMap = testhelper.SendEventsMap()
	testhelper.SendModifiedEvents(t, warehouseTest)
	testhelper.SendModifiedEvents(t, warehouseTest)
	testhelper.SendModifiedEvents(t, warehouseTest)
	testhelper.SendIntegratedEvents(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.StagingFilesEventsMap()
	testhelper.VerifyingEventsInStagingFiles(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.LoadFilesEventsMap()
	testhelper.VerifyingEventsInLoadFiles(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.TableUploadsEventsMap()
	testhelper.VerifyingEventsInTableUploads(t, warehouseTest)

	warehouseTest.EventsCountMap = testhelper.WarehouseEventsMap()
	testhelper.VerifyingEventsInWareHouse(t, warehouseTest)
}

func TestMain(m *testing.M) {
	_, exists := os.LookupEnv(TestCredentialsKey)
	if !exists {
		log.Println("Skipping Redshift Test as the Test credentials does not exists.")
		return
	}

	handle = &TestHandle{
		WriteKey: "JAAwdCxmM8BIabKERsUhPNmMmdf",
		Schema:   testhelper.GetSchema(warehouseutils.RS, TestSchemaKey),
		Tables:   []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
	}
	os.Exit(testhelper.Run(m, handle))
}
