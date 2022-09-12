//go:build warehouse_integration

package snowflake_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"log"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/utils/timeutil"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/snowflake"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

type TestHandle struct {
	DB       *sql.DB
	WriteKey string
	Schema   string
	Tables   []string
}

var handle *TestHandle

const (
	TestCredentialsKey = testhelper.SnowflakeIntegrationTestCredentials
	TestSchemaKey      = testhelper.SnowflakeIntegrationTestSchema
)

// snowflakeCredentials extracting snowflake credentials
func snowflakeCredentials() (snowflakeCredentials snowflake.SnowflakeCredentialsT, err error) {
	cred, exists := os.LookupEnv(TestCredentialsKey)
	if !exists {
		err = fmt.Errorf("following %s does not exists while running the Snowflake test", TestCredentialsKey)
		return
	}

	err = json.Unmarshal([]byte(cred), &snowflakeCredentials)
	if err != nil {
		err = fmt.Errorf("error occurred while unmarshalling snowflake test credentials with err: %s", err.Error())
		return
	}
	return
}

// VerifyConnection test connection for snowflake
func (t *TestHandle) VerifyConnection() (err error) {
	credentials, err := snowflakeCredentials()
	if err != nil {
		return err
	}

	err = testhelper.WithConstantBackoff(func() (err error) {
		handle.DB, err = snowflake.Connect(credentials)
		if err != nil {
			err = fmt.Errorf("could not connect to warehouse snowflake with error: %w", err)
			return
		}
		return
	})
	if err != nil {
		return fmt.Errorf("error while running test connection for snowflake with err: %s", err.Error())
	}
	return nil
}

func TestSnowflakeIntegration(t *testing.T) {
	// Cleanup resources
	t.Cleanup(func() {
		require.NoError(t, testhelper.WithConstantBackoff(func() (err error) {
			_, err = handle.DB.Exec(fmt.Sprintf(`DROP SCHEMA "%s" CASCADE;`, handle.Schema))
			return
		}), fmt.Sprintf("Failed dropping schema %s for Snowflake", handle.Schema))
	})

	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  handle.DB,
			Type: client.SQLClient,
		},
		WriteKey:      handle.WriteKey,
		Schema:        handle.Schema,
		Tables:        handle.Tables,
		Provider:      warehouseutils.SNOWFLAKE,
		SourceID:      "24p1HhPk09FW25Kuzvx7GshCLKR",
		DestinationID: "24qeADObp6eIhjjDnEppO6P1SNc",
	}

	// Scenario 1
	warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.SNOWFLAKE)

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
	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.SNOWFLAKE)

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

func TestSnowflakeConfigurationValidation(t *testing.T) {
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
	testhelper.VerifyingConfigurationTest(t, destination)
}

func TestMain(m *testing.M) {
	_, exists := os.LookupEnv(TestCredentialsKey)
	if !exists {
		log.Println("Skipping Snowflake Test as the Test credentials does not exists.")
		return
	}

	handle = &TestHandle{
		WriteKey: "2eSJyYtqwcFiUILzXv2fcNIrWO7",
		Schema:   testhelper.GetSchema(warehouseutils.SNOWFLAKE, TestSchemaKey),
		Tables:   []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
	}
	os.Exit(testhelper.Run(m, handle))
}
