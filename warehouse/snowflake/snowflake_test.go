//go:build warehouse_integration

package snowflake_test

import (
	"database/sql"
	"fmt"
	"testing"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

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

func (t *TestHandle) VerifyConnection() error {
	credentials, err := testhelper.SnowflakeCredentials()
	if err != nil {
		return err
	}
	return testhelper.WithConstantBackoff(func() (err error) {
		handle.DB, err = snowflake.Connect(credentials)
		if err != nil {
			err = fmt.Errorf("could not connect to warehouse snowflake with error: %w", err)
			return
		}
		return
	})
}

func TestSnowflakeIntegration(t *testing.T) {
	t.SkipNow()

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
	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.SNOWFLAKE)

	sendEventsMap = testhelper.SendEventsMap()
	testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

	testhelper.VerifyEventsInStagingFiles(t, warehouseTest, testhelper.StagingFilesEventsMap())
	testhelper.VerifyEventsInLoadFiles(t, warehouseTest, testhelper.LoadFilesEventsMap())
	testhelper.VerifyEventsInTableUploads(t, warehouseTest, testhelper.TableUploadsEventsMap())
	testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.WarehouseEventsMap())
}

func TestSnowflakeConfigurationValidation(t *testing.T) {
	t.SkipNow()

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
	//_, exists := os.LookupEnv(testhelper.SnowflakeIntegrationTestCredentials)
	//if !exists {
	//	log.Println("Skipping Snowflake Test as the Test credentials does not exists.")
	//	return
	//}
	//
	//handle = &TestHandle{
	//	WriteKey: "2eSJyYtqwcFiUILzXv2fcNIrWO7",
	//	Schema:   testhelper.Schema(warehouseutils.SNOWFLAKE, testhelper.SnowflakeIntegrationTestSchema),
	//	Tables:   []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
	//}
	//os.Exit(testhelper.Run(m, handle))
}
