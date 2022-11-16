//go:build sources_integration && !warehouse_integration

package snowflake_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/snowflake"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

func TestSourcesSnowflakeIntegration(t *testing.T) {
	t.SkipNow()
	t.Parallel()

	if _, exists := os.LookupEnv(testhelper.SnowflakeIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.SnowflakeIntegrationTestCredentials)
	}

	snowflake.Init()

	credentials, err := testhelper.SnowflakeCredentials()
	require.NoError(t, err)

	db, err := snowflake.Connect(credentials)
	require.NoError(t, err)

	schema := testhelper.Schema(warehouseutils.SNOWFLAKE, testhelper.SnowflakeIntegrationTestSchema)

	t.Cleanup(func() {
		require.NoError(t, testhelper.WithConstantBackoff(func() (err error) {
			_, err = db.Exec(fmt.Sprintf(`DROP SCHEMA "%s" CASCADE;`, schema))
			return
		}), fmt.Sprintf("Failed dropping schema %s for Snowflake", schema))
	})

	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  db,
			Type: client.SQLClient,
		},
		Schema:                schema,
		Tables:                []string{"tracks", "google_sheet"},
		MessageId:             misc.FastUUID().String(),
		Provider:              warehouseutils.SNOWFLAKE,
		SourceID:              "2DkCpUr0xfiGBPJxIwqyqfyHdq4",
		DestinationID:         "24qeADObp6eIhjjDnEppO6P1SNc",
		SourceWriteKey:        "2DkCpXZcEvJK2fcpUD3LmjPI7J6",
		LatestSourceRunConfig: testhelper.DefaultSourceRunConfig(),
	}

	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.SNOWFLAKE)
	sendEventsMap := testhelper.DefaultSourceEventMap()

	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)

	testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.DefaultWarehouseSourceEventsMapWithoutDedup())

	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendEvents(t, warehouseTest, sendEventsMap)

	testhelper.SendAsyncRequest(t, warehouseTest)
	testhelper.SendAsyncStatusRequest(t, warehouseTest)

	testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.DefaultWarehouseSourceEventsMap())
}
