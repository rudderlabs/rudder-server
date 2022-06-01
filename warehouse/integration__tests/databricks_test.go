package integration__tests

import (
	"encoding/json"
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/deltalake"
	"github.com/rudderlabs/rudder-server/warehouse/deltalake/databricks"
	"github.com/rudderlabs/rudder-server/warehouse/integration__tests/testhelper"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

func databricksCredentials() (credentials *testhelper.DatabricksCredentials) {
	cred := os.Getenv("DATABRICKS_INTEGRATION_TEST_USER_CRED")
	if cred == "" {
		log.Panic("ERROR: ENV variable DATABRICKS_INTEGRATION_TEST_USER_CRED not found ")
	}

	var err error
	err = json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		log.Panicf("Could not unmarshal REDSHIFT_INTEGRATION_TEST_USER_CRED with error: %s", err.Error())
	}

	return
}

// SetupDatabricks setup warehouse redshift destination
func SetupDatabricks() (databricksTest *testhelper.DatabricksTest) {
	databricksTest = &testhelper.DatabricksTest{
		WriteKey:    testhelper.RandString(27),
		Credentials: databricksCredentials(),
		EventsMap: testhelper.EventsCountMap{
			"identifies":    1,
			"users":         1,
			"tracks":        1,
			"product_track": 1,
			"pages":         1,
			"screens":       1,
			"aliases":       1,
			"groups":        1,
			"gateway":       6,
			"batchRT":       8,
		},
		TableTestQueryFreq: 5000 * time.Millisecond,
	}

	var err error

	operation := func() error {
		var err error
		databricksTest.DB, err = deltalake.Connect(&databricks.CredentialsT{
			Host:  databricksTest.Credentials.Host,
			Port:  databricksTest.Credentials.Port,
			Path:  databricksTest.Credentials.Path,
			Token: databricksTest.Credentials.Token,
		}, 0)
		return err
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), uint64(5))
	if err = backoff.Retry(operation, backoffWithMaxRetry); err != nil {
		log.Panicf("could not connect to warehouse redshift with error: %s", err.Error())
	}
	return
}

func TestDatabricks(t *testing.T) {
	if runDatabricksTest == false {
		t.Skip("redshift integration skipped. use -redshiftintegration to add this test ")
	}

	t.Parallel()

	databricksTest := DatabricksTest
	randomness := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")

	whDestTest := &testhelper.WareHouseDestinationTest{
		Client: &client.Client{
			DBHandleT: databricksTest.DB,
			Type:      client.DBClient,
		},
		EventsCountMap:     databricksTest.EventsMap,
		WriteKey:           databricksTest.WriteKey,
		UserId:             fmt.Sprintf("userId_databricks_%s", randomness),
		Schema:             "databricks_wh_integration",
		TableTestQueryFreq: databricksTest.TableTestQueryFreq,
	}

	sendEvents(whDestTest)
	destinationTest(t, whDestTest)

	randomness = strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	whDestTest.UserId = fmt.Sprintf("userId_databricks_%s", randomness)
	sendUpdatedEvents(whDestTest)
	destinationTest(t, whDestTest)
}
