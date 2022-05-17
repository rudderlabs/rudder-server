package integration__tests

import (
	"encoding/json"
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integration__tests/testhelper"
	"github.com/rudderlabs/rudder-server/warehouse/snowflake"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

func sfCredentials() (sfCredentials *testhelper.SnowflakeCredentials) {
	cred := os.Getenv("SNOWFLAKE_INTEGRATION_TEST_USER_CRED")
	if cred == "" {
		log.Panic("ERROR: ENV variable SNOWFLAKE_INTEGRATION_TEST_USER_CRED not found ")
	}

	var err error
	err = json.Unmarshal([]byte(cred), &sfCredentials)
	if err != nil {
		log.Panicf("Could not unmarshal SNOWFLAKE_INTEGRATION_TEST_USER_CRED with error: %s", err.Error())
	}

	return
}

// SetupSnowflake setup warehouse snowflake destination
func SetupSnowflake() (sfTest *testhelper.SnowflakeTest) {
	sfTest = &testhelper.SnowflakeTest{
		WriteKey:    testhelper.RandString(27),
		Credentials: sfCredentials(),
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
		sfTest.DB, err = snowflake.Connect(snowflake.SnowflakeCredentialsT{
			Account:  sfTest.Credentials.Account,
			WHName:   sfTest.Credentials.Warehouse,
			DBName:   sfTest.Credentials.Database,
			Username: sfTest.Credentials.User,
			Password: sfTest.Credentials.Password,
		})
		return err
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), uint64(5))
	if err = backoff.Retry(operation, backoffWithMaxRetry); err != nil {
		log.Panicf("could not connect to warehouse snowflake with error: %s", err.Error())
	}
	return
}

func TestSnowflake(t *testing.T) {
	if runSnowflakeTest == false {
		t.Skip("Snowflake integration skipped. use -snowflakeintegration to add this test ")
	}

	t.Parallel()

	rsTest := RSTest
	randomness := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")

	whDestTest := &testhelper.WareHouseDestinationTest{
		Client: &client.Client{
			SQL:  rsTest.DB,
			Type: client.SQLClient,
		},
		EventsCountMap:     rsTest.EventsMap,
		WriteKey:           rsTest.WriteKey,
		UserId:             fmt.Sprintf("userId_snowflake_%s", randomness),
		Schema:             "rudderstack_sample_http_source",
		TableTestQueryFreq: rsTest.TableTestQueryFreq,
	}

	sendEvents(whDestTest)
	destinationTest(t, whDestTest)

	randomness = strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	whDestTest.UserId = fmt.Sprintf("userId_snowflake_%s", randomness)
	sendUpdatedEvents(whDestTest)
	destinationTest(t, whDestTest)
}
