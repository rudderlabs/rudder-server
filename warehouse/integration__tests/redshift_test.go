package integration__tests

import (
	"encoding/json"
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integration__tests/testhelper"
	"github.com/rudderlabs/rudder-server/warehouse/redshift"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

func rsCredentials() (rsCredentials *testhelper.RedshiftCredentials) {
	cred := os.Getenv("REDSHIFT_INTEGRATION_TEST_USER_CRED")
	if cred == "" {
		log.Panic("ERROR: ENV variable REDSHIFT_INTEGRATION_TEST_USER_CRED not found ")
	}

	var err error
	err = json.Unmarshal([]byte(cred), &rsCredentials)
	if err != nil {
		log.Panicf("Could not unmarshal REDSHIFT_INTEGRATION_TEST_USER_CRED with error: %s", err.Error())
	}

	return
}

// SetupRedshift setup warehouse redshift destination
func SetupRedshift() (rsTest *testhelper.RedshiftTest) {
	rsTest = &testhelper.RedshiftTest{
		WriteKey:    testhelper.RandString(27),
		Credentials: rsCredentials(),
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
		rsTest.DB, err = redshift.Connect(redshift.RedshiftCredentialsT{
			Host:     rsTest.Credentials.Host,
			Port:     rsTest.Credentials.Port,
			DbName:   rsTest.Credentials.Database,
			Username: rsTest.Credentials.User,
			Password: rsTest.Credentials.Password,
		})
		return err
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), uint64(5))
	if err = backoff.Retry(operation, backoffWithMaxRetry); err != nil {
		log.Panicf("could not connect to warehouse redshift with error: %s", err.Error())
	}
	return
}

func TestRedshift(t *testing.T) {
	if runRedshiftTest == false {
		t.Skip("redshift integration skipped. use -redshiftintegration to add this test ")
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
		UserId:             fmt.Sprintf("userId_redshift_%s", randomness),
		Schema:             "redshift_wh_integration",
		TableTestQueryFreq: rsTest.TableTestQueryFreq,
	}

	sendEvents(whDestTest)
	destinationTest(t, whDestTest)

	randomness = strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	whDestTest.UserId = fmt.Sprintf("userId_redshift_%s", randomness)
	sendUpdatedEvents(whDestTest)
	destinationTest(t, whDestTest)
}
