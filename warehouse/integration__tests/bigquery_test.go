package integration__tests

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integration__tests/testhelper"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-server/warehouse/bigquery"
)

func bqCredentials() (bqCredentials *testhelper.BigQueryCredentials) {
	cred := os.Getenv("BIGQUERY_INTEGRATION_TEST_USER_CRED")
	if cred == "" {
		log.Panic("ERROR: ENV variable BIGQUERY_INTEGRATION_TEST_USER_CRED not found ")
	}

	var err error
	err = json.Unmarshal([]byte(cred), &bqCredentials)
	if err != nil {
		log.Panicf("Could not unmarshal BIGQUERY_INTEGRATION_TEST_USER_CRED.credentials with error: %s", err.Error())
	}

	return
}

// SetupBigQuery setup warehouse Big query destination
func SetupBigQuery() (bqTest *testhelper.BiqQueryTest) {
	bqTest = &testhelper.BiqQueryTest{
		WriteKey:    testhelper.RandString(27),
		Credentials: bqCredentials(),
		EventsMap: testhelper.EventsCountMap{
			"identifies":    1,
			"users":         1,
			"tracks":        1,
			"product_track": 1,
			"pages":         1,
			"screens":       1,
			"aliases":       1,
			"groups":        1,
			"_groups":       1,
			"gateway":       6,
			"batchRT":       8,
		},
		Context:            context.Background(),
		Tables:             []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "_groups"},
		PrimaryKeys:        []string{"user_id", "id", "user_id", "user_id", "user_id", "user_id", "user_id", "user_id"},
		TableTestQueryFreq: 5000 * time.Millisecond,
	}

	var err error

	//Convert Map to Bytes(which can easily be converted to JSON string)
	credentials, err := json.Marshal(bqTest.Credentials.Credentials)
	if err != nil {
		log.Panicf("Error while unmarshalling credentials for bigquery with error: %s", err.Error())
	}

	bqTest.Credentials.CredentialsEscaped, err = testhelper.JsonEscape(string(credentials))
	if err != nil {
		log.Panicf("Error while doing json ecape for bigquery with error: %s", err.Error())
		return
	}

	operation := func() error {
		var err error
		bqTest.DB, err = bigquery.Connect(bqTest.Context,
			&bigquery.BQCredentialsT{
				ProjectID:   bqTest.Credentials.ProjectID,
				Credentials: string(credentials),
			})
		return err
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), uint64(5))
	if err = backoff.Retry(operation, backoffWithMaxRetry); err != nil {
		log.Panicf("could not connect to warehouse bigquery with error: %s", err.Error())
	}
	return
}

func TestBigQuery(t *testing.T) {
	if runBigQueryTest == false {
		t.Skip("Big query integration skipped. use -bigqueryintegration to add this test ")
	}

	t.Parallel()
	//Disabling big query dedup
	config.SetBool("Warehouse.bigquery.isDedupEnabled", false)
	bigquery.Init()
	bqTest := BQTest
	randomness := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")

	whDestTest := &testhelper.WareHouseDestinationTest{
		Client: &client.Client{
			BQ:   bqTest.DB,
			Type: client.BQClient,
		},
		EventsCountMap:     bqTest.EventsMap,
		WriteKey:           bqTest.WriteKey,
		UserId:             fmt.Sprintf("userId_bq_%s", randomness),
		Schema:             "rudderstack_sample_http_source",
		BQContext:          bqTest.Context,
		Tables:             bqTest.Tables,
		PrimaryKeys:        bqTest.PrimaryKeys,
		TableTestQueryFreq: bqTest.TableTestQueryFreq,
	}

	whDestTest.MessageId = uuid.Must(uuid.NewV4()).String()

	whDestTest.EventsCountMap = testhelper.EventsCountMap{
		"identifies": 2,
		"tracks":     2,
		"pages":      2,
		"screens":    2,
		"aliases":    2,
		"groups":     2,
	}
	sendEvents(whDestTest)

	whDestTest.EventsCountMap = testhelper.EventsCountMap{
		"identifies":    2,
		"users":         1,
		"tracks":        2,
		"product_track": 2,
		"pages":         2,
		"screens":       2,
		"aliases":       2,
		"_groups":       2,
		"gateway":       12,
		"batchRT":       16,
	}

	destinationTest(t, whDestTest)

	//Enabling big query dedup
	config.SetBool("Warehouse.bigquery.isDedupEnabled", true)
	bigquery.Init()

	whDestTest.EventsCountMap = testhelper.EventsCountMap{
		"identifies": 2,
		"tracks":     2,
		"pages":      2,
		"screens":    2,
		"aliases":    2,
		"groups":     2,
	}

	sendEvents(whDestTest)

	whDestTest.EventsCountMap = testhelper.EventsCountMap{
		"identifies":    2,
		"users":         1,
		"tracks":        2,
		"product_track": 2,
		"pages":         2,
		"screens":       2,
		"aliases":       2,
		"_groups":       2,
		"gateway":       24,
		"batchRT":       32,
	}

	destinationTest(t, whDestTest)
}
