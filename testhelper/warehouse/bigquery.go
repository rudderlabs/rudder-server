package warehouse_test

import (
	bq "cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"os"
	"time"
)

type BiqQueryTest struct {
	Credentials *bigquery.BQCredentialsT
	DB          *bq.Client
	Context     context.Context
	EventsMap   EventsCountMap
	WriteKey    string
}

// SetWHBigQueryDestination setup warehouse Big query destination
func SetWHBigQueryDestination() (cleanup func()) {

	fmt.Println(os.Getenv("RSERVER_WAREHOUSE_BIGQUERY_CREDENTIALS"))
	fmt.Println("##################################")

	Test.BQTest = &BiqQueryTest{
		WriteKey: randString(27),
		Credentials: &bigquery.BQCredentialsT{
			ProjectID:   os.Getenv("RSERVER_WAREHOUSE_BIGQUERY_PROJECT"),
			Credentials: os.Getenv("RSERVER_WAREHOUSE_BIGQUERY_CREDENTIALS"),
		},
		EventsMap: EventsCountMap{
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
		Context: context.Background(),
	}
	bqTest := Test.BQTest
	credentials := bqTest.Credentials
	cleanup = func() {}
	var err error

	operation := func() error {
		var err error
		bqTest.DB, err = bigquery.Connect(credentials, bqTest.Context)
		return err
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), uint64(5))
	err = backoff.Retry(operation, backoffWithMaxRetry)

	if err = backoff.Retry(operation, backoffWithMaxRetry); err != nil {
		panic(fmt.Errorf("could not connect to warehouse postgres with error: %s", err.Error()))
	}
	return
}
