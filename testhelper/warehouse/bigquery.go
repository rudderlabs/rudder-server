package warehouse_test

import (
	bq "cloud.google.com/go/bigquery"
	"context"
	"encoding/json"
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"os"
	"time"
)

type BiqQueryTest struct {
	Credentials            *BigQueryCredentials
	DB                     *bq.Client
	Context                context.Context
	EventsMap              EventsCountMap
	WriteKey               string
	Tables                 []string
	PrimaryKeys            []string
	TableTestQueryFreqInMS time.Duration
}
type BigQueryCredentials struct {
	ProjectID          string            `json:"projectID"`
	Credentials        map[string]string `json:"credentials"`
	Location           string            `json:"location"`
	Bucket             string            `json:"bucketName"`
	CredentialsEscaped string
}

func jsonEscape(i string) string {
	b, err := json.Marshal(i)
	if err != nil {
		panic(err)
	}
	// Trim the beginning and trailing " character
	return string(b[1 : len(b)-1])
}

// SetWHBigQueryDestination setup warehouse Big query destination
func SetWHBigQueryDestination() (cleanup func()) {

	cred := os.Getenv("BIGQUERY_INTEGRATION_TEST_USER_CRED")
	if cred == "" {
		panic("ENV variable BIGQUERY_INTEGRATION_TEST_USER_CRED not found ")
	}
	var bqCredentials BigQueryCredentials
	var err error
	err = json.Unmarshal([]byte(cred), &bqCredentials)
	if err != nil {
		panic("")
	}
	Test.BQTest = &BiqQueryTest{
		WriteKey:    randString(27),
		Credentials: &bqCredentials,
		EventsMap: EventsCountMap{
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
		Context:                context.Background(),
		Tables:                 []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "_groups"},
		PrimaryKeys:            []string{"user_id", "id", "user_id", "user_id", "user_id", "user_id", "user_id", "user_id"},
		TableTestQueryFreqInMS: 5000,
	}
	bqTest := Test.BQTest

	//Convert Map to Bytes(which can  easily be converted to JSON string)
	credentials, _ := json.Marshal(bqTest.Credentials.Credentials)
	cleanup = func() {}
	operation := func() error {
		var err error
		bqTest.DB, err = bigquery.Connect(&bigquery.BQCredentialsT{
			ProjectID:   bqTest.Credentials.ProjectID,
			Credentials: string(credentials),
		}, bqTest.Context)
		return err
	}
	bqTest.Credentials.CredentialsEscaped = jsonEscape(string(credentials))

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), uint64(5))
	err = backoff.Retry(operation, backoffWithMaxRetry)

	if err = backoff.Retry(operation, backoffWithMaxRetry); err != nil {
		panic(fmt.Errorf("could not connect to warehouse bigquery with error: %s", err.Error()))
	}
	return
}
