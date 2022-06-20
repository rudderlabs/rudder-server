package warehouse_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	bq "cloud.google.com/go/bigquery"
	"github.com/cenkalti/backoff"

	"github.com/rudderlabs/rudder-server/testhelper/rand"
	"github.com/rudderlabs/rudder-server/warehouse/bigquery"
)

type BiqQueryTest struct {
	Credentials        *BigQueryCredentials
	DB                 *bq.Client
	Context            context.Context
	EventsMap          EventsCountMap
	WriteKey           string
	Tables             []string
	PrimaryKeys        []string
	TableTestQueryFreq time.Duration
}
type BigQueryCredentials struct {
	ProjectID          string            `json:"projectID"`
	Credentials        map[string]string `json:"credentials"`
	Location           string            `json:"location"`
	Bucket             string            `json:"bucketName"`
	CredentialsEscaped string
}

func jsonEscape(i string) (string, error) {
	b, err := json.Marshal(i)
	if err != nil {
		return "", fmt.Errorf("could not escape big query JSON credentials for workspace config with error: %s", err.Error())
	}
	// Trim the beginning and trailing " character
	// TODO:  instead of the below return?
	return strings.Trim(string(b), `"`), nil
}

// SetWHBigQueryDestination setup warehouse Big query destination
func SetWHBigQueryDestination() (cleanup func()) {
	cred := os.Getenv("BIGQUERY_INTEGRATION_TEST_USER_CRED")
	cleanup = func() {
	}
	if cred == "" {
		fmt.Println("ERROR: ENV variable BIGQUERY_INTEGRATION_TEST_USER_CRED not found ")
		return
	}
	var bqCredentials BigQueryCredentials
	var err error
	err = json.Unmarshal([]byte(cred), &bqCredentials)
	if err != nil {
		fmt.Printf("Could not unmarshal  BIGQUERY_INTEGRATION_TEST_USER_CRED.credentials with error: %s", err.Error())
		return
	}
	Test.BQTest = &BiqQueryTest{
		WriteKey:    rand.String(27),
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
		Context:            context.Background(),
		Tables:             []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "_groups"},
		PrimaryKeys:        []string{"user_id", "id", "user_id", "user_id", "user_id", "user_id", "user_id", "user_id"},
		TableTestQueryFreq: 5000 * time.Millisecond,
	}

	bqTest := Test.BQTest

	// Convert Map to Bytes(which can  easily be converted to JSON string)
	credentials, _ := json.Marshal(bqTest.Credentials.Credentials)
	operation := func() error {
		var err error
		bqTest.DB, err = bigquery.Connect(bqTest.Context,
			&bigquery.BQCredentialsT{
				ProjectID:   bqTest.Credentials.ProjectID,
				Credentials: string(credentials),
			})
		return err
	}
	bqTest.Credentials.CredentialsEscaped, err = jsonEscape(string(credentials))
	if err != nil {
		fmt.Println(err)
		return
	}
	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), uint64(5))
	if err = backoff.Retry(operation, backoffWithMaxRetry); err != nil {
		fmt.Println(fmt.Errorf("could not connect to warehouse bigquery with error: %s", err.Error()))
	}
	return
}
