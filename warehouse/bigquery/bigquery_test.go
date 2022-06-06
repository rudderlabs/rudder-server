package bigquery_test

import (
	"cloud.google.com/go/bigquery"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/config"
	bigquery2 "github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type BiqQueryTest struct {
	Credentials        *BigQueryCredentials
	DB                 *bigquery.Client
	Context            context.Context
	EventsMap          testhelper.EventsCountMap
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

var (
	runBigQueryTest bool
	BQTest          *BiqQueryTest
)

func TestUnsupportedCredentials(t *testing.T) {
	credentials := bigquery2.BQCredentialsT{
		ProjectID:   "projectId",
		Credentials: "{\"installed\":{\"client_id\":\"1234.apps.googleusercontent.com\",\"project_id\":\"project_id\",\"auth_uri\":\"https://accounts.google.com/o/oauth2/auth\",\"token_uri\":\"https://oauth2.googleapis.com/token\",\"auth_provider_x509_cert_url\":\"https://www.googleapis.com/oauth2/v1/certs\",\"client_secret\":\"client_secret\",\"redirect_uris\":[\"urn:ietf:wg:oauth:2.0:oob\",\"http://localhost\"]}}",
	}

	_, err := bigquery2.Connect(context.Background(), &credentials)

	assert.NotNil(t, err)
	assert.EqualError(t, err, "Google Developers Console client_credentials.json file is not supported")
}

func bqCredentials() (bqCredentials *BigQueryCredentials) {
	cred := os.Getenv("BIGQUERY_INTEGRATION_TEST_USER_CRED")
	if cred == "" {
		log.Fatalf("ERROR: ENV variable BIGQUERY_INTEGRATION_TEST_USER_CRED not found ")
	}

	var err error
	err = json.Unmarshal([]byte(cred), &bqCredentials)
	if err != nil {
		log.Fatalf("Could not unmarshal BIGQUERY_INTEGRATION_TEST_USER_CRED.credentials with error: %s", err.Error())
	}

	return
}

func (*BiqQueryTest) EnhanceWorkspaceConfig(configMap map[string]string) {
	configMap["bqEventWriteKey"] = BQTest.WriteKey
	configMap["bqProject"] = BQTest.Credentials.ProjectID
	configMap["bqLocation"] = BQTest.Credentials.Location
	configMap["bqBucketName"] = BQTest.Credentials.Bucket
	configMap["bqCredentials"] = BQTest.Credentials.CredentialsEscaped
}

func (*BiqQueryTest) SetUpDestination() {
	BQTest.WriteKey = testhelper.RandString(27)
	BQTest.Credentials = bqCredentials()
	BQTest.EventsMap = testhelper.EventsCountMap{
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
	}
	BQTest.Context = context.Background()
	BQTest.Tables = []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "_groups"}
	BQTest.PrimaryKeys = []string{"user_id", "id", "user_id", "user_id", "user_id", "user_id", "user_id", "user_id"}
	BQTest.TableTestQueryFreq = 5000 * time.Millisecond

	var err error

	//Convert Map to Bytes(which can easily be converted to JSON string)
	credentials, err := json.Marshal(BQTest.Credentials.Credentials)
	if err != nil {
		log.Panicf("Error while unmarshalling credentials for bigquery with error: %s", err.Error())
	}

	BQTest.Credentials.CredentialsEscaped, err = testhelper.JsonEscape(string(credentials))
	if err != nil {
		log.Panicf("Error while doing json ecape for bigquery with error: %s", err.Error())
		return
	}

	operation := func() error {
		var err error
		BQTest.DB, err = bigquery2.Connect(BQTest.Context,
			&bigquery2.BQCredentialsT{
				ProjectID:   BQTest.Credentials.ProjectID,
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

func TestBigquery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test. Remove -short flag to run integration test.")
	}
	if runBigQueryTest == false {
		t.Skip("Big query integration skipped. use -bigqueryintegration to add this test ")
	}

	t.Skip()
	t.Parallel()

	//Disabling big query dedup
	config.SetBool("Warehouse.bigquery.isDedupEnabled", false)
	bigquery2.Init()
	randomness := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")

	whDestTest := &testhelper.WareHouseDestinationTest{
		Client: &client.Client{
			BQ:   BQTest.DB,
			Type: client.BQClient,
		},
		EventsCountMap:           BQTest.EventsMap,
		WriteKey:                 BQTest.WriteKey,
		UserId:                   fmt.Sprintf("userId_bq_%s", randomness),
		Schema:                   "rudderstack_sample_http_source",
		Tables:                   BQTest.Tables,
		PrimaryKeys:              BQTest.PrimaryKeys,
		VerifyingTablesFrequency: BQTest.TableTestQueryFreq,
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
	testhelper.SendEvents(t, whDestTest)

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

	testhelper.VerifyingDestination(t, whDestTest)

	//Enabling big query dedup
	config.SetBool("Warehouse.bigquery.isDedupEnabled", true)
	bigquery2.Init()

	whDestTest.EventsCountMap = testhelper.EventsCountMap{
		"identifies": 2,
		"tracks":     2,
		"pages":      2,
		"screens":    2,
		"aliases":    2,
		"groups":     2,
	}

	testhelper.SendEvents(t, whDestTest)

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

	testhelper.VerifyingDestination(t, whDestTest)
}

func TestMain(m *testing.M) {
	flag.BoolVar(&runBigQueryTest, "bigqueryintegration", false, "run big query test")
	flag.Parse()

	BQTest = &BiqQueryTest{}

	os.Exit(testhelper.Setup(m, BQTest))
}
