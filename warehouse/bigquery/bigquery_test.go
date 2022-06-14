package bigquery_test

import (
	"cloud.google.com/go/bigquery"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/config"
	bigquery2 "github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

type BigQueryCredentials struct {
	ProjectID          string            `json:"projectID"`
	Credentials        map[string]string `json:"credentials"`
	Location           string            `json:"location"`
	Bucket             string            `json:"bucketName"`
	CredentialsEscaped string
}

type BiqQueryTest struct {
	Credentials *BigQueryCredentials
	DB          *bigquery.Client
	EventsMap   testhelper.EventsCountMap
	WriteKey    string
	Context     context.Context
}

var (
	BQTest *BiqQueryTest
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

func credentials() (bqCredentials *BigQueryCredentials) {
	cred := os.Getenv("BIGQUERY_INTEGRATION_TEST_USER_CRED")
	if cred == "" {
		log.Panic("Error occurred while getting env variable BIGQUERY_INTEGRATION_TEST_USER_CRED")
	}

	var err error
	err = json.Unmarshal([]byte(cred), &bqCredentials)
	if err != nil {
		log.Panicf("Error occurred while unmarshalling bigquery integration test credentials with error: %s", err.Error())
	}
	return
}

func (*BiqQueryTest) SetUpDestination() {
	BQTest.WriteKey = "J77aX7tLFJ84qYU6UrN8ctecwZt"
	BQTest.Credentials = credentials()
	BQTest.EventsMap = testhelper.DefaultEventMap()
	BQTest.EventsMap["_groups"] = 1
	BQTest.Context = context.Background()

	var err error

	//Convert Map to Bytes(which can easily be converted to JSON string)
	credentials, err := json.Marshal(BQTest.Credentials.Credentials)
	if err != nil {
		log.Panicf("Error while unmarshalling credentials for bigquery with error: %s", err.Error())
	}

	BQTest.Credentials.CredentialsEscaped, err = testhelper.JsonEscape(string(credentials))
	if err != nil {
		log.Panicf("Error while doing json ecape for bigquery with error: %s", err.Error())
	}

	testhelper.ConnectWithBackoff(func() (err error) {
		BQTest.DB, err = bigquery2.Connect(BQTest.Context,
			&bigquery2.BQCredentialsT{
				ProjectID:   BQTest.Credentials.ProjectID,
				Credentials: string(credentials),
			})
		if err != nil {
			err = fmt.Errorf("could not connect to warehouse bigquery with error: %s", err.Error())
			return
		}
		return
	})
}

func TestBigQueryIntegration(t *testing.T) {
	t.Skip()

	//Disabling big query dedup
	config.SetBool("Warehouse.bigquery.isDedupEnabled", false)
	bigquery2.Init()

	whDestTest := &testhelper.WareHouseDestinationTest{
		Client: &client.Client{
			BQ:   BQTest.DB,
			Type: client.BQClient,
		},
		WriteKey:                 BQTest.WriteKey,
		Schema:                   "rudderstack_sample_http_source",
		EventsCountMap:           BQTest.EventsMap,
		VerifyingTablesFrequency: testhelper.LongRunningQueryFrequency,
	}
	whDestTest.Reset(warehouseutils.BQ, true)
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
	BQTest = &BiqQueryTest{}
	os.Exit(testhelper.Run(m, BQTest))
}
