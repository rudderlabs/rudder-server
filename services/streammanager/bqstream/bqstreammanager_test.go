package bqstream_test

import (
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_bqstream "github.com/rudderlabs/rudder-server/mocks/services/streammanager/bqstream"

	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-server/services/streammanager/bqstream"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

type BigQueryCredentials struct {
	ProjectID   string                 `json:"projectID"`
	Credentials map[string]interface{} `json:"credentials"`
}

var once sync.Once

func initBQTest() {
	once.Do(func() {
		config.Reset()
		logger.Reset()
		bqstream.Init()
	})
}

func TestTimeout(t *testing.T) {
	initBQTest()
	cred := os.Getenv("BIGQUERY_INTEGRATION_TEST_USER_CRED")
	if cred == "" {
		t.Skip("Skipping bigquery test, since no credentials are available in the environment")
	}
	var bqCredentials BigQueryCredentials
	var err error
	err = json.Unmarshal([]byte(cred), &bqCredentials)
	if err != nil {
		t.Fatalf("could not unmarshal BIGQUERY_INTEGRATION_TEST_USER_CRED: %s", err)
	}
	credentials, _ := json.Marshal(bqCredentials.Credentials)
	config := map[string]interface{}{
		"Credentials": string(credentials),
		"ProjectId":   bqCredentials.ProjectID,
	}
	destination := backendconfig.DestinationT{Config: config}
	opts := common.Opts{Timeout: 1 * time.Microsecond}
	producer, err := bqstream.NewProducer(&destination, opts)
	if err != nil {
		t.Errorf(" %+v", err)
		return
	}

	assert.NotNil(t, producer.Client)
	assert.Equal(t, opts, producer.Opts)

	payload := `{
		"datasetId": "timeout_test",
		"tableId": "rudder",
		"properties": {
			"key": "key",
			"value": "value"
		}
	}`
	statusCode, respStatus, responseMessage := producer.Produce([]byte(payload), nil)

	const expectedStatusCode = 504
	if statusCode != expectedStatusCode {
		t.Errorf("Expected status code %d, got %d.", expectedStatusCode, statusCode)
	}

	const expectedRespStatus = "Failure"
	if respStatus != expectedRespStatus {
		t.Errorf("Expected response status %s, got %s.", expectedRespStatus, respStatus)
	}

	const expectedResponseMessage = "[BQStream] error :: timeout in data insertion:: context deadline exceeded"
	if responseMessage != expectedResponseMessage {
		t.Errorf("Expected response message %s, got %s.", expectedResponseMessage, responseMessage)
	}
}

func TestUnsupportedCredentials(t *testing.T) {
	initBQTest()
	var bqCredentials BigQueryCredentials
	var err error
	err = json.Unmarshal(
		[]byte(`{
			"projectID": "my-project",
			"credentials": {
				"installed": {
					"client_id": "1234.apps.googleusercontent.com",
					"project_id": "project_id",
					"auth_uri": "https://accounts.google.com/o/oauth2/auth",
					"token_uri": "https://oauth2.googleapis.com/token",
					"auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
					"client_secret": "client_secret",
					"redirect_uris": [
						"urn:ietf:wg:oauth:2.0:oob",
						"http://localhost"
					]
				}
			}
		}`), &bqCredentials)
	assert.NoError(t, err)
	credentials, _ := json.Marshal(bqCredentials.Credentials)
	config := map[string]interface{}{
		"Credentials": string(credentials),
		"ProjectId":   bqCredentials.ProjectID,
	}
	destination := backendconfig.DestinationT{Config: config}
	_, err = bqstream.NewProducer(&destination, common.Opts{Timeout: 1 * time.Microsecond})

	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "incompatible credentials")
}

func TestInvalidCredentials(t *testing.T) {
	initBQTest()
	var bqCredentials BigQueryCredentials
	var err error
	err = json.Unmarshal(
		[]byte(`{
			"projectID": "my-project",
			"credentials": {
				"somekey": {
				}
			}
		}`), &bqCredentials)
	assert.NoError(t, err)
	credentials, _ := json.Marshal(bqCredentials.Credentials)
	config := map[string]interface{}{
		"Credentials": string(credentials),
		"ProjectId":   bqCredentials.ProjectID,
	}
	destination := backendconfig.DestinationT{Config: config}
	_, err = bqstream.NewProducer(&destination, common.Opts{Timeout: 1 * time.Microsecond})

	assert.NotNil(t, err)
	assert.EqualError(t, err, "bigquery: constructing client: credentials: unsupported filetype '\\x00'")
}

func TestProduceWithInvalidClient(t *testing.T) {
	initBQTest()
	invalidProducer := bqstream.BQStreamProducer{}
	invalidProducer.Produce([]byte("{}"), map[string]interface{}{})
	statusCode, statusMsg, respMsg := invalidProducer.Produce([]byte("{}"), map[string]interface{}{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Contains(t, respMsg, "invalid client")
}

func TestCloseSuccessfulCase(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_bqstream.NewMockBQClient(ctrl)
	producer := &bqstream.BQStreamProducer{Client: mockClient}
	mockClient.EXPECT().Close().Return(nil)
	assert.Nil(t, producer.Close())
}

func TestCloseFailedCase(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_bqstream.NewMockBQClient(ctrl)
	producer := &bqstream.BQStreamProducer{Client: mockClient}
	mockClient.EXPECT().Close().Return(errors.New("failed close"))
	assert.ErrorContains(t, producer.Close(), "failed close")
}

func TestCloseWithInvalidClient(t *testing.T) {
	producer := &bqstream.BQStreamProducer{}
	assert.ErrorContains(t, producer.Close(), "error while trying to close the client")
}

func TestProduceWithMissingTableId(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_bqstream.NewMockBQClient(ctrl)
	producer := &bqstream.BQStreamProducer{Client: mockClient}

	// properties -> array of objects
	sampleEventJson, _ := json.Marshal(map[string]interface{}{
		"datasetId":  "bigquery_batching",
		"properties": json.RawMessage(`[{"id":"25","name":"rudder"}, {"id":"50","name":"ruddertest"}]`),
	})

	var genericRecs []*bqstream.GenericRecord
	_ = json.Unmarshal([]byte("[{\"id\":\"25\",\"name\":\"rudder\"}, {\"id\":\"50\",\"name\":\"ruddertest\"}]"), &genericRecs)

	mockClient.
		EXPECT().
		Put(gomock.Any(), "bigquery_batching", "", genericRecs).
		Return(errors.New("invalid data"))
	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Contains(t, respMsg, "error in data insertion")
}

func TestProduceWithArrayOfRecords(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_bqstream.NewMockBQClient(ctrl)
	producer := &bqstream.BQStreamProducer{Client: mockClient}

	// properties -> array of objects
	sampleEventJson, _ := json.Marshal(map[string]interface{}{
		"datasetId":  "bigquery_batching",
		"tableId":    "Streaming",
		"properties": json.RawMessage(`[{"id":"25","name":"rudder"}, {"id":"50","name":"ruddertest"}]`),
	})

	var genericRecs []*bqstream.GenericRecord
	_ = json.Unmarshal([]byte("[{\"id\":\"25\",\"name\":\"rudder\"}, {\"id\":\"50\",\"name\":\"ruddertest\"}]"), &genericRecs)

	mockClient.
		EXPECT().
		Put(gomock.Any(), "bigquery_batching", "Streaming", genericRecs).
		Return(nil)
	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, "Success", statusMsg)
	assert.NotEmpty(t, respMsg)
}

func TestProduceWithWithSingleRecord(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_bqstream.NewMockBQClient(ctrl)
	producer := &bqstream.BQStreamProducer{Client: mockClient}

	// properties -> json objects
	sampleEventJson, _ := json.Marshal(map[string]interface{}{
		"datasetId":  "bigquery_batching",
		"tableId":    "Streaming",
		"properties": json.RawMessage(`{"id":"25","name":"rudder"}`),
	})

	var genericRecs []*bqstream.GenericRecord
	var genericRec *bqstream.GenericRecord
	_ = json.Unmarshal([]byte("{\"id\":\"25\",\"name\":\"rudder\"}"), &genericRec)
	genericRecs = append(genericRecs, genericRec)

	mockClient.
		EXPECT().
		Put(gomock.Any(), "bigquery_batching", "Streaming", genericRecs).
		Return(nil)
	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, "Success", statusMsg)
	assert.NotEmpty(t, respMsg)
}

func TestProduceFailedCase(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_bqstream.NewMockBQClient(ctrl)
	producer := &bqstream.BQStreamProducer{Client: mockClient}

	// properties -> string
	sampleEventJson, _ := json.Marshal(map[string]interface{}{
		"datasetId":  "bigquery_batching",
		"tableId":    "Streaming",
		"properties": json.RawMessage(`"id"`),
	})

	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Contains(t, respMsg, "error in unmarshalling data")
}
