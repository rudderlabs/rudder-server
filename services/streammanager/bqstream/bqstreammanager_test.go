package bqstream

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	mock_logger "github.com/rudderlabs/rudder-server/mocks/utils/logger"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/stretchr/testify/assert"
)

type BigQueryCredentials struct {
	ProjectID   string                 `json:"projectID"`
	Credentials map[string]interface{} `json:"credentials"`
}

func TestTimeout(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockLogger := mock_logger.NewMockLoggerI(mockCtrl)
	mockLogger.EXPECT().Errorf(gomock.Any(), gomock.Any()).AnyTimes()
	pkgLogger = mockLogger

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
	producer, err := NewProducer(&destination, common.Opts{Timeout: 1 * time.Microsecond})
	if err != nil {
		t.Errorf(" %+v", err)
		return
	}

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
	mockCtrl := gomock.NewController(t)
	mockLogger := mock_logger.NewMockLoggerI(mockCtrl)
	mockLogger.EXPECT().Errorf(gomock.Any(), gomock.Any()).AnyTimes()
	pkgLogger = mockLogger

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
	_, err = NewProducer(&destination, common.Opts{Timeout: 1 * time.Microsecond})

	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "incompatible credentials")
}

func TestInvalidCredentials(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockLogger := mock_logger.NewMockLoggerI(mockCtrl)
	mockLogger.EXPECT().Errorf(gomock.Any(), gomock.Any()).AnyTimes()
	pkgLogger = mockLogger

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
	_, err = NewProducer(&destination, common.Opts{Timeout: 1 * time.Microsecond})

	assert.NotNil(t, err)
	assert.EqualError(t, err, "bigquery: constructing client: missing 'type' field in credentials")
}
