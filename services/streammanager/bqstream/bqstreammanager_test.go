package bqstream

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	mock_logger "github.com/rudderlabs/rudder-server/mocks/utils/logger"
)

type BigQueryCredentials struct {
	ProjectID   string            `json:"projectID"`
	Credentials map[string]string `json:"credentials"`
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
	config := Config{
		Credentials: string(credentials),
		ProjectId:   bqCredentials.ProjectID,
	}
	client, err := NewProducer(config, Opts{Timeout: 1 * time.Microsecond})
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
	statusCode, respStatus, responseMessage := Produce([]byte(payload), client, nil)

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
