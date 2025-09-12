package googlesheets

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/oauth2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"

	"github.com/rudderlabs/rudder-go-kit/logger/mock_logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

const (
	sheetId       = "sheetId"
	sheetName     = "sheetName"
	destinationId = "destinationId"
	header1       = "Product Purchased"
	header2       = "Product Value"
)

func Test_Timeout(t *testing.T) {
	pool, err := dockertest.NewPool("")
	pool.MaxWait = 2 * time.Minute
	require.NoError(t, err)

	testConfig, err := SetupTestGoogleSheets(pool, t)
	require.NoError(t, err)

	mockCtrl := gomock.NewController(t)
	mockLogger := mock_logger.NewMockLogger(mockCtrl)
	mockLogger.EXPECT().Errorn(gomock.Any(), gomock.Any()).AnyTimes()
	pkgLogger = mockLogger

	config := map[string]interface{}{
		"SheetId":     sheetId,
		"SheetName":   sheetName,
		"DestID":      destinationId,
		"Credentials": "",
		"EventKeyMap": []map[string]string{
			{"to": header1},
			{"to": header2},
		},
		"TestConfig": testConfig,
	}
	destination := backendconfig.DestinationT{Config: config}
	producer, err := NewProducer(&destination, common.Opts{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf(" %+v", err)
	}
	producer.client.producerOpts = common.Opts{Timeout: 1 * time.Microsecond}
	json := fmt.Sprintf(`{
		"spreadSheetId": "%s",
		"spreadSheet": "%s",
		"message":{
			"0": { "attributeKey": "%s", "attributeValue": "Realme C3" },
			"1": { "attributeKey": "%s", "attributeValue": "5900"}
		}
	}`, sheetId, sheetName, header1, header2)
	statusCode, respStatus, responseMessage := producer.Produce([]byte(json), nil)
	const expectedStatusCode = 504
	if statusCode != expectedStatusCode {
		t.Errorf("Expected status code %d, got %d.", expectedStatusCode, statusCode)
	}

	const expectedRespStatus = "Failure"
	if respStatus != expectedRespStatus {
		t.Errorf("Expected response status %s, got %s.", expectedRespStatus, respStatus)
	}

	const expectedResponseMessage = "[GoogleSheets] error :: Failed to insert Payload :: context deadline exceeded"
	if responseMessage != expectedResponseMessage {
		t.Errorf("Expected response message %s, got %s.", expectedResponseMessage, responseMessage)
	}
}

type cleaner interface {
	Cleanup(func())
	Log(...interface{})
}

func SetupTestGoogleSheets(pool *dockertest.Pool, cln cleaner) (*TestConfig, error) {
	var config TestConfig
	dockerContainer, err := pool.Run("atzoum/simulator-google-sheets", "latest", []string{})
	if err != nil {
		return nil, fmt.Errorf("Could not start resource: %s", err)
	}
	cln.Cleanup(func() {
		if err := pool.Purge(dockerContainer); err != nil {
			cln.Log(fmt.Errorf("could not purge resource: %v", err))
		}
	})
	config.Endpoint = fmt.Sprintf("https://127.0.0.1:%s/", dockerContainer.GetPort("8443/tcp"))
	config.AccessToken = "cd887efc-7c7d-4e8e-9580-f7502123badf"
	config.RefreshToken = "bdbbe5ec-6081-4c6c-8974-9c4abfc0fdcc"

	token := &oauth2.Token{
		AccessToken:  config.AccessToken,
		RefreshToken: config.RefreshToken,
	}
	// skipcq: GO-S1020
	tlsConfig := &tls.Config{
		// skipcq: GSC-G402
		InsecureSkipVerify: true,
	}
	client := oauth2.NewClient(context.Background(), oauth2.StaticTokenSource(token))
	trans := client.Transport.(*oauth2.Transport)
	trans.Base = &http.Transport{TLSClientConfig: tlsConfig}
	sheetService, err := sheets.NewService(context.Background(), option.WithEndpoint(config.Endpoint), option.WithHTTPClient(client))

	if err := pool.Retry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, err = sheetService.Spreadsheets.Get("sheetId").Context(ctx).Do()
		return err
	}); err != nil {
		return nil, fmt.Errorf("Could not connect to Google sheets service")
	}
	return &config, nil
}

// Test suite for Google Sheets integration tests
func TestGoogleSheetsIntegration(t *testing.T) {
	pool, err := dockertest.NewPool("")
	pool.MaxWait = 2 * time.Minute
	require.NoError(t, err)

	testConfig, err := SetupTestGoogleSheets(pool, t)
	require.NoError(t, err)

	// Run all integration tests as subtests
	t.Run("NewClient", func(t *testing.T) {
		testNewClientWithConfig(t, testConfig)
	})

	t.Run("NewProducer", func(t *testing.T) {
		testNewProducerWithConfig(t, testConfig)
	})

	t.Run("Produce", func(t *testing.T) {
		testProduceWithConfig(t, testConfig)
	})

	t.Run("ProduceBatch", func(t *testing.T) {
		testProduceBatchWithConfig(t, testConfig)
	})

	t.Run("ProduceNumeric", func(t *testing.T) {
		testProduceNumericWithConfig(t, testConfig)
	})

	t.Run("ProduceTimeout", func(t *testing.T) {
		testProduceTimeoutWithConfig(t, testConfig)
	})

	t.Run("ClientOptions", func(t *testing.T) {
		testClientOptionsWithConfig(t, testConfig)
	})

	t.Run("HandleServiceError", func(t *testing.T) {
		testHandleServiceErrorWithConfig(t, testConfig)
	})
}

// Helper functions for individual test cases
func testNewClientWithConfig(t *testing.T, testConfig *TestConfig) {
	config := &Config{
		TestConfig: *testConfig,
	}

	serviceOpts := testClientOptions(config)
	producerOpts := common.Opts{Timeout: 30 * time.Second}

	client, err := newClient(serviceOpts, producerOpts)
	require.NoError(t, err)
	require.NotNil(t, client)
	require.Equal(t, serviceOpts, client.serviceOpts)
	require.Equal(t, producerOpts, client.producerOpts)
	require.NotNil(t, client.service)
}

func testNewProducerWithConfig(t *testing.T, testConfig *TestConfig) {
	config := map[string]interface{}{
		"SheetId":     sheetId,
		"SheetName":   sheetName,
		"DestID":      destinationId,
		"Credentials": "",
		"EventKeyMap": []map[string]string{
			{"to": header1},
			{"to": header2},
		},
		"TestConfig": *testConfig,
	}

	destination := backendconfig.DestinationT{Config: config}
	producerOpts := common.Opts{Timeout: 30 * time.Second}

	producer, err := NewProducer(&destination, producerOpts)
	require.NoError(t, err)
	require.NotNil(t, producer)
	require.NotNil(t, producer.client)
	require.Equal(t, producerOpts, producer.client.producerOpts)
	require.NotEmpty(t, producer.client.serviceOpts)
}

func testProduceWithConfig(t *testing.T, testConfig *TestConfig) {
	config := map[string]interface{}{
		"SheetId":     sheetId,
		"SheetName":   sheetName,
		"DestID":      destinationId,
		"Credentials": "",
		"EventKeyMap": []map[string]string{
			{"to": header1},
			{"to": header2},
		},
		"TestConfig": *testConfig,
	}

	destination := backendconfig.DestinationT{Config: config}
	producerOpts := common.Opts{Timeout: 30 * time.Second}

	producer, err := NewProducer(&destination, producerOpts)
	require.NoError(t, err)

	// Test successful data insertion
	jsonData := fmt.Sprintf(`{
		"spreadSheetId": "%s",
		"spreadSheet": "%s",
		"message": {
			"0": { "attributeKey": "%s", "attributeValue": "Realme C3" },
			"1": { "attributeKey": "%s", "attributeValue": "5900" }
		}
	}`, sheetId, sheetName, header1, header2)

	statusCode, respStatus, responseMessage := producer.Produce([]byte(jsonData), nil)
	require.Equal(t, 200, statusCode)
	require.Equal(t, "Success", respStatus)
	require.Contains(t, responseMessage, "[GoogleSheets] :: Message Payload inserted with messageId")
}

func testProduceBatchWithConfig(t *testing.T, testConfig *TestConfig) {
	config := map[string]interface{}{
		"SheetId":     sheetId,
		"SheetName":   sheetName,
		"DestID":      destinationId,
		"Credentials": "",
		"EventKeyMap": []map[string]string{
			{"to": header1},
			{"to": header2},
		},
		"TestConfig": *testConfig,
	}

	destination := backendconfig.DestinationT{Config: config}
	producerOpts := common.Opts{Timeout: 30 * time.Second}

	producer, err := NewProducer(&destination, producerOpts)
	require.NoError(t, err)

	// Test batch data insertion
	jsonData := fmt.Sprintf(`{
		"spreadSheetId": "%s",
		"spreadSheet": "%s",
		"batch": [
			{
				"message": {
					"0": { "attributeKey": "%s", "attributeValue": "Product 1" },
					"1": { "attributeKey": "%s", "attributeValue": "100" }
				}
			},
			{
				"message": {
					"0": { "attributeKey": "%s", "attributeValue": "Product 2" },
					"1": { "attributeKey": "%s", "attributeValue": "200" }
				}
			}
		]
	}`, sheetId, sheetName, header1, header2, header1, header2)

	statusCode, respStatus, responseMessage := producer.Produce([]byte(jsonData), nil)
	require.Equal(t, 200, statusCode)
	require.Equal(t, "Success", respStatus)
	require.Contains(t, responseMessage, "[GoogleSheets] :: Message Payload inserted with messageId")
}

func testProduceNumericWithConfig(t *testing.T, testConfig *TestConfig) {
	config := map[string]interface{}{
		"SheetId":     sheetId,
		"SheetName":   sheetName,
		"DestID":      destinationId,
		"Credentials": "",
		"EventKeyMap": []map[string]string{
			{"to": header1},
			{"to": header2},
		},
		"TestConfig": *testConfig,
	}

	destination := backendconfig.DestinationT{Config: config}
	producerOpts := common.Opts{Timeout: 30 * time.Second}

	producer, err := NewProducer(&destination, producerOpts)
	require.NoError(t, err)

	// Test numeric data handling
	jsonData := fmt.Sprintf(`{
		"spreadSheetId": "%s",
		"spreadSheet": "%s",
		"message": {
			"0": { "attributeKey": "%s", "attributeValue": "Test Product" },
			"1": { "attributeKey": "%s", "attributeValue": 123.45 }
		}
	}`, sheetId, sheetName, header1, header2)

	statusCode, respStatus, responseMessage := producer.Produce([]byte(jsonData), nil)
	require.Equal(t, 200, statusCode)
	require.Equal(t, "Success", respStatus)
	require.Contains(t, responseMessage, "[GoogleSheets] :: Message Payload inserted with messageId")
}

func testProduceTimeoutWithConfig(t *testing.T, testConfig *TestConfig) {
	config := map[string]interface{}{
		"SheetId":     sheetId,
		"SheetName":   sheetName,
		"DestID":      destinationId,
		"Credentials": "",
		"EventKeyMap": []map[string]string{
			{"to": header1},
			{"to": header2},
		},
		"TestConfig": *testConfig,
	}

	destination := backendconfig.DestinationT{Config: config}
	producerOpts := common.Opts{Timeout: 1 * time.Microsecond} // Very short timeout

	_, err := NewProducer(&destination, producerOpts)

	require.Error(t, err)
	require.Contains(t, err.Error(), "context deadline exceeded")
}

func testClientOptionsWithConfig(t *testing.T, testConfig *TestConfig) {
	config := &Config{
		TestConfig: *testConfig,
	}

	serviceOpts := testClientOptions(config)
	require.NotNil(t, serviceOpts)
	require.Len(t, serviceOpts, 2) // Should have WithEndpoint and WithHTTPClient options
}

func testHandleServiceErrorWithConfig(t *testing.T, testConfig *TestConfig) {
	config := map[string]interface{}{
		"SheetId":     sheetId,
		"SheetName":   sheetName,
		"DestID":      destinationId,
		"Credentials": "",
		"EventKeyMap": []map[string]string{
			{"to": header1},
			{"to": header2},
		},
		"TestConfig": *testConfig,
	}

	destination := backendconfig.DestinationT{Config: config}
	producerOpts := common.Opts{Timeout: 30 * time.Second}

	producer, err := NewProducer(&destination, producerOpts)
	require.NoError(t, err)

	// Test various error scenarios
	testCases := []struct {
		name            string
		err             error
		expectedCode    int
		expectedMessage string
	}{
		{
			name:            "context deadline exceeded",
			err:             context.DeadlineExceeded,
			expectedCode:    504,
			expectedMessage: "context deadline exceeded",
		},
		{
			name:            "googleapi error",
			err:             &googleapi.Error{Code: 400, Message: "Bad Request"},
			expectedCode:    400,
			expectedMessage: "Bad Request",
		},
		{
			name:            "quota exceeded error",
			err:             &googleapi.Error{Code: 429, Message: "Quota exceeded for requests"},
			expectedCode:    429,
			expectedMessage: "Quota exceeded for requests",
		},
		{
			name:            "token expired error",
			err:             errors.New("token expired and refresh token is not set"),
			expectedCode:    500,
			expectedMessage: "token expired and refresh token is not set",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			statusCode, responseMessage := producer.handleServiceError(tc.err)
			require.Equal(t, tc.expectedCode, statusCode)
			require.Equal(t, tc.expectedMessage, responseMessage)
		})
	}
}
