package googlepubsub

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

const (
	projectId = "my-project-id"
	topic     = "my-topic"
)

func Test_Timeout(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	testConfig, err := SetupTestGooglePubSub(pool, t)
	require.NoError(t, err)

	config := map[string]interface{}{
		"ProjectId": projectId,
		"EventToTopicMap": []map[string]string{
			{"to": topic},
		},
		"TestConfig": testConfig,
	}
	destination := backendconfig.DestinationT{Config: config}

	producer, err := NewProducer(&destination, common.Opts{Timeout: 1 * time.Microsecond})
	if err != nil {
		t.Fatalf("Expected no error, got: %s.", err)
	}
	json := `{"topicId": "my-topic", "message": "{}"}`
	statusCode, respStatus, responseMessage := producer.Produce([]byte(json), nil)

	const expectedStatusCode = 504
	if statusCode != expectedStatusCode {
		t.Errorf("Expected status code %d, got %d.", expectedStatusCode, statusCode)
	}

	const expectedRespStatus = "Failure"
	if respStatus != expectedRespStatus {
		t.Errorf("Expected response status %s, got %s.", expectedRespStatus, respStatus)
	}

	const expectedResponseMessage = "[GooglePubSub] error :: Failed to publish:context deadline exceeded"
	if responseMessage != expectedResponseMessage {
		t.Errorf("Expected response message %s, got %s.", expectedResponseMessage, responseMessage)
	}
}

func TestUnsupportedCredentials(t *testing.T) {
	config := map[string]interface{}{
		"ProjectId": projectId,
		"EventToTopicMap": []map[string]string{
			{"to": topic},
		},
		"Credentials": "{\"installed\":{\"client_id\":\"1234.apps.googleusercontent.com\",\"project_id\":\"project_id\",\"auth_uri\":\"https://accounts.google.com/o/oauth2/auth\",\"token_uri\":\"https://oauth2.googleapis.com/token\",\"auth_provider_x509_cert_url\":\"https://www.googleapis.com/oauth2/v1/certs\",\"client_secret\":\"client_secret\",\"redirect_uris\":[\"urn:ietf:wg:oauth:2.0:oob\",\"http://localhost\"]}}",
	}
	destination := backendconfig.DestinationT{Config: config}

	_, err := NewProducer(&destination, common.Opts{})

	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "client_credentials.json file is not supported")
}

// TestNewProducer_ConfigurationValidation tests producer creation with various configuration scenarios
func TestNewProducer_ConfigurationValidation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		config      map[string]interface{}
		opts        common.Opts
		wantErr     bool
		errorSubstr string
	}{
		{
			name: "valid configuration with test endpoint",
			config: map[string]interface{}{
				"ProjectId": projectId,
				"EventToTopicMap": []map[string]string{
					{"to": topic},
				},
				"TestConfig": TestConfig{Endpoint: "localhost:8085"},
			},
			opts:    common.Opts{Timeout: 10 * time.Second},
			wantErr: false,
		},
		{
			name: "missing project ID should fail",
			config: map[string]interface{}{
				"EventToTopicMap": []map[string]string{
					{"to": topic},
				},
			},
			opts:        common.Opts{Timeout: 10 * time.Second},
			wantErr:     true,
			errorSubstr: "missing projectId",
		},
		{
			name: "empty project ID should fail",
			config: map[string]interface{}{
				"ProjectId": "",
				"EventToTopicMap": []map[string]string{
					{"to": topic},
				},
			},
			opts:        common.Opts{Timeout: 10 * time.Second},
			wantErr:     true,
			errorSubstr: "missing projectId",
		},
		{
			name: "invalid JSON config should fail marshalling",
			config: map[string]interface{}{
				"ProjectId": make(chan int), // This will cause JSON marshaling to fail
			},
			opts:        common.Opts{Timeout: 10 * time.Second},
			wantErr:     true,
			errorSubstr: "marshalling destination config",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			destination := backendconfig.DestinationT{Config: tc.config}
			producer, err := NewProducer(&destination, tc.opts)

			if tc.wantErr {
				require.Error(t, err)
				if tc.errorSubstr != "" {
					require.Contains(t, err.Error(), tc.errorSubstr)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, producer)
			require.NotNil(t, producer.client)
			require.NotNil(t, producer.conf)
		})
	}
}

// TestProduce_MessageValidation tests the Produce method with various message validation scenarios
func TestProduce_MessageValidation(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	testConfig, err := SetupTestGooglePubSub(pool, t)
	require.NoError(t, err)

	baseConfig := map[string]interface{}{
		"ProjectId": projectId,
		"EventToTopicMap": []map[string]string{
			{"to": topic},
		},
		"TestConfig": testConfig,
	}

	testCases := []struct {
		name               string
		jsonData           string
		expectedStatusCode int
		expectedRespStatus string
		expectedMessage    string
	}{
		{
			name:               "valid message should succeed",
			jsonData:           `{"topicId": "my-topic", "message": {"key": "value"}}`,
			expectedStatusCode: 200,
			expectedRespStatus: "Success",
			expectedMessage:    "Message publish with serverID",
		},
		{
			name:               "message with attributes should succeed",
			jsonData:           `{"topicId": "my-topic", "message": {"key": "value"}, "attributes": {"attr1": "value1", "attr2": "value2"}}`,
			expectedStatusCode: 200,
			expectedRespStatus: "Success",
			expectedMessage:    "Message publish with serverID",
		},
		{
			name:               "missing message should fail with 400",
			jsonData:           `{"topicId": "my-topic"}`,
			expectedStatusCode: 400,
			expectedRespStatus: "Failure",
			expectedMessage:    "message from payload not found",
		},
		{
			name:               "missing topic ID should fail with 400",
			jsonData:           `{"message": {"key": "value"}}`,
			expectedStatusCode: 400,
			expectedRespStatus: "Failure",
			expectedMessage:    "Topic Id not found",
		},
		{
			name:               "empty topic ID should fail with 400",
			jsonData:           `{"topicId": "", "message": {"key": "value"}}`,
			expectedStatusCode: 400,
			expectedRespStatus: "Failure",
			expectedMessage:    "empty topic id string",
		},
		{
			name:               "non-string topic ID should fail with 400",
			jsonData:           `{"topicId": 123, "message": {"key": "value"}}`,
			expectedStatusCode: 400,
			expectedRespStatus: "Failure",
			expectedMessage:    "Could not parse topic id to string",
		},
		{
			name:               "topic not found should fail with 400",
			jsonData:           `{"topicId": "non-existent-topic", "message": {"key": "value"}}`,
			expectedStatusCode: 400,
			expectedRespStatus: "Failure",
			expectedMessage:    "Topic not found in project",
		},
		{
			name:               "invalid JSON should fail with 400",
			jsonData:           `invalid json`,
			expectedStatusCode: 400,
			expectedRespStatus: "Failure",
			expectedMessage:    "[GooglePubSub] error",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			destination := backendconfig.DestinationT{Config: baseConfig}
			producer, err := NewProducer(&destination, common.Opts{Timeout: 10 * time.Second})
			require.NoError(t, err)

			statusCode, respStatus, responseMessage := producer.Produce([]byte(tc.jsonData), nil)

			require.Equal(t, tc.expectedStatusCode, statusCode, "Status code mismatch")
			require.Equal(t, tc.expectedRespStatus, respStatus, "Response status mismatch")
			require.Contains(t, responseMessage, tc.expectedMessage, "Response message should contain expected text")
		})
	}
}

// TestRetryConfiguration_Initialization tests that retry configuration is properly initialized
func TestRetryConfiguration_Initialization(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                string
		config              map[string]interface{}
		expectedRetryFields int
	}{
		{
			name: "producer should initialize all retry configuration fields",
			config: map[string]interface{}{
				"ProjectId": projectId,
				"EventToTopicMap": []map[string]string{
					{"to": topic},
				},
				"TestConfig": TestConfig{Endpoint: "localhost:8085"},
			},
			expectedRetryFields: 5, // enableRetry, initialInterval, maxInterval, maxElapsedTime, maxRetries
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			destination := backendconfig.DestinationT{Config: tc.config}
			producer, err := NewProducer(&destination, common.Opts{Timeout: 10 * time.Second})
			require.NoError(t, err)

			// Verify all retry configuration fields are properly initialized
			require.NotNil(t, producer.enableRetry, "enableRetry should be initialized")
			require.NotNil(t, producer.retryInitialInterval, "retryInitialInterval should be initialized")
			require.NotNil(t, producer.retryMaxInterval, "retryMaxInterval should be initialized")
			require.NotNil(t, producer.retryMaxElapsedTime, "retryMaxElapsedTime should be initialized")
			require.NotNil(t, producer.retryMaxRetries, "retryMaxRetries should be initialized")

			// Test that retry configuration can be loaded and has reasonable values
			initialInterval := producer.retryInitialInterval.Load()
			maxInterval := producer.retryMaxInterval.Load()
			maxElapsedTime := producer.retryMaxElapsedTime.Load()
			maxRetries := producer.retryMaxRetries.Load()

			require.Greater(t, initialInterval, time.Duration(0), "Initial interval should be positive")
			require.Greater(t, maxInterval, time.Duration(0), "Max interval should be positive")
			require.Greater(t, maxElapsedTime, time.Duration(0), "Max elapsed time should be positive")
			require.GreaterOrEqual(t, maxRetries, 0, "Max retries should be non-negative")
		})
	}
}

// TestClose_ProducerCleanup tests that the Close method properly cleans up resources
func TestClose_ProducerCleanup(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		config map[string]interface{}
	}{
		{
			name: "close should not panic and handle cleanup properly",
			config: map[string]interface{}{
				"ProjectId": projectId,
				"EventToTopicMap": []map[string]string{
					{"to": topic},
				},
				"TestConfig": TestConfig{Endpoint: "localhost:8085"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			destination := backendconfig.DestinationT{Config: tc.config}
			producer, err := NewProducer(&destination, common.Opts{Timeout: 10 * time.Second})
			require.NoError(t, err)

			// Test that Close doesn't panic and returns no error
			err = producer.Close()
			require.NoError(t, err, "Close should not return an error")
		})
	}
}

// TestGetError_DefaultCase tests the error code mapping function with default case
func TestGetError_DefaultCase(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		err            error
		expectedStatus int
	}{
		{
			name:           "regular error should return 0 (codes.OK)",
			err:            fmt.Errorf("mock error"),
			expectedStatus: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			statusCode := getError(tc.err)
			require.Equal(t, tc.expectedStatus, statusCode, "Status code should match expected value")
		})
	}
}

// TestProducerNilClient_ErrorHandling tests behavior when client is nil
func TestProducerNilClient_ErrorHandling(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name               string
		producer           *GooglePubSubProducer
		jsonData           string
		expectedStatusCode int
		expectedRespStatus string
		expectedMessage    string
	}{
		{
			name: "nil client should return 400 with appropriate error message",
			producer: &GooglePubSubProducer{
				client: nil,
				conf:   nil,
			},
			jsonData:           `{"topicId": "my-topic", "message": {"key": "value"}}`,
			expectedStatusCode: 400,
			expectedRespStatus: "Failure",
			expectedMessage:    "Could not create producer",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			statusCode, respStatus, responseMessage := tc.producer.Produce([]byte(tc.jsonData), nil)

			require.Equal(t, tc.expectedStatusCode, statusCode, "Status code should be 400 for nil client")
			require.Equal(t, tc.expectedRespStatus, respStatus, "Response status should be Failure")
			require.Contains(t, responseMessage, tc.expectedMessage, "Response message should contain expected error text")
		})
	}
}

// TestProducerWithTimeout_Behavior tests producer behavior with different timeout values
func TestProducerWithTimeout_Behavior(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	testConfig, err := SetupTestGooglePubSub(pool, t)
	require.NoError(t, err)

	testCases := []struct {
		name               string
		timeout            time.Duration
		expectedStatusCode int
		expectedBehavior   string
	}{
		{
			name:               "very short timeout should result in 504 gateway timeout",
			timeout:            1 * time.Microsecond,
			expectedStatusCode: 504,
			expectedBehavior:   "Gateway Timeout",
		},
		{
			name:               "reasonable timeout should result in 200 success",
			timeout:            10 * time.Second,
			expectedStatusCode: 200,
			expectedBehavior:   "Success",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := map[string]interface{}{
				"ProjectId": projectId,
				"EventToTopicMap": []map[string]string{
					{"to": topic},
				},
				"TestConfig": testConfig,
			}
			destination := backendconfig.DestinationT{Config: config}
			producer, err := NewProducer(&destination, common.Opts{Timeout: tc.timeout})
			require.NoError(t, err)

			jsonData := `{"topicId": "my-topic", "message": {"key": "value"}}`
			statusCode, _, _ := producer.Produce([]byte(jsonData), nil)

			require.Equal(t, tc.expectedStatusCode, statusCode, "Status code should match expected value for timeout behavior")
		})
	}
}

type cleaner interface {
	Cleanup(func())
	Log(...interface{})
}

func SetupTestGooglePubSub(pool *dockertest.Pool, cln cleaner) (*TestConfig, error) {
	var testConfig TestConfig
	pubsubContainer, err := pool.Run("messagebird/gcloud-pubsub-emulator", "latest", []string{
		"PUBSUB_PROJECT1=my-project-id,my-topic1",
	})
	if err != nil {
		return nil, fmt.Errorf("Could not start resource: %s", err)
	}
	cln.Cleanup(func() {
		if err := pool.Purge(pubsubContainer); err != nil {
			cln.Log(fmt.Errorf("could not purge resource: %v", err))
		}
	})
	testConfig.Endpoint = fmt.Sprintf("127.0.0.1:%s", pubsubContainer.GetPort("8681/tcp"))

	client, err := pubsub.NewClient(
		context.Background(),
		projectId,
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithEndpoint(testConfig.Endpoint))
	if err != nil {
		return nil, err
	}
	if err := pool.Retry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, err = client.CreateTopic(ctx, topic)
		return err
	}); err != nil {
		return nil, err
	}
	return &testConfig, nil
}
