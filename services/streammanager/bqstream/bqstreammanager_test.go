package bqstream_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/api/option"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_bqstream "github.com/rudderlabs/rudder-server/mocks/services/streammanager/bqstream"
	"github.com/rudderlabs/rudder-server/services/streammanager/bqstream"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	bqhelper "github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery/testhelper"
)

type BigQueryCredentials struct {
	ProjectID   string         `json:"projectID"`
	Credentials map[string]any `json:"credentials"`
}

var once sync.Once

func initBQTest() {
	once.Do(func() {
		config.Reset()
		logger.Reset()
		bqstream.Init()
	})
}

// TestBQStreamIntegration exercises the producer against real BigQuery. It
// provisions an isolated dataset + table for the run and tears them down
// afterwards, then covers three scenarios: a timeout, a successful uncompressed
// insert and a successful gzip-compressed insert.
func TestBQStreamIntegration(t *testing.T) {
	initBQTest()

	if _, exists := os.LookupEnv(bqhelper.TestKey); !exists {
		if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
			t.Fatalf("%s environment variable not set", bqhelper.TestKey)
		}
		t.Skipf("Skipping %s as %s is not set", t.Name(), bqhelper.TestKey)
	}
	credentials, err := bqhelper.GetBQTestCredentials()
	require.NoError(t, err)

	location := credentials.Location
	if location == "" {
		location = "US"
	}

	ctx := context.Background()

	// Admin client used only to create and tear down the test fixtures.
	adminClient, err := bigquery.NewClient(ctx, credentials.ProjectID, option.WithAuthCredentialsJSON(option.ServiceAccount, []byte(credentials.Credentials)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = adminClient.Close() })

	datasetID := fmt.Sprintf("bqstream_integration_test_%d", time.Now().UnixNano())
	const tableID = "streaming"
	require.NoError(t, adminClient.Dataset(datasetID).Create(ctx, &bigquery.DatasetMetadata{Location: location}))
	t.Cleanup(func() { _ = adminClient.Dataset(datasetID).DeleteWithContents(context.Background()) })
	require.NoError(t, adminClient.Dataset(datasetID).Table(tableID).Create(ctx, &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			{Name: "id", Type: bigquery.StringFieldType},
			{Name: "name", Type: bigquery.StringFieldType},
		},
	}))

	destinationConfig := map[string]any{
		"Credentials": credentials.Credentials,
		"ProjectId":   credentials.ProjectID,
	}
	payload := fmt.Sprintf(`{"datasetId":%q,"tableId":%q,"properties":{"id":"25","name":"rudder"}}`, datasetID, tableID)

	newProducer := func(t *testing.T, compression bool, timeout time.Duration) *bqstream.BQStreamProducer {
		t.Helper()
		config.Set("Router.BQSTREAM.enableCompression", compression)
		t.Cleanup(func() { config.Set("Router.BQSTREAM.enableCompression", false) })

		destination := backendconfig.DestinationT{Config: destinationConfig}
		opts := common.Opts{Timeout: timeout}
		producer, err := bqstream.NewProducer(&destination, opts)
		require.NoError(t, err)
		require.NotNil(t, producer.Client)
		require.Equal(t, opts, producer.Opts)
		t.Cleanup(func() { _ = producer.Close() })
		return producer
	}

	// A freshly created table's streaming buffer can take a moment to warm up, so
	// retry transient failures before asserting the insert succeeded.
	produceUntilSuccess := func(t *testing.T, producer *bqstream.BQStreamProducer) {
		t.Helper()
		var statusCode int
		var respStatus, responseMessage string
		deadline := time.Now().Add(90 * time.Second)
		for {
			statusCode, respStatus, responseMessage = producer.Produce([]byte(payload), nil)
			if statusCode == 200 || time.Now().After(deadline) {
				break
			}
			t.Logf("retrying insert, got %d %s: %s", statusCode, respStatus, responseMessage)
			time.Sleep(3 * time.Second)
		}
		assert.Equal(t, 200, statusCode, responseMessage)
		assert.Equal(t, "Success", respStatus)
		assert.NotEmpty(t, responseMessage)
	}

	t.Run("timeout", func(t *testing.T) {
		producer := newProducer(t, false, 1*time.Microsecond)
		statusCode, respStatus, responseMessage := producer.Produce([]byte(payload), nil)
		assert.Equal(t, 504, statusCode)
		assert.Equal(t, "Failure", respStatus)
		assert.Equal(t, "[BQStream] error :: timeout in data insertion:: context deadline exceeded", responseMessage)
	})

	t.Run("without compression", func(t *testing.T) {
		produceUntilSuccess(t, newProducer(t, false, 30*time.Second))
	})

	t.Run("with compression", func(t *testing.T) {
		produceUntilSuccess(t, newProducer(t, true, 30*time.Second))
	})
}

func TestUnsupportedCredentials(t *testing.T) {
	initBQTest()
	var bqCredentials BigQueryCredentials
	var err error
	err = jsonrs.Unmarshal(
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
	credentials, _ := jsonrs.Marshal(bqCredentials.Credentials)
	config := map[string]any{
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
	err = jsonrs.Unmarshal(
		[]byte(`{
			"projectID": "my-project",
			"credentials": {
				"somekey": {
				}
			}
		}`), &bqCredentials)
	assert.NoError(t, err)
	credentials, _ := jsonrs.Marshal(bqCredentials.Credentials)
	config := map[string]any{
		"Credentials": string(credentials),
		"ProjectId":   bqCredentials.ProjectID,
	}
	destination := backendconfig.DestinationT{Config: config}
	_, err = bqstream.NewProducer(&destination, common.Opts{Timeout: 1 * time.Microsecond})

	assert.NotNil(t, err)
	assert.EqualError(t, err, "[BQStream] error :: incompatible credentials:: unsupported credential type \"\": only service account credentials are supported")
}

func TestProduceWithInvalidClient(t *testing.T) {
	initBQTest()
	invalidProducer := bqstream.BQStreamProducer{}
	invalidProducer.Produce([]byte("{}"), map[string]any{})
	statusCode, statusMsg, respMsg := invalidProducer.Produce([]byte("{}"), map[string]any{})
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
	sampleEventJson, _ := jsonrs.Marshal(map[string]any{
		"datasetId":  "bigquery_batching",
		"properties": json.RawMessage(`[{"id":"25","name":"rudder"}, {"id":"50","name":"ruddertest"}]`),
	})

	var genericRecs []*bqstream.GenericRecord
	_ = jsonrs.Unmarshal([]byte("[{\"id\":\"25\",\"name\":\"rudder\"}, {\"id\":\"50\",\"name\":\"ruddertest\"}]"), &genericRecs)

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
	sampleEventJson, _ := jsonrs.Marshal(map[string]any{
		"datasetId":  "bigquery_batching",
		"tableId":    "Streaming",
		"properties": json.RawMessage(`[{"id":"25","name":"rudder"}, {"id":"50","name":"ruddertest"}]`),
	})

	var genericRecs []*bqstream.GenericRecord
	_ = jsonrs.Unmarshal([]byte("[{\"id\":\"25\",\"name\":\"rudder\"}, {\"id\":\"50\",\"name\":\"ruddertest\"}]"), &genericRecs)

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
	sampleEventJson, _ := jsonrs.Marshal(map[string]any{
		"datasetId":  "bigquery_batching",
		"tableId":    "Streaming",
		"properties": json.RawMessage(`{"id":"25","name":"rudder"}`),
	})

	var genericRecs []*bqstream.GenericRecord
	var genericRec *bqstream.GenericRecord
	_ = jsonrs.Unmarshal([]byte("{\"id\":\"25\",\"name\":\"rudder\"}"), &genericRec)
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
	sampleEventJson, _ := jsonrs.Marshal(map[string]any{
		"datasetId":  "bigquery_batching",
		"tableId":    "Streaming",
		"properties": json.RawMessage(`"id"`),
	})

	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Contains(t, respMsg, "error in unmarshalling data")
}
