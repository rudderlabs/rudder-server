package streammanager_test

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/config"
	mock_streammanager "github.com/rudderlabs/rudder-server/mocks/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/utils/logger"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/services/streammanager"
	"github.com/rudderlabs/rudder-server/services/streammanager/bqstream"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/services/streammanager/eventbridge"
	"github.com/rudderlabs/rudder-server/services/streammanager/firehose"
	"github.com/rudderlabs/rudder-server/services/streammanager/googlepubsub"
	"github.com/rudderlabs/rudder-server/services/streammanager/kafka"
	"github.com/rudderlabs/rudder-server/services/streammanager/kinesis"
	"github.com/rudderlabs/rudder-server/services/streammanager/lambda"
	"github.com/rudderlabs/rudder-server/services/streammanager/personalize"
	"github.com/stretchr/testify/assert"
)

var (
	once            sync.Once
	streamProducers []string = []string{
		"KINESIS", "KAFKA", "AZURE_EVENT_HUB", "CONFLUENT_CLOUD",
		"PERSONALIZE", "FIREHOSE", "EVENTBRIDGE", "GOOGLEPUBSUB",
		"GOOGLESHEETS", "BQSTREAM", "LAMBDA",
	}
	nonCloseableStreamProducers []string = []string{
		"PERSONALIZE", "FIREHOSE", "EVENTBRIDGE", "KINESIS", "LAMBDA",
	}
	closableStreamProducers []string = []string{
		"BQSTREAM", "KAFKA", "AZURE_EVENT_HUB", "CONFLUENT_CLOUD", "GOOGLEPUBSUB",
	}
)

func initStreamManager() {
	once.Do(func() {
		config.Load()
		logger.Init()
		stats.Setup()
		kafka.Init()
	})
}

func TestStreammanager(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Streammanager Suite")
}

func TestNewProducerWithNilDestination(t *testing.T) {
	initStreamManager()
	producer, err := streammanager.NewProducer(nil, common.Opts{})
	assert.NotNil(t, err)
	assert.Nil(t, producer)
	assert.EqualError(t, err, "destination should not be nil")
}

func TestNewProducerWithEmptyDestination(t *testing.T) {
	initStreamManager()
	producer, err := streammanager.NewProducer(&backendconfig.DestinationT{}, common.Opts{})
	assert.NotNil(t, err)
	assert.Nil(t, producer)
	assert.EqualError(t, err, "no provider configured for StreamManager")
}

func TestNewProducerWithNonStreamingDestinationType(t *testing.T) {
	initStreamManager()
	producer, err := streammanager.NewProducer(&backendconfig.DestinationT{
		DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "someDest"},
	}, common.Opts{})
	assert.NotNil(t, err)
	assert.Nil(t, producer)
	assert.EqualError(t, err, "no provider configured for StreamManager")
}

func TestNewProducerWithAzureEventHubsDestination(t *testing.T) {
	initStreamManager()
	// We want to test if streammanager is using right producer for the given destination type
	_, err := streammanager.NewProducer(
		&backendconfig.DestinationT{
			DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "AZURE_EVENT_HUB"},
			Config:                map[string]interface{}{},
		},
		common.Opts{})
	assert.Error(t, err)
	// error contains "Azure Event Hubs" means we called right producer
	assert.ErrorContains(t, err, "Azure Event Hubs")
}

func TestNewProducerWithConfluentCloudDestination(t *testing.T) {
	initStreamManager()
	_, err := streammanager.NewProducer(
		&backendconfig.DestinationT{
			DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "CONFLUENT_CLOUD"},
			Config:                map[string]interface{}{},
		},
		common.Opts{})
	assert.Error(t, err)
	// error contains "Confluent Cloud" means we called right producer
	assert.ErrorContains(t, err, "Confluent Cloud")
}

func TestNewProducerWithKafkaDestination(t *testing.T) {
	initStreamManager()
	_, err := streammanager.NewProducer(
		&backendconfig.DestinationT{
			DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "KAFKA"},
			Config:                map[string]interface{}{},
		},
		common.Opts{})
	assert.Error(t, err)
	// error contains "Kafka" means we called right producer
	assert.ErrorContains(t, err, "Kafka")
}

func TestNewProducerWithEventBridgeDestination(t *testing.T) {
	initStreamManager()
	producer, err := streammanager.NewProducer(
		&backendconfig.DestinationT{
			DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "EVENTBRIDGE"},
			Config: map[string]interface{}{
				"region": "someRegion",
			},
		},
		common.Opts{})
	assert.Nil(t, err)
	assert.NotNil(t, producer)
	assert.IsType(t, producer, &eventbridge.EventBridgeProducer{})
}

func TestNewProducerWithFirehoseDestination(t *testing.T) {
	initStreamManager()
	producer, err := streammanager.NewProducer(
		&backendconfig.DestinationT{
			DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "FIREHOSE"},
			Config: map[string]interface{}{
				"region": "someRegion",
			},
		},
		common.Opts{})
	assert.Nil(t, err)
	assert.NotNil(t, producer)
	assert.IsType(t, producer, &firehose.FireHoseProducer{})
}

func TestNewProducerWithKinesisDestination(t *testing.T) {
	initStreamManager()
	producer, err := streammanager.NewProducer(
		&backendconfig.DestinationT{
			DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "KINESIS"},
			Config: map[string]interface{}{
				"region": "someRegion",
			},
		},
		common.Opts{})
	assert.Nil(t, err)
	assert.NotNil(t, producer)
	assert.IsType(t, producer, &kinesis.KinesisProducer{})
}

func TestNewProducerWithLambdaDestination(t *testing.T) {
	initStreamManager()
	producer, err := streammanager.NewProducer(
		&backendconfig.DestinationT{
			DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "LAMBDA"},
			Config: map[string]interface{}{
				"region": "someRegion",
			},
		},
		common.Opts{})
	assert.Nil(t, err)
	assert.NotNil(t, producer)
	assert.IsType(t, producer, &lambda.LambdaProducer{})
}

func TestNewProducerWithPersonalizeDestination(t *testing.T) {
	initStreamManager()
	producer, err := streammanager.NewProducer(
		&backendconfig.DestinationT{
			DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "PERSONALIZE"},
			Config: map[string]interface{}{
				"region": "someRegion",
			},
		},
		common.Opts{})
	assert.Nil(t, err)
	assert.NotNil(t, producer)
	assert.IsType(t, producer, &personalize.PersonalizeProducer{})
}

func TestNewProducerWithBQStreamDestination(t *testing.T) {
	initStreamManager()
	producer, err := streammanager.NewProducer(
		&backendconfig.DestinationT{
			DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "BQSTREAM"},
			Config: map[string]interface{}{
				"ProjectId": "someProjectID",
				"Credentials": `
					{
						"type": "service_account",
						"project_id": "",
						"private_key_id": "",
						"private_key": "-----BEGIN PRIVATE KEY----------END PRIVATE KEY-----\n",
						"client_email": "",
						"client_id": "",
						"auth_uri": "https://accounts.google.com/o/oauth2/auth",
						"token_uri": "https://oauth2.googleapis.com/token",
						"auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
						"client_x509_cert_url": ""
					}
				`,
			},
		},
		common.Opts{})
	assert.Nil(t, err)
	assert.NotNil(t, producer)
	assert.IsType(t, producer, &bqstream.BQStreamProducer{})
}

func TestNewProducerWithGooglePubsubDestination(t *testing.T) {
	initStreamManager()
	producer, err := streammanager.NewProducer(
		&backendconfig.DestinationT{
			DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "GOOGLEPUBSUB"},
			Config: map[string]interface{}{
				"ProjectId": "someProjectID",
				"Credentials": `
					{
						"type": "service_account",
						"project_id": "",
						"private_key_id": "",
						"private_key": "-----BEGIN PRIVATE KEY----------END PRIVATE KEY-----\n",
						"client_email": "",
						"client_id": "",
						"auth_uri": "https://accounts.google.com/o/oauth2/auth",
						"token_uri": "https://oauth2.googleapis.com/token",
						"auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
						"client_x509_cert_url": ""
					}
				`,
			},
		},
		common.Opts{})
	assert.Nil(t, err)
	assert.NotNil(t, producer)
	assert.IsType(t, producer, &googlepubsub.GooglePubSubProducer{})
}

func TestNewProducerWithGoogleSheetsDestination(t *testing.T) {
	initStreamManager()
	_, err := streammanager.NewProducer(
		&backendconfig.DestinationT{
			DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "GOOGLESHEETS"},
			Config:                map[string]interface{}{},
		},
		common.Opts{})
	assert.Error(t, err)
	// error contains "Kafka" means we called right producer
	assert.ErrorContains(t, err, "GoogleSheets")
}

func TestProduceWithInvalidDestinationType(t *testing.T) {
	initStreamManager()
	statusCode, statusMsg, respMsg := streammanager.Produce(json.RawMessage{}, "notAStreamingDestination", nil, nil)
	assert.Equal(t, 404, statusCode)
	assert.Equal(t, "No provider configured for StreamManager", statusMsg)
	assert.Equal(t, "No provider configured for StreamManager", respMsg)
}

func TestProduceWithNilProducer(t *testing.T) {
	initStreamManager()
	for _, streamProducer := range streamProducers {
		statusCode, statusMsg, respMsg := streammanager.Produce(json.RawMessage{}, streamProducer, nil, nil)
		assert.Equal(t, 500, statusCode)
		assert.Equal(t, "producer is nil", statusMsg)
		assert.Equal(t, "producer is nil", respMsg)
	}
}

func TestProduceWithValidProducer(t *testing.T) {
	initStreamManager()
	ctrl := gomock.NewController(t)
	mockProducer := mock_streammanager.NewMockStreamProducer(ctrl)
	message := json.RawMessage{}
	config := map[string]interface{}{}
	for _, streamProducer := range streamProducers {
		mockProducer.EXPECT().Produce(message, config).Times(1)
		streammanager.Produce(message, streamProducer, mockProducer, config)
	}
}

func TestCloseWithInvalidDestinationType(t *testing.T) {
	initStreamManager()
	err := streammanager.Close(nil, "notAStreamingDestination")
	assert.EqualError(t, err, "no provider configured for StreamManager")
}

func TestCloseWithNilProducer(t *testing.T) {
	initStreamManager()
	for _, streamProducer := range closableStreamProducers {
		err := streammanager.Close(nil, streamProducer)
		assert.EqualError(t, err, "producer is not closable")
	}
	for _, streamProducer := range nonCloseableStreamProducers {
		assert.Nil(t, streammanager.Close(nil, streamProducer))
	}
}

func TestCloseWithValidProducer(t *testing.T) {
	initStreamManager()
	ctrl := gomock.NewController(t)
	mockProducer := mock_streammanager.NewMockClosableStreamProducer(ctrl)
	for _, streamProducer := range closableStreamProducers {
		mockProducer.EXPECT().Close().Times(1)
		streammanager.Close(mockProducer, streamProducer)
	}
	for _, streamProducer := range nonCloseableStreamProducers {
		streammanager.Close(mockProducer, streamProducer)
	}
}
