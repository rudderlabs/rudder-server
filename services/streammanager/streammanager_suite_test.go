package streammanager_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"sync"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/stretchr/testify/assert"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager"
	"github.com/rudderlabs/rudder-server/services/streammanager/bqstream"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/services/streammanager/eventbridge"
	"github.com/rudderlabs/rudder-server/services/streammanager/firehose"
	cloudfunctions "github.com/rudderlabs/rudder-server/services/streammanager/googlecloudfunction"
	"github.com/rudderlabs/rudder-server/services/streammanager/googlepubsub"
	"github.com/rudderlabs/rudder-server/services/streammanager/kafka"
	"github.com/rudderlabs/rudder-server/services/streammanager/kinesis"
	"github.com/rudderlabs/rudder-server/services/streammanager/lambda"
	"github.com/rudderlabs/rudder-server/services/streammanager/personalize"
)

var once sync.Once

func initStreamManager() {
	once.Do(func() {
		config.Reset()
		logger.Reset()
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
						"client_email": "foo@barr.com",
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

	reader := rand.Reader
	bitSize := 2048

	key, err := rsa.GenerateKey(reader, bitSize)
	assert.Nil(t, err)

	encoded, err := json.Marshal(string(pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})))
	assert.Nil(t, err)

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
						"private_key": ` + string(encoded) + `,
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

func TestNewProducerWithGoogleCloudFunctionDestination(t *testing.T) {
	initStreamManager()
	producer, err := streammanager.NewProducer(
		&backendconfig.DestinationT{
			DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "GOOGLE_CLOUD_FUNCTION"},
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
					"client_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
					"universe_domain": "googleapis.com"
				}
				`,
				"GoogleCloudFunctionUrl": "https://sample-location-sample-proj-test-poc.cloudfunctions.net/function-x",
			},
		},
		common.Opts{})

	assert.Nil(t, err)
	assert.NotNil(t, producer)
	assert.IsType(t, producer, &cloudfunctions.GoogleCloudFunctionProducer{})
}
