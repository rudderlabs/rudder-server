package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/streammanager/kafka/client"
	rslogger "github.com/rudderlabs/rudder-server/utils/logger"
)

type Opts struct {
	Timeout time.Duration
}

// configuration is the config that is required to send data to Kafka
type configuration struct {
	Topic         string
	HostName      string
	Port          string
	SslEnabled    bool
	CACertificate string
	UseSASL       bool
	SaslType      string
	Username      string
	Password      string
}

// azureEventHubConfig is the config that is required to send data to Azure Event Hub
type azureEventHubConfig struct {
	Topic                     string
	BootstrapServer           string
	EventHubsConnectionString string
}

// confluentCloudConfig is the config that is required to send data to Confluent Cloud
type confluentCloudConfig struct {
	Topic           string
	BootstrapServer string
	APIKey          string
	APISecret       string
}

type producer interface {
	Close(context.Context) error
	Publish(context.Context, ...client.Message) error
}

type logger interface {
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
}

var (
	clientCert, clientKey                []byte
	kafkaDialTimeout                     time.Duration
	kafkaWriteTimeout                    time.Duration
	kafkaProducerRetryInterval           time.Duration
	kafkaProducerMaxRetries              int
	kafkaBatchingEnabled                 bool
	allowReqsWithoutUserIDAndAnonymousID bool

	pkgLogger logger
)

func Init() {
	clientCertFile := config.GetEnv("KAFKA_SSL_CERTIFICATE_FILE_PATH", "")
	clientKeyFile := config.GetEnv("KAFKA_SSL_KEY_FILE_PATH", "")
	if clientCertFile != "" && clientKeyFile != "" {
		var err error
		clientCert, err = ioutil.ReadFile(clientCertFile)
		if err != nil {
			panic(fmt.Errorf("could not read certificate file: %w", err))
		}
		clientKey, err = ioutil.ReadFile(clientKeyFile)
		if err != nil {
			panic(fmt.Errorf("could not read key file: %w", err))
		}
	}

	config.RegisterDurationConfigVariable(
		10, &kafkaDialTimeout, false, time.Second,
		[]string{"Router.kafkaDialTimeout", "Router.kafkaDialTimeoutInSec"}...,
	)
	config.RegisterDurationConfigVariable(
		2, &kafkaWriteTimeout, false, time.Second,
		[]string{"Router.kafkaWriteTimeout", "Router.kafkaWriteTimeoutInSec"}...,
	)
	config.RegisterDurationConfigVariable(
		1, &kafkaProducerRetryInterval, false, time.Second,
		[]string{"Router.kafkaProducerRetryInterval", "Router.kafkaProducerRetryIntervalInSec"}...,
	)
	config.RegisterIntConfigVariable(10, &kafkaProducerMaxRetries, false, 1, "Router.kafkaProducerMaxRetries")
	config.RegisterBoolConfigVariable(false, &kafkaBatchingEnabled, false, "Router.KAFKA.enableBatching")
	config.RegisterBoolConfigVariable(
		false, &allowReqsWithoutUserIDAndAnonymousID, true, "Gateway.allowReqsWithoutUserIDAndAnonymousID",
	)

	pkgLogger = rslogger.NewLogger().Child("streammanager").Child("kafka")
}

// NewProducer creates a producer based on destination config
func NewProducer(destConfigJSON interface{}, o Opts) (*client.Producer, error) {
	var destConfig = configuration{}
	jsonConfig, err := json.Marshal(destConfigJSON)
	if err != nil {
		return nil, fmt.Errorf(
			"[Kafka] Error while marshaling destination configuration %+v, with Error: %w",
			destConfigJSON, err,
		)
	}
	err = json.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return nil, fmt.Errorf("[Kafka] Error while unmarshalling dest config: %w", err)
	}

	clientConf := client.Config{}
	if destConfig.SslEnabled {
		if destConfig.CACertificate != "" {
			clientConf.TLS = &client.TLS{
				CACertificate: []byte(destConfig.CACertificate),
				Cert:          clientCert,
				Key:           clientKey,
			}
		} else {
			clientConf.TLS = &client.TLS{WithSystemCertPool: true}
		}

		if destConfig.UseSASL { // SASL is enabled only with SSL
			clientConf.SASL = &client.SASL{
				Username: destConfig.Username,
				Password: destConfig.Password,
			}
			clientConf.SASL.ScramHashGen, err = client.ScramHashGeneratorFromString(destConfig.SaslType)
			if err != nil {
				return nil, fmt.Errorf("[Kafka] invalid SASL type: %w", err)
			}
		}
	}

	var c *client.Client
	// The only component that supports multiple hosts is the consumer which we are not using here.
	// It is also worth mentioning that internally the list of brokers should be populated accordingly anyway given
	// that Kafka uses the "advertised.listeners" property to let clients know where they can connect.
	// see: https://kafka.apache.org/documentation/#brokerconfigs_advertised.listeners
	for _, host := range strings.Split(destConfig.HostName, ",") {
		c, err = client.New("tcp", host+":"+destConfig.Port, clientConf)
		if err != nil {
			continue
		}
		if err = c.Ping(context.TODO()); err != nil {
			continue
		}

		return c.NewProducer(destConfig.Topic, client.ProducerConfig{
			WriteTimeout:  o.Timeout,
			RetryInterval: kafkaProducerRetryInterval,
			MaxRetries:    kafkaProducerMaxRetries,
		})
	}

	return nil, fmt.Errorf("could not create producer [%q at %s]: %w", destConfig.HostName, destConfig.Port, err)
}

// NewProducerForAzureEventHubs creates a producer for Azure event hub based on destination config
func NewProducerForAzureEventHubs(destinationConfig interface{}, o Opts) (*client.Producer, error) {
	var destConfig = azureEventHubConfig{}
	jsonConfig, err := json.Marshal(destinationConfig)
	if err != nil {
		return nil, fmt.Errorf(
			"[Azure Event Hubs] Cannot marshaling destination configuration %+v, got error: %w",
			destinationConfig, err,
		)
	}
	err = json.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return nil, fmt.Errorf(
			"[Azure Event Hubs] Cannot unmarshaling destination configuration %+v, got error: %w",
			destinationConfig, err,
		)
	}

	c, err := client.NewAzureEventHubs(
		destConfig.BootstrapServer, destConfig.EventHubsConnectionString, client.Config{},
	)
	if err != nil {
		return nil, fmt.Errorf("[Azure Event Hubs] Cannot create client: %w", err)
	}
	if err = c.Ping(context.TODO()); err != nil {
		return nil, fmt.Errorf("[Azure Event Hubs] Cannot connect: %w", err)
	}

	return c.NewProducer(destConfig.Topic, client.ProducerConfig{
		WriteTimeout:  o.Timeout,
		RetryInterval: kafkaProducerRetryInterval,
		MaxRetries:    kafkaProducerMaxRetries,
	})
}

// NewProducerForConfluentCloud creates a producer for Confluent cloud based on destination config
func NewProducerForConfluentCloud(destinationConfig interface{}, o Opts) (*client.Producer, error) {
	var destConfig = confluentCloudConfig{}
	jsonConfig, err := json.Marshal(destinationConfig)
	if err != nil {
		return nil, fmt.Errorf("[Confluent Cloud] Error while marshalling destination config: %w", err)
	}

	err = json.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return nil, fmt.Errorf("[Confluent Cloud] Error while unmarshalling destination config: %w", err)
	}

	c, err := client.NewConfluentCloud(
		destConfig.BootstrapServer, destConfig.APIKey, destConfig.APISecret, client.Config{},
	)
	if err != nil {
		return nil, fmt.Errorf("[Confluent Cloud] Cannot create client: %w", err)
	}
	if err = c.Ping(context.TODO()); err != nil {
		return nil, fmt.Errorf("[Confluent Cloud] Cannot connect: %w", err)
	}

	return c.NewProducer(destConfig.Topic, client.ProducerConfig{
		WriteTimeout:  o.Timeout,
		RetryInterval: kafkaProducerRetryInterval,
		MaxRetries:    kafkaProducerMaxRetries,
	})
}

func prepareMessage(topic, key string, message []byte, timestamp time.Time) client.Message {
	return client.Message{
		Topic:     topic,
		Key:       []byte(key),
		Value:     message,
		Timestamp: timestamp,
	}
}

func prepareBatchOfMessages(topic string, batch []map[string]interface{}, timestamp time.Time) (
	[]client.Message, error,
) {
	var messages []client.Message
	for _, data := range batch {
		message, ok := data["message"]
		if !ok {
			// TODO bump metric
			pkgLogger.Errorf("batch from topic %s is missing the message attribute", topic)
			continue
		}
		userID, ok := data["userId"].(string)
		if !ok && !allowReqsWithoutUserIDAndAnonymousID {
			// TODO bump metric
			pkgLogger.Errorf("batch from topic %s is missing the userId attribute", topic)
			continue
		}

		marshalledMsg, err := json.Marshal(message)
		if err != nil {
			return nil, err
		}
		messages = append(messages, prepareMessage(topic, userID, marshalledMsg, timestamp))
	}
	return messages, nil
}

// CloseProducer closes a given producer
func CloseProducer(ctx context.Context, pi interface{}) error {
	p, ok := pi.(producer)
	if !ok {
		return fmt.Errorf("error while closing producer")
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err := p.Close(ctx)
	cancel()
	if err != nil {
		pkgLogger.Errorf("error in closing Kafka producer: %v", err)
	}
	return err
}

// Produce creates a producer and send data to Kafka.
func Produce(jsonData json.RawMessage, pi interface{}, destConfig interface{}) (int, string, string) {
	p, ok := pi.(producer)
	if !ok {
		return 400, "Could not create producer", "Could not create producer"
	}

	var conf = configuration{}
	jsonConfig, err := json.Marshal(destConfig)
	if err != nil {
		return makeErrorResponse(err) //returning 500 for retrying, in case of bad configuration
	}
	err = json.Unmarshal(jsonConfig, &conf)
	if err != nil {
		return makeErrorResponse(err) //returning 500 for retrying, in case of bad configuration
	}

	if conf.Topic == "" {
		return makeErrorResponse(fmt.Errorf("invalid destination configuration: no topic"))
	}

	if kafkaBatchingEnabled {
		return sendBatchedMessage(jsonData, p, conf.Topic)
	}

	return sendMessage(jsonData, p, conf.Topic)
}

func sendBatchedMessage(jsonData json.RawMessage, p producer, topic string) (int, string, string) {
	var batch []map[string]interface{}
	err := json.Unmarshal(jsonData, &batch)
	if err != nil {
		return 400, "Failure", "Error while unmarshalling json data: " + err.Error()
	}

	timestamp := time.Now()
	batchOfMessages, err := prepareBatchOfMessages(topic, batch, timestamp)
	if err != nil {
		return 400, "Failure", "Error while preparing batched message: " + err.Error()
	}

	err = p.Publish(context.TODO(), batchOfMessages...)
	if err != nil {
		return makeErrorResponse(err) // would retry the messages in batch in case brokers are down
	}

	returnMessage := "Kafka: Message delivered in batch"
	return 200, returnMessage, returnMessage
}

func sendMessage(jsonData json.RawMessage, p producer, topic string) (int, string, string) {
	parsedJSON := gjson.ParseBytes(jsonData)
	messageValue := parsedJSON.Get("message").Value()
	if messageValue == nil {
		return 400, "Failure", "Invalid message"
	}

	data := messageValue.(interface{})
	value, err := json.Marshal(data)
	if err != nil {
		return makeErrorResponse(err)
	}

	timestamp := time.Now()
	userID, _ := parsedJSON.Get("userId").Value().(string)
	message := prepareMessage(topic, userID, value, timestamp)
	if err = p.Publish(context.TODO(), message); err != nil {
		return makeErrorResponse(err)
	}

	returnMessage := fmt.Sprintf("Message delivered to topic: %s", topic)
	return 200, returnMessage, returnMessage
}

func makeErrorResponse(err error) (int, string, string) {
	returnMessage := fmt.Sprintf("%s error occurred.", err)
	pkgLogger.Error(returnMessage)
	return getStatusCodeFromError(err), returnMessage, err.Error()
}

// getStatusCodeFromError parses the error and returns the status so that event gets retried or failed.
func getStatusCodeFromError(err error) int {
	statusCode := 500
	errorString := err.Error()

	for _, s := range abortableErrors {
		if strings.Contains(errorString, s) {
			statusCode = 400
			break
		}
	}

	return statusCode
}
