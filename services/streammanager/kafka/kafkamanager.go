package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
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

func (c *configuration) validate() error {
	if c.Topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	if c.HostName == "" {
		return fmt.Errorf("hostname cannot be empty")
	}
	port, err := strconv.Atoi(c.Port)
	if err != nil {
		return fmt.Errorf("invalid port: %w", err)
	}
	if port < 1 {
		return fmt.Errorf("invalid port: %d", port)
	}
	return nil
}

// azureEventHubConfig is the config that is required to send data to Azure Event Hub
type azureEventHubConfig struct {
	Topic                     string
	BootstrapServer           string
	EventHubsConnectionString string
}

func (c *azureEventHubConfig) validate() error {
	if c.Topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	if c.BootstrapServer == "" {
		return fmt.Errorf("bootstrap server cannot be empty")
	}
	if c.EventHubsConnectionString == "" {
		return fmt.Errorf("connection string cannot be empty")
	}
	return nil
}

// confluentCloudConfig is the config that is required to send data to Confluent Cloud
type confluentCloudConfig struct {
	Topic           string
	BootstrapServer string
	APIKey          string
	APISecret       string
}

func (c *confluentCloudConfig) validate() error {
	if c.Topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	if c.BootstrapServer == "" {
		return fmt.Errorf("bootstrap server cannot be empty")
	}
	if c.APIKey == "" {
		return fmt.Errorf("API key cannot be empty")
	}
	if c.APISecret == "" {
		return fmt.Errorf("API secret cannot be empty")
	}
	return nil
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
	kafkaDialTimeout                     = 10 * time.Second
	kafkaWriteTimeout                    = 2 * time.Second
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
			"[Kafka] Error while marshaling destination configuration %+v, got error: %w",
			destConfigJSON, err,
		)
	}
	err = json.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return nil, fmt.Errorf("[Kafka] Error while unmarshalling destination configuration %+v, got error: %w",
			destConfigJSON, err,
		)
	}

	if err = destConfig.validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	clientConf := client.Config{
		DialTimeout: kafkaDialTimeout,
	}
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
		ctx, cancel := context.WithTimeout(context.TODO(), kafkaDialTimeout)
		err = c.Ping(ctx)
		cancel()
		if err != nil {
			continue
		}

		return c.NewProducer(destConfig.Topic, client.ProducerConfig{
			DefaultPublishTimeout: o.Timeout,
			WriteTimeout:          kafkaWriteTimeout,
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
			"[Azure Event Hubs] Error while marshaling destination configuration %+v, got error: %w",
			destinationConfig, err,
		)
	}
	err = json.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return nil, fmt.Errorf(
			"[Azure Event Hubs] Error while unmarshaling destination configuration %+v, got error: %w",
			destinationConfig, err,
		)
	}

	if err = destConfig.validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	c, err := client.NewAzureEventHubs(
		destConfig.BootstrapServer, destConfig.EventHubsConnectionString, client.Config{
			DialTimeout: kafkaDialTimeout,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("[Azure Event Hubs] Cannot create client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.TODO(), kafkaDialTimeout)
	defer cancel()
	if err = c.Ping(ctx); err != nil {
		return nil, fmt.Errorf("[Azure Event Hubs] Cannot connect: %w", err)
	}

	return c.NewProducer(destConfig.Topic, client.ProducerConfig{
		DefaultPublishTimeout: o.Timeout,
		WriteTimeout:          kafkaWriteTimeout,
	})
}

// NewProducerForConfluentCloud creates a producer for Confluent cloud based on destination config
func NewProducerForConfluentCloud(destinationConfig interface{}, o Opts) (*client.Producer, error) {
	var destConfig = confluentCloudConfig{}
	jsonConfig, err := json.Marshal(destinationConfig)
	if err != nil {
		return nil, fmt.Errorf(
			"[Confluent Cloud] Error while marshaling destination configuration %+v, got error: %w",
			destinationConfig, err,
		)
	}

	err = json.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return nil, fmt.Errorf(
			"[Confluent Cloud] Error while unmarshaling destination configuration %+v, got error: %w",
			destinationConfig, err,
		)
	}

	if err = destConfig.validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	c, err := client.NewConfluentCloud(
		destConfig.BootstrapServer, destConfig.APIKey, destConfig.APISecret, client.Config{
			DialTimeout: kafkaDialTimeout,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("[Confluent Cloud] Cannot create client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.TODO(), kafkaDialTimeout)
	defer cancel()
	if err = c.Ping(ctx); err != nil {
		return nil, fmt.Errorf("[Confluent Cloud] Cannot connect: %w", err)
	}

	return c.NewProducer(destConfig.Topic, client.ProducerConfig{
		DefaultPublishTimeout: o.Timeout,
		WriteTimeout:          kafkaWriteTimeout,
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

	ctx := context.TODO()
	if kafkaBatchingEnabled {
		return sendBatchedMessage(ctx, jsonData, p, conf.Topic)
	}

	return sendMessage(ctx, jsonData, p, conf.Topic)
}

func sendBatchedMessage(ctx context.Context, jsonData json.RawMessage, p producer, topic string) (int, string, string) {
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

	err = p.Publish(ctx, batchOfMessages...)
	if err != nil {
		return makeErrorResponse(err) // would retry the messages in batch in case brokers are down
	}

	returnMessage := "Kafka: Message delivered in batch"
	return 200, returnMessage, returnMessage
}

func sendMessage(ctx context.Context, jsonData json.RawMessage, p producer, topic string) (int, string, string) {
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
	if err = p.Publish(ctx, message); err != nil {
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
	if client.IsProducerErrTemporary(err) {
		return 500
	}
	return 400
}
