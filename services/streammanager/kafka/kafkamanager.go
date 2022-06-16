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
	"github.com/rudderlabs/rudder-server/services/stats"
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

	getTimeout() time.Duration
}

type producerImpl struct {
	p       *client.Producer
	timeout time.Duration
}

func (p *producerImpl) getTimeout() time.Duration {
	if p.timeout < 1 {
		return defaultPublishTimeout
	}
	return p.timeout
}

func (p *producerImpl) Close(ctx context.Context) error {
	if p == nil || p.p == nil {
		return nil
	}
	return p.p.Close(ctx)
}

func (p *producerImpl) Publish(ctx context.Context, msgs ...client.Message) error {
	return p.p.Publish(ctx, msgs...)
}

type logger interface {
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
}

type managerStats struct {
	creationTime               stats.RudderStats
	creationTimeConfluentCloud stats.RudderStats
	creationTimeAzureEventHubs stats.RudderStats
	missingUserID              stats.RudderStats
	missingMessage             stats.RudderStats
	publishTime                stats.RudderStats
	produceTime                stats.RudderStats
	prepareBatchTime           stats.RudderStats
	closeProducerTime          stats.RudderStats
}

const (
	defaultPublishTimeout = 10 * time.Second
)

var (
	_ producer = &producerImpl{}

	clientCert, clientKey                []byte
	kafkaDialTimeout                     = 10 * time.Second
	kafkaReadTimeout                     = 2 * time.Second
	kafkaWriteTimeout                    = 2 * time.Second
	kafkaBatchingEnabled                 bool
	allowReqsWithoutUserIDAndAnonymousID bool

	kafkaStats managerStats
	pkgLogger  logger

	now   = func() time.Time { return time.Now() }                   // skipcq: CRT-A0018
	since = func(t time.Time) time.Duration { return time.Since(t) } // skipcq: CRT-A0018
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
		2, &kafkaReadTimeout, false, time.Second,
		[]string{"Router.kafkaReadTimeout", "Router.kafkaReadTimeoutInSec"}...,
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
	kafkaStats = managerStats{
		creationTime:               stats.DefaultStats.NewStat("router.kafka.creation_time", stats.TimerType),
		creationTimeConfluentCloud: stats.DefaultStats.NewStat("router.kafka.creation_time_confluent_cloud", stats.TimerType),
		creationTimeAzureEventHubs: stats.DefaultStats.NewStat("router.kafka.creation_time_azure_event_hubs", stats.TimerType),
		missingUserID:              stats.DefaultStats.NewStat("router.kafka.missing_user_id", stats.CountType),
		missingMessage:             stats.DefaultStats.NewStat("router.kafka.missing_message", stats.CountType),
		publishTime:                stats.DefaultStats.NewStat("router.kafka.publish_time", stats.TimerType),
		produceTime:                stats.DefaultStats.NewStat("router.kafka.produce_time", stats.TimerType),
		prepareBatchTime:           stats.DefaultStats.NewStat("router.kafka.prepare_batch_time", stats.TimerType),
		closeProducerTime:          stats.DefaultStats.NewStat("router.kafka.close_producer_time", stats.TimerType),
	}
}

// NewProducer creates a producer based on destination config
func NewProducer(destConfigJSON interface{}, o Opts) (*producerImpl, error) { // skipcq: RVV-B0011
	start := now()
	defer func() { kafkaStats.creationTime.SendTiming(since(start)) }()

	destConfig := configuration{}
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

	hostNames := strings.Split(destConfig.HostName, ",")
	hosts := make([]string, len(hostNames))
	for i, hostName := range hostNames {
		hosts[i] = hostName + ":" + destConfig.Port
	}

	c, err := client.New("tcp", hosts, clientConf)
	if err != nil {
		return nil, fmt.Errorf("could not create client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.TODO(), kafkaDialTimeout)
	defer cancel()
	if err = c.Ping(ctx); err != nil {
		return nil, fmt.Errorf("could not ping: %w", err)
	}

	p, err := c.NewProducer(destConfig.Topic, client.ProducerConfig{
		ReadTimeout:  kafkaReadTimeout,
		WriteTimeout: kafkaWriteTimeout,
	})
	if err != nil {
		return nil, err
	}
	return &producerImpl{p: p, timeout: o.Timeout}, nil
}

// NewProducerForAzureEventHubs creates a producer for Azure event hub based on destination config
func NewProducerForAzureEventHubs(destinationConfig interface{}, o Opts) (*producerImpl, error) { // skipcq: RVV-B0011
	start := now()
	defer func() { kafkaStats.creationTimeAzureEventHubs.SendTiming(since(start)) }()

	destConfig := azureEventHubConfig{}
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

	p, err := c.NewProducer(destConfig.Topic, client.ProducerConfig{
		ReadTimeout:  kafkaReadTimeout,
		WriteTimeout: kafkaWriteTimeout,
	})
	if err != nil {
		return nil, err
	}
	return &producerImpl{p: p, timeout: o.Timeout}, nil
}

// NewProducerForConfluentCloud creates a producer for Confluent cloud based on destination config
func NewProducerForConfluentCloud(destinationConfig interface{}, o Opts) (*producerImpl, error) { // skipcq: RVV-B0011
	start := now()
	defer func() { kafkaStats.creationTimeConfluentCloud.SendTiming(since(start)) }()

	destConfig := confluentCloudConfig{}
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

	p, err := c.NewProducer(destConfig.Topic, client.ProducerConfig{
		ReadTimeout:  kafkaReadTimeout,
		WriteTimeout: kafkaWriteTimeout,
	})
	if err != nil {
		return nil, err
	}
	return &producerImpl{p: p, timeout: o.Timeout}, nil
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
	start := now()
	defer func() { kafkaStats.prepareBatchTime.SendTiming(since(start)) }()

	var messages []client.Message
	for _, data := range batch {
		message, ok := data["message"]
		if !ok {
			kafkaStats.missingMessage.Increment()
			pkgLogger.Errorf("batch from topic %s is missing the message attribute", topic)
			continue
		}
		userID, ok := data["userId"].(string)
		if !ok && !allowReqsWithoutUserIDAndAnonymousID {
			kafkaStats.missingUserID.Increment()
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
	start := now()
	defer func() { kafkaStats.closeProducerTime.SendTiming(since(start)) }()

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
func Produce(jsonData json.RawMessage, pi, destConfig interface{}) (int, string, string) {
	start := now()
	defer func() { kafkaStats.produceTime.SendTiming(since(start)) }()

	p, ok := pi.(producer)
	if !ok {
		return 400, "Could not create producer", "Could not create producer"
	}

	conf := configuration{}
	jsonConfig, err := json.Marshal(destConfig)
	if err != nil {
		return makeErrorResponse(err) // returning 500 for retrying, in case of bad configuration
	}
	err = json.Unmarshal(jsonConfig, &conf)
	if err != nil {
		return makeErrorResponse(err) // returning 500 for retrying, in case of bad configuration
	}

	if conf.Topic == "" {
		return makeErrorResponse(fmt.Errorf("invalid destination configuration: no topic"))
	}

	ctx, cancel := context.WithTimeout(context.TODO(), p.getTimeout())
	defer cancel()
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

	err = publish(ctx, p, batchOfMessages...)
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
	if err = publish(ctx, p, message); err != nil {
		return makeErrorResponse(err)
	}

	returnMessage := fmt.Sprintf("Message delivered to topic: %s", topic)
	return 200, returnMessage, returnMessage
}

func publish(ctx context.Context, p producer, msgs ...client.Message) error {
	start := now()
	defer func() { kafkaStats.publishTime.SendTiming(since(start)) }()
	return p.Publish(ctx, msgs...)
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
