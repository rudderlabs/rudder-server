package kafka

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	client "github.com/rudderlabs/rudder-go-kit/kafkaclient"
	rslogger "github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/controlplane"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

// schema is the AVRO schema required to convert the data to AVRO
type avroSchema struct {
	SchemaId string
	Schema   string
}

// configuration is the config that is required to send data to Kafka
type configuration struct {
	Topic    string
	HostName string
	Port     string

	SslEnabled    bool
	CACertificate string
	UseSASL       bool
	SaslType      string
	Username      string
	Password      string

	ConvertToAvro     bool
	EmbedAvroSchemaID bool
	AvroSchemas       []avroSchema

	UseSSH  bool
	SSHHost string
	SSHPort string
	SSHUser string
}

func (c *configuration) validate() error {
	if c.Topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	if c.HostName == "" {
		return fmt.Errorf("hostname cannot be empty")
	}
	if err := isValidPort(c.Port); err != nil {
		return fmt.Errorf("invalid port: %w", err)
	}
	if c.UseSSH {
		if c.SSHHost == "" {
			return fmt.Errorf("ssh host cannot be empty")
		}
		if c.SSHUser == "" {
			return fmt.Errorf("ssh user cannot be empty")
		}
		if err := isValidPort(c.SSHPort); err != nil {
			return fmt.Errorf("invalid ssh port: %w", err)
		}
	}
	return nil
}

// azureEventHubConfig is the config that is required to send data to Azure Event Hub.
// Make sure to select at least the Standard tier since the Basic tier does not support Kafka.
type azureEventHubConfig struct {
	// Topic is the name of the Event Hub on Azure (not the Event Hubs Namespace)
	Topic string
	// BootstrapServer should be in the form of "host:port" (the port is usually 9093 on Azure Event Hubs)
	BootstrapServer string
	// EventHubsConnectionString starts with "Endpoint=sb://" and contains the SharedAccessKey
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

type publisher interface {
	Publish(context.Context, ...client.Message) error
}

type producerManager interface {
	io.Closer
	publisher
	getTimeout() time.Duration
	getEmbedAvroSchemaID() bool
	getCodecs() map[string]*goavro.Codec
}

type internalProducer interface {
	publisher
	Close(context.Context) error
}

type ProducerManager struct {
	p                 internalProducer
	timeout           time.Duration
	embedAvroSchemaID bool
	codecs            map[string]*goavro.Codec
}

func (p *ProducerManager) getTimeout() time.Duration {
	if p.timeout < 1 {
		return defaultPublishTimeout
	}
	return p.timeout
}

func (p *ProducerManager) getCodecs() map[string]*goavro.Codec { return p.codecs }
func (p *ProducerManager) getEmbedAvroSchemaID() bool          { return p.embedAvroSchemaID }

type logger interface {
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Infof(format string, args ...interface{})
}

type managerStats struct {
	creationTime               stats.Measurement
	creationTimeConfluentCloud stats.Measurement
	creationTimeAzureEventHubs stats.Measurement
	missingUserID              stats.Measurement
	missingMessage             stats.Measurement
	publishTime                stats.Measurement
	produceTime                stats.Measurement
	prepareBatchTime           stats.Measurement
	closeProducerTime          stats.Measurement
	jsonSerializationMsgErr    stats.Measurement
	avroSerializationErr       stats.Measurement
	batchSize                  stats.Measurement
}

const (
	defaultPublishTimeout = 10 * time.Second
	defaultBatchTimeout   = 1 * time.Second
	defaultBatchSize      = 100
)

var (
	_ producerManager = &ProducerManager{}

	clientCert, clientKey                []byte
	kafkaDialTimeout                     = 10 * time.Second
	kafkaReadTimeout                     = 2 * time.Second
	kafkaWriteTimeout                    = 2 * time.Second
	kafkaBatchTimeout                    = defaultBatchTimeout
	kafkaBatchSize                       = defaultBatchSize
	kafkaBatchingEnabled                 bool
	kafkaCompression                     client.Compression
	allowReqsWithoutUserIDAndAnonymousID config.ValueLoader[bool]

	kafkaStats managerStats
	pkgLogger  logger

	now   = func() time.Time { return time.Now() }                   // skipcq: CRT-A0018
	since = func(t time.Time) time.Duration { return time.Since(t) } // skipcq: CRT-A0018
)

func Init() {
	clientCertFile := config.GetString("KAFKA_SSL_CERTIFICATE_FILE_PATH", "")
	clientKeyFile := config.GetString("KAFKA_SSL_KEY_FILE_PATH", "")
	if clientCertFile != "" && clientKeyFile != "" {
		var err error
		clientCert, err = os.ReadFile(clientCertFile)
		if err != nil {
			panic(fmt.Errorf("could not read certificate file: %w", err))
		}
		clientKey, err = os.ReadFile(clientKeyFile)
		if err != nil {
			panic(fmt.Errorf("could not read key file: %w", err))
		}
	}

	kafkaDialTimeout = config.GetDurationVar(10, time.Second, "Router.kafkaDialTimeout", "Router.kafkaDialTimeoutInSec")
	kafkaReadTimeout = config.GetDurationVar(2, time.Second, "Router.kafkaReadTimeout", "Router.kafkaReadTimeoutInSec")
	kafkaWriteTimeout = config.GetDurationVar(2, time.Second, "Router.kafkaWriteTimeout", "Router.kafkaWriteTimeoutInSec")
	kafkaBatchTimeout = config.GetDurationVar(
		int64(defaultBatchTimeout)/int64(time.Second), time.Second,
		"Router.kafkaBatchTimeout", "Router.kafkaBatchTimeoutInSec",
	)
	kafkaBatchSize = config.GetIntVar(defaultBatchSize, 1, "Router.kafkaBatchSize")
	kafkaBatchingEnabled = config.GetBoolVar(false, "Router.KAFKA.enableBatching")
	allowReqsWithoutUserIDAndAnonymousID = config.GetReloadableBoolVar(false, "Gateway.allowReqsWithoutUserIDAndAnonymousID")

	pkgLogger = rslogger.NewLogger().Child("streammanager").Child("kafka")

	kafkaCompression = client.CompressionNone
	if kafkaBatchingEnabled {
		kafkaCompression = client.CompressionZstd

		pkgLogger.Infof("Kafka batching is enabled with batch size: %d and batch timeout: %s",
			kafkaBatchSize, kafkaBatchTimeout,
		)
	}
	if kc := config.GetInt("Router.kafkaCompression", -1); kc != -1 {
		switch client.Compression(kc) {
		case client.CompressionNone,
			client.CompressionGzip,
			client.CompressionSnappy,
			client.CompressionLz4,
			client.CompressionZstd:
			kafkaCompression = client.Compression(kc)
		default:
			pkgLogger.Errorf("Invalid Kafka compression codec: %d", kc)
		}
	}

	kafkaStats = managerStats{
		creationTime:               stats.Default.NewStat("router.kafka.creation_time", stats.TimerType),
		creationTimeConfluentCloud: stats.Default.NewStat("router.kafka.creation_time_confluent_cloud", stats.TimerType),
		creationTimeAzureEventHubs: stats.Default.NewStat("router.kafka.creation_time_azure_event_hubs", stats.TimerType),
		missingUserID:              stats.Default.NewStat("router.kafka.missing_user_id", stats.CountType),
		missingMessage:             stats.Default.NewStat("router.kafka.missing_message", stats.CountType),
		publishTime:                stats.Default.NewStat("router.kafka.publish_time", stats.TimerType),
		produceTime:                stats.Default.NewStat("router.kafka.produce_time", stats.TimerType),
		prepareBatchTime:           stats.Default.NewStat("router.kafka.prepare_batch_time", stats.TimerType),
		closeProducerTime:          stats.Default.NewStat("router.kafka.close_producer_time", stats.TimerType),
		jsonSerializationMsgErr:    stats.Default.NewStat("router.kafka.json_serialization_msg_err", stats.CountType),
		avroSerializationErr:       stats.Default.NewStat("router.kafka.avro_serialization_err", stats.CountType),
		batchSize:                  stats.Default.NewStat("router.kafka.batch_size", stats.HistogramType),
	}
}

// NewProducer creates a producer based on destination config
func NewProducer(destination *backendconfig.DestinationT, o common.Opts) (*ProducerManager, error) {
	start := now()
	defer func() { kafkaStats.creationTime.SendTiming(since(start)) }()

	destConfig := configuration{}
	jsonConfig, err := json.Marshal(destination.Config)
	if err != nil {
		return nil, fmt.Errorf(
			"[Kafka] Error while marshaling destination configuration %+v, got error: %w",
			destination.Config, err,
		)
	}
	err = json.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return nil, fmt.Errorf("[Kafka] Error while unmarshalling destination configuration %+v, got error: %w",
			destination.Config, err,
		)
	}

	if err = destConfig.validate(); err != nil {
		return nil, fmt.Errorf("[Kafka] invalid configuration: %w", err)
	}

	convertToAvro := destConfig.ConvertToAvro
	avroSchemas := destConfig.AvroSchemas
	var codecs map[string]*goavro.Codec
	if convertToAvro {
		codecs = make(map[string]*goavro.Codec, len(avroSchemas))
		for i, avroSchema := range avroSchemas {
			if avroSchema.SchemaId == "" {
				return nil, fmt.Errorf("length of a schemaId is 0, of index: %d", i)
			}
			newCodec, err := goavro.NewCodec(avroSchema.Schema)
			if err != nil {
				return nil, fmt.Errorf("unable to create codec for schemaId:%+v, with error: %w", avroSchema.SchemaId, err)
			}
			codecs[avroSchema.SchemaId] = newCodec
		}
	}

	// TODO: once the latest control-plane changes are in production we can safely remove this
	sshConfig, err := getSSHConfig(destination.ID, config.Default)
	if err != nil {
		return nil, fmt.Errorf("[Kafka] invalid SSH configuration: %w", err)
	}
	if sshConfig == nil && destConfig.UseSSH {
		privateKey, err := getSSHPrivateKey(context.Background(), destination.ID)
		if err != nil {
			return nil, fmt.Errorf("[Kafka] invalid SSH private key: %w", err)
		}
		sshConfig = &client.SSHConfig{
			Host:       destConfig.SSHHost + ":" + destConfig.SSHPort,
			User:       destConfig.SSHUser,
			PrivateKey: privateKey,
		}
	}

	clientConf := client.Config{
		DialTimeout: kafkaDialTimeout,
		SSHConfig:   sshConfig,
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

	p, err := c.NewProducer(newProducerConfig())
	if err != nil {
		return nil, err
	}

	embedAvroSchemaID := config.GetBool("ROUTER_KAFKA_EMBED_AVRO_SCHEMA_ID_"+strings.ToUpper(destination.ID), false)
	if !embedAvroSchemaID {
		embedAvroSchemaID = destConfig.EmbedAvroSchemaID
	}
	return &ProducerManager{
		p:                 p,
		timeout:           o.Timeout,
		embedAvroSchemaID: embedAvroSchemaID,
		codecs:            codecs,
	}, nil
}

// NewProducerForAzureEventHubs creates a producer for Azure event hub based on destination config
func NewProducerForAzureEventHubs(destination *backendconfig.DestinationT, o common.Opts) (*ProducerManager, error) {
	start := now()
	defer func() { kafkaStats.creationTimeAzureEventHubs.SendTiming(since(start)) }()

	destConfig := azureEventHubConfig{}
	jsonConfig, err := json.Marshal(destination.Config)
	if err != nil {
		return nil, fmt.Errorf(
			"[Azure Event Hubs] Error while marshaling destination configuration %+v, got error: %w",
			destination.Config, err,
		)
	}
	err = json.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return nil, fmt.Errorf(
			"[Azure Event Hubs] Error while unmarshaling destination configuration %+v, got error: %w",
			destination.Config, err,
		)
	}

	if err = destConfig.validate(); err != nil {
		return nil, fmt.Errorf("[Azure Event Hubs] invalid configuration: %w", err)
	}

	addresses := strings.Split(destConfig.BootstrapServer, ",")
	c, err := client.NewAzureEventHubs(
		addresses, destConfig.EventHubsConnectionString, client.Config{
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

	p, err := c.NewProducer(newProducerConfig())
	if err != nil {
		return nil, err
	}
	return &ProducerManager{p: p, timeout: o.Timeout}, nil
}

// NewProducerForConfluentCloud creates a producer for Confluent cloud based on destination config
func NewProducerForConfluentCloud(destination *backendconfig.DestinationT, o common.Opts) (*ProducerManager, error) {
	start := now()
	defer func() { kafkaStats.creationTimeConfluentCloud.SendTiming(since(start)) }()

	destConfig := confluentCloudConfig{}
	jsonConfig, err := json.Marshal(destination.Config)
	if err != nil {
		return nil, fmt.Errorf(
			"[Confluent Cloud] Error while marshaling destination configuration %+v, got error: %w",
			destination.Config, err,
		)
	}

	err = json.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return nil, fmt.Errorf(
			"[Confluent Cloud] Error while unmarshaling destination configuration %+v, got error: %w",
			destination.Config, err,
		)
	}

	if err = destConfig.validate(); err != nil {
		return nil, fmt.Errorf("[Confluent Cloud] invalid configuration: %w", err)
	}

	addresses := strings.Split(destConfig.BootstrapServer, ",")
	c, err := client.NewConfluentCloud(
		addresses, destConfig.APIKey, destConfig.APISecret, client.Config{
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

	p, err := c.NewProducer(newProducerConfig())
	if err != nil {
		return nil, err
	}
	return &ProducerManager{p: p, timeout: o.Timeout}, nil
}

func prepareMessage(topic, key string, message []byte, timestamp time.Time) client.Message {
	return client.Message{
		Topic:     topic,
		Key:       []byte(key),
		Value:     message,
		Timestamp: timestamp,
	}
}

// This function is used to serialize the binary data according to the avroSchema.
// It iterates over the schemas provided by the customer and tries to serialize the data.
// If it's able to serialize the data then it returns the converted data otherwise it returns an error.
// We are using the LinkedIn goavro library for data serialization. Ref: https://github.com/linkedin/goavro
func serializeAvroMessage(schemaID string, embedSchemaID bool, value []byte, codec goavro.Codec) ([]byte, error) {
	native, _, err := codec.NativeFromTextual(value)
	if err != nil {
		return nil, fmt.Errorf("unable convert the event to native from textual, with error: %s", err)
	}
	bin, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("unable convert the event to binary from native, with error: %s", err)
	}

	if !embedSchemaID {
		return bin, nil
	}

	msg, err := addAvroSchemaIDHeader(schemaID, bin)
	if err != nil {
		return nil, fmt.Errorf("unable to add Avro schema ID header: %v", err)
	}
	return msg, nil
}

func addAvroSchemaIDHeader(schemaID string, msgBytes []byte) (header []byte, err error) {
	schemaIDInt, err := strconv.ParseInt(schemaID, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("avro header: unable to convert schemaID %q to int: %v", schemaID, err)
	}

	var buf bytes.Buffer
	err = buf.WriteByte(byte(0x0))
	if err != nil {
		return nil, fmt.Errorf("avro header: unable to write magic byte: %v", err)
	}

	idBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(idBytes, uint32(schemaIDInt))
	_, err = buf.Write(idBytes)
	if err != nil {
		return nil, fmt.Errorf("avro header: unable to write schema id: %v", err)
	}

	_, err = buf.Write(msgBytes)
	if err != nil {
		return nil, fmt.Errorf("avro header: unable to write message bytes: %v", err)
	}

	return buf.Bytes(), nil
}

func prepareBatchOfMessages(batch []map[string]interface{}, timestamp time.Time, p producerManager, defaultTopic string) (
	[]client.Message, error,
) {
	start := now()
	defer func() { kafkaStats.prepareBatchTime.SendTiming(since(start)) }()

	var messages []client.Message
	var errorSamples []error
	addErrorSample := func(msg string, args ...any) {
		err := fmt.Errorf(msg, args...)
		if len(errorSamples) < 3 {
			errorSamples = append(errorSamples, err)
		}
		pkgLogger.Error(err.Error())
	}

	for i, data := range batch {
		topic, ok := data["topic"].(string)
		if !ok || topic == "" {
			topic = defaultTopic
		}

		message, ok := data["message"]
		if !ok {
			kafkaStats.missingMessage.Increment()
			addErrorSample("batch from topic %s is missing the message attribute", topic)
			continue
		}
		userID, ok := data["userId"].(string)
		if !ok && !allowReqsWithoutUserIDAndAnonymousID.Load() {
			kafkaStats.missingUserID.Increment()
			addErrorSample("batch from topic %s is missing the userId attribute", topic)
			continue
		}
		marshalledMsg, err := json.Marshal(message)
		if err != nil {
			kafkaStats.jsonSerializationMsgErr.Increment()
			addErrorSample("unable to marshal message at index %d", i)
			continue
		}
		codecs := p.getCodecs()
		if len(codecs) > 0 {
			schemaId, _ := data["schemaId"].(string)
			if schemaId == "" {
				kafkaStats.avroSerializationErr.Increment()
				addErrorSample("schemaId is not available for the event at index %d", i)
				continue
			}
			codec, ok := codecs[schemaId]
			if !ok {
				kafkaStats.avroSerializationErr.Increment()
				addErrorSample("unable to find schema with schemaId %q", schemaId)
				continue
			}
			marshalledMsg, err = serializeAvroMessage(schemaId, p.getEmbedAvroSchemaID(), marshalledMsg, *codec)
			if err != nil {
				kafkaStats.avroSerializationErr.Increment()
				addErrorSample(
					"unable to serialize the event with schemaId %q at index %d: %s",
					schemaId, i, err,
				)
				continue
			}
		}
		messages = append(messages, prepareMessage(topic, userID, marshalledMsg, timestamp))
	}
	if len(messages) == 0 {
		if len(errorSamples) > 0 {
			return nil, fmt.Errorf("unable to process any of the event in the batch: some errors are: %v", errorSamples)
		}
		return nil, fmt.Errorf("unable to process any of the event in the batch")
	}
	return messages, nil
}

// Close closes a given producer
func (p *ProducerManager) Close() error {
	if p == nil || p.p == nil {
		return nil
	}

	start := now()
	defer func() { kafkaStats.closeProducerTime.SendTiming(since(start)) }()

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()
	if err := p.p.Close(ctx); err != nil {
		return fmt.Errorf("failed to close producer: %w", err)
	}
	return nil
}

// Publish publishes a given message to Kafka
func (p *ProducerManager) Publish(ctx context.Context, msgs ...client.Message) error {
	return p.p.Publish(ctx, msgs...)
}

// Produce creates a producer and send data to Kafka.
func (p *ProducerManager) Produce(jsonData json.RawMessage, destConfig interface{}) (int, string, string) {
	if p.p == nil {
		// return 400 if producer is invalid
		return 400, "Could not create producer", "Could not create producer"
	}
	start := now()
	defer func() { kafkaStats.produceTime.SendTiming(since(start)) }()

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

func sendBatchedMessage(ctx context.Context, jsonData json.RawMessage, p producerManager, defaultTopic string) (int, string, string) {
	var batch []map[string]interface{}
	err := json.Unmarshal(jsonData, &batch)
	if err != nil {
		return 400, "Failure", "Error while unmarshalling json data: " + err.Error()
	}

	timestamp := time.Now()
	batchOfMessages, err := prepareBatchOfMessages(batch, timestamp, p, defaultTopic)
	if err != nil {
		return 400, "Failure", "Error while preparing batched message: " + err.Error()
	}

	err = publish(ctx, p, batchOfMessages...)
	if err != nil {
		return makeErrorResponse(err) // would retry the messages in batch in case brokers are down
	}

	kafkaStats.batchSize.Observe(float64(len(batchOfMessages)))

	returnMessage := "Kafka: Message delivered in batch"
	return 200, returnMessage, returnMessage
}

func sendMessage(ctx context.Context, jsonData json.RawMessage, p producerManager, defaultTopic string) (int, string, string) {
	parsedJSON := gjson.ParseBytes(jsonData)
	messageValue := parsedJSON.Get("message").Value()
	if messageValue == nil {
		return 400, "Failure", "Invalid message"
	}

	value, err := json.Marshal(messageValue)
	if err != nil {
		return makeErrorResponse(err)
	}

	timestamp := time.Now()
	userID := parsedJSON.Get("userId").String()
	codecs := p.getCodecs()
	if len(codecs) > 0 {
		schemaId := parsedJSON.Get("schemaId").String()
		messageId := parsedJSON.Get("message.messageId").String()
		if schemaId == "" {
			return makeErrorResponse(fmt.Errorf("schemaId is not available for event with messageId: %s", messageId))
		}
		codec, ok := codecs[schemaId]
		if !ok {
			return makeErrorResponse(fmt.Errorf("unable to find schema with ID %v", schemaId))
		}
		value, err = serializeAvroMessage(schemaId, p.getEmbedAvroSchemaID(), value, *codec)
		if err != nil {
			return makeErrorResponse(fmt.Errorf(
				"unable to serialize event with schemaId %q and messageId %s: %s",
				schemaId, messageId, err,
			))
		}
	}

	topic := parsedJSON.Get("topic").String()

	if topic == "" {
		topic = defaultTopic
	}

	message := prepareMessage(topic, userID, value, timestamp)

	if err = publish(ctx, p, message); err != nil {
		return makeErrorResponse(fmt.Errorf("could not publish to %q: %w", topic, err))
	}

	returnMessage := fmt.Sprintf("Message delivered to topic: %s", topic)
	return 200, returnMessage, returnMessage
}

func publish(ctx context.Context, p producerManager, msgs ...client.Message) error {
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

func newProducerConfig() client.ProducerConfig {
	pc := client.ProducerConfig{
		ReadTimeout:  kafkaReadTimeout,
		WriteTimeout: kafkaWriteTimeout,
		Compression:  kafkaCompression,
		Logger:       &client.KafkaLogger{Logger: pkgLogger},
		ErrorLogger:  &client.KafkaLogger{Logger: pkgLogger, IsErrorLogger: true},
	}
	if kafkaBatchingEnabled {
		pc.BatchTimeout = kafkaBatchTimeout
		pc.BatchSize = kafkaBatchSize
	}
	return pc
}

// @TODO getSSHConfig should come from control plane (i.e. destination config)
func getSSHConfig(destinationID string, c *config.Config) (*client.SSHConfig, error) {
	enabled := c.GetString("ROUTER_KAFKA_SSH_ENABLED", "")
	if enabled == "" {
		return nil, nil // nolint
	}

	var found bool
	for _, id := range strings.Split(enabled, ",") {
		if id == destinationID {
			found = true
			break
		}
	}
	if !found {
		return nil, nil // nolint
	}

	privateKey := c.GetString("ROUTER_KAFKA_SSH_PRIVATE_KEY", "")
	if privateKey == "" {
		return nil, fmt.Errorf("kafka SSH private key is not set")
	}

	rawPrivateKey, err := base64.StdEncoding.DecodeString(privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 private key: %w", err)
	}

	return &client.SSHConfig{
		User:       c.GetString("ROUTER_KAFKA_SSH_USER", ""),
		Host:       c.GetString("ROUTER_KAFKA_SSH_HOST", ""),
		PrivateKey: string(rawPrivateKey),
	}, nil
}

func getSSHPrivateKey(ctx context.Context, destinationID string) (string, error) {
	c := controlplane.NewAdminClient(
		config.GetString("CONFIG_BACKEND_URL", "https://api.rudderstack.com"),
		&identity.Admin{
			Username: config.GetString("CP_INTERNAL_API_USERNAME", ""),
			Password: config.GetString("CP_INTERNAL_API_PASSWORD", ""),
		},
	)
	keyPair, err := c.GetDestinationSSHKeyPair(ctx, destinationID)
	return keyPair.PrivateKey, err
}

func isValidPort(p string) error {
	port, err := strconv.Atoi(p)
	if err != nil {
		return err
	}
	if port < 1 || port > 65535 {
		return fmt.Errorf("port not within valid range 1>=p<=65535: %d", port)
	}
	return nil
}
