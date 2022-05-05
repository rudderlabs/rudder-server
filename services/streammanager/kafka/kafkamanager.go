package kafka

import (
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/tidwall/gjson"
	"github.com/xdg/scram"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

// Config is the config that is required to send data to Kafka
type Config struct {
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

//AzureEventHubConfig is the config that is required to send data to Azure Event Hub
type AzureEventHubConfig struct {
	Topic                     string
	BootstrapServer           string
	EventHubsConnectionString string
}

//ConfluentCloudConfig is the config that is required to send data to Confluent Cloud
type ConfluentCloudConfig struct {
	Topic           string
	BootstrapServer string
	APIKey          string
	APISecret       string
}

var (
	clientCertFile, clientKeyFile string
	certificate                   tls.Certificate
	kafkaDialTimeout              time.Duration
	kafkaWriteTimeout             time.Duration
	kafkaBatchingEnabled          bool
)

var (
	SHA256 scram.HashGeneratorFcn = sha256.New
	SHA512 scram.HashGeneratorFcn = sha512.New
)

var pkgLogger logger.LoggerI

const (
	azureEventHubUser = "$ConnectionString"
)

func Init() {
	loadConfig()
	loadCertificate()
	pkgLogger = logger.NewLogger().Child("streammanager").Child("kafka")
}

func loadConfig() {
	clientCertFile = config.GetEnv("KAFKA_SSL_CERTIFICATE_FILE_PATH", "")
	clientKeyFile = config.GetEnv("KAFKA_SSL_KEY_FILE_PATH", "")
	config.RegisterDurationConfigVariable(
		10, &kafkaDialTimeout, false, time.Second,
		[]string{"Router.kafkaDialTimeout", "Router.kafkaDialTimeoutInSec"}...,
	)
	config.RegisterDurationConfigVariable(
		2, &kafkaWriteTimeout, false, time.Second,
		[]string{"Router.kafkaWriteTimeout", "Router.kafkaWriteTimeoutInSec"}...,
	)
	config.RegisterBoolConfigVariable(false, &kafkaBatchingEnabled, false, "Router.KAFKA.enableBatching")
}

func loadCertificate() {
	if clientCertFile != "" && clientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			panic(fmt.Errorf("error loading certificate: %v", err))
		}
		certificate = cert
	}
}

func getDefaultConfiguration() *sarama.Config {
	conf := sarama.NewConfig()
	conf.Net.DialTimeout = kafkaDialTimeout
	conf.Net.WriteTimeout = kafkaWriteTimeout
	conf.Net.ReadTimeout = kafkaWriteTimeout
	conf.Producer.Partitioner = sarama.NewReferenceHashPartitioner
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Return.Successes = true
	conf.Version = sarama.V1_0_0_0
	return conf
}

// Boilerplate needed for SCRAM Authentication in Kafka
type xdgSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *xdgSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *xdgSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *xdgSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

// NewProducer creates a producer based on destination config
func NewProducer(destinationConfig interface{}, o Opts) (sarama.SyncProducer, error) {
	var destConfig = Config{}
	jsonConfig, err := json.Marshal(destinationConfig)
	if err != nil {
		return nil, fmt.Errorf(
			"[Kafka] Error while marshaling destination Config %+v, with Error : %w",
			destinationConfig, err)
	}
	err = json.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return nil, fmt.Errorf("[Kafka] Error while unmarshalling dest config :: %w", err)
	}
	hosts := make([]string, 0)
	hostNames := strings.Split(destConfig.HostName, ",")
	for _, hostName := range hostNames {
		hosts = append(hosts, hostName+":"+destConfig.Port)
	}
	isSslEnabled := destConfig.SslEnabled

	conf := getDefaultConfiguration()
	conf.Producer.Timeout = o.Timeout

	if isSslEnabled {
		caCertificate := destConfig.CACertificate
		conf.Net.TLS.Enable = true
		if caCertificate != "" {
			tlsConfig := newTLSConfig(caCertificate)
			if tlsConfig != nil {
				conf.Net.TLS.Config = tlsConfig
			}
		}
		if destConfig.UseSASL {
			// SASL is enabled only with SSL
			err = setSASLConfig(conf, destConfig)
			if err != nil {
				return nil, fmt.Errorf("[Kafka] Error while setting SASL config :: %w", err)
			}
		}
	}

	return sarama.NewSyncProducer(hosts, conf)
}

// Sets SASL authentication config for Kafka
func setSASLConfig(config *sarama.Config, destConfig Config) (err error) {
	config.Net.SASL.Enable = true
	config.Net.SASL.User = destConfig.Username
	config.Net.SASL.Password = destConfig.Password
	config.Net.SASL.Handshake = true
	switch destConfig.SaslType {
	case "plain":
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	case "sha512":
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &xdgSCRAMClient{HashGeneratorFcn: SHA512}
		}
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	case "sha256":
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &xdgSCRAMClient{HashGeneratorFcn: SHA256}
		}
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
	default:
		return fmt.Errorf("[Kafka] invalid SASL type %s", destConfig.SaslType)
	}
	return nil
}

type Opts struct {
	Timeout time.Duration
}

// NewProducerForAzureEventHub creates a producer for Azure event hub based on destination config
func NewProducerForAzureEventHub(destinationConfig interface{}, o Opts) (sarama.SyncProducer, error) {
	var destConfig = AzureEventHubConfig{}
	jsonConfig, err := json.Marshal(destinationConfig)
	if err != nil {
		return nil, fmt.Errorf(
			"[Confluent Cloud] Error while marshaling destination Config %+v, with Error : %w",
			destinationConfig, err)
	}
	err = json.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return nil, fmt.Errorf(
			"[Confluent Cloud] Error while UnMarshaling destination Config %+v, with Error : %w",
			destinationConfig, err)
	}

	hostName := destConfig.BootstrapServer
	hosts := []string{hostName}

	conf := getDefaultConfiguration()

	conf.Net.SASL.Enable = true
	conf.Net.SASL.User = azureEventHubUser
	conf.Net.SASL.Password = destConfig.EventHubsConnectionString
	conf.Net.SASL.Mechanism = sarama.SASLTypePlaintext

	conf.Net.TLS.Enable = true
	conf.Net.TLS.Config = &tls.Config{ // skipcq: GO-S1020
		InsecureSkipVerify: true,
		ClientAuth:         0,
	}

	conf.Producer.Timeout = o.Timeout

	return sarama.NewSyncProducer(hosts, conf)
}

// NewProducerForConfluentCloud creates a producer for Confluent cloud based on destination config
func NewProducerForConfluentCloud(destinationConfig interface{}, o Opts) (sarama.SyncProducer, error) {
	var destConfig = ConfluentCloudConfig{}
	jsonConfig, err := json.Marshal(destinationConfig)
	if err != nil {
		return nil, fmt.Errorf("[Confluent Cloud] Error while marshalling destination config :: %w", err)
	}
	err = json.Unmarshal(jsonConfig, &destConfig)

	if err != nil {
		return nil, fmt.Errorf("[Confluent Cloud] Error while unmarshalling destination config :: %w", err)
	}

	hostName := destConfig.BootstrapServer
	hosts := []string{hostName}

	conf := getDefaultConfiguration()

	conf.Net.SASL.Enable = true
	conf.Net.SASL.User = destConfig.APIKey
	conf.Net.SASL.Password = destConfig.APISecret
	conf.Net.SASL.Mechanism = sarama.SASLTypePlaintext

	conf.Net.TLS.Enable = true
	conf.Net.TLS.Config = &tls.Config{ // skipcq: GO-S1020
		InsecureSkipVerify: true,
		ClientAuth:         0,
	}

	conf.Producer.Timeout = o.Timeout

	return sarama.NewSyncProducer(hosts, conf)
}

func prepareMessage(topic string, key string, message []byte, timestamp time.Time) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.StringEncoder(message),
		Timestamp: timestamp,
	}

	return msg
}

func prepareBatchedMessage(
	topic string, batch []map[string]interface{}, timestamp time.Time,
) (batchMessage []*sarama.ProducerMessage, err error) {
	var batchedMessage []*sarama.ProducerMessage
	for _, data := range batch {
		message, err := json.Marshal(data["message"])
		if err != nil {
			return nil, err
		}
		userId, _ := data["userId"].(string)
		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Key:       sarama.StringEncoder(userId),
			Value:     sarama.StringEncoder(message),
			Timestamp: timestamp,
		}
		batchedMessage = append(batchedMessage, msg)
	}
	return batchedMessage, nil
}

// newTLSConfig generates a TLS configuration used to authenticate on server with certificates.
func newTLSConfig(caCertFile string) *tls.Config {
	tlsConfig := tls.Config{}
	if len(certificate.Certificate) > 0 {
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	caCert := []byte(caCertFile)
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig.RootCAs = caCertPool

	//tlsConfig.BuildNameToCertificate()
	tlsConfig.InsecureSkipVerify = false
	return &tlsConfig
}

// CloseProducer closes a given producer
func CloseProducer(producer interface{}) error {
	kafkaProducer, ok := producer.(sarama.SyncProducer)
	if ok {
		err := kafkaProducer.Close()
		if err != nil {
			pkgLogger.Errorf("error in closing Kafka producer: %s", err.Error())
		}
		return err
	}
	return fmt.Errorf("error while closing producer")
}

// Produce creates a producer and send data to Kafka.
func Produce(jsonData json.RawMessage, producer interface{}, destConfig interface{}) (int, string, string) {
	kafkaProducer, ok := producer.(sarama.SyncProducer)
	if !ok {
		return 400, "Could not create producer", "Could not create producer"
	}

	var conf = Config{}
	jsonConfig, err := json.Marshal(destConfig)
	if err != nil {
		return makeErrorResponse(err) //returning 500 for retrying, in case of bad configuration
	}
	err = json.Unmarshal(jsonConfig, &conf)
	if err != nil {
		return makeErrorResponse(err) //returning 500 for retrying, in case of bad configuration
	}

	topic := conf.Topic

	if kafkaBatchingEnabled {
		return sendBatchedMessage(jsonData, kafkaProducer, topic)
	}

	return sendMessage(jsonData, kafkaProducer, topic)
}

func sendBatchedMessage(
	jsonData json.RawMessage, kafkaProducer sarama.SyncProducer, topic string,
) (int, string, string) {
	timestamp := time.Now()
	var batch []map[string]interface{}
	err := json.Unmarshal(jsonData, &batch)
	if err != nil {
		return 400, "Failure", "Error while unmarshalling json data :: " + err.Error()
	}

	batchedMessage, err := prepareBatchedMessage(topic, batch, timestamp)
	if err != nil {
		return 400, "Failure", "Error while preparing batched message :: " + err.Error()
	}

	err = kafkaProducer.SendMessages(batchedMessage)
	if err != nil {
		return makeErrorResponse(err) // would retry the messages in batch in case brokers are down
	}

	returnMessage := "Kafka: Message delivered in batch"
	statusCode := 200
	errorMessage := returnMessage

	return statusCode, returnMessage, errorMessage
}

func sendMessage(jsonData json.RawMessage, kafkaProducer sarama.SyncProducer, topic string) (int, string, string) {
	timestamp := time.Now()
	parsedJSON := gjson.ParseBytes(jsonData)
	data := parsedJSON.Get("message").Value().(interface{})
	value, err := json.Marshal(data)
	if err != nil {
		return makeErrorResponse(err)
	}
	userID, _ := parsedJSON.Get("userId").Value().(string)

	message := prepareMessage(topic, userID, value, timestamp)

	partition, offset, err := kafkaProducer.SendMessage(message)
	if err != nil {
		return makeErrorResponse(err)
	}

	returnMessage := fmt.Sprintf("Message delivered at Offset: %v , Partition: %v for topic: %s", offset, partition, topic)
	statusCode := 200
	errorMessage := returnMessage

	return statusCode, returnMessage, errorMessage
}

func makeErrorResponse(err error) (int, string, string) {
	returnMessage := fmt.Sprintf("%s error occured.", err.Error())
	statusCode := getStatusCodeFromError(err) //400
	errorMessage := err.Error()
	pkgLogger.Error(returnMessage)
	return statusCode, returnMessage, errorMessage
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
