package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/tidwall/gjson"
)

// Config is the config that is required to send data to Kafka
type Config struct {
	Topic         string
	HostName      string
	Port          string
	SslEnabled    bool
	CACertificate string
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
	kafkaDialTimeoutInSec         int64
	kafkaWriteTimeoutInSec        int64
)

var pkgLogger logger.LoggerI

const (
	azureEventHubUser = "$ConnectionString"
)

func init() {
	loadConfig()
	loadCertificate()
	pkgLogger = logger.NewLogger().Child("streammanager").Child("kafka")
}

func loadConfig() {
	clientCertFile = config.GetEnv("KAFKA_SSL_CERTIFICATE_FILE_PATH", "")
	clientKeyFile = config.GetEnv("KAFKA_SSL_KEY_FILE_PATH", "")
	kafkaDialTimeoutInSec = config.GetInt64("Router.kafkaDialTimeoutInSec", 10)
	kafkaWriteTimeoutInSec = config.GetInt64("Router.kafkaWriteTimeoutInSec", 2)
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
	config := sarama.NewConfig()
	config.Net.DialTimeout = time.Duration(kafkaDialTimeoutInSec) * time.Second
	config.Net.WriteTimeout = time.Duration(kafkaWriteTimeoutInSec) * time.Second
	config.Net.ReadTimeout = time.Duration(kafkaWriteTimeoutInSec) * time.Second
	config.Producer.Partitioner = sarama.NewReferenceHashPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Version = sarama.V1_0_0_0
	return config
}

// NewProducer creates a producer based on destination config
func NewProducer(destinationConfig interface{}) (sarama.SyncProducer, error) {

	var destConfig = Config{}
	jsonConfig, err := json.Marshal(destinationConfig)
	if err != nil {
		return nil, fmt.Errorf("[Confluent Cloud] Error while marshaling destination Config %+v, with Error : %w", destinationConfig, err)
	}
	err = json.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return nil, fmt.Errorf("Error while unmarshalling dest config :: %w", err)
	}
	hostName := destConfig.HostName + ":" + destConfig.Port
	isSslEnabled := destConfig.SslEnabled

	hosts := []string{hostName}

	config := getDefaultConfiguration()

	if isSslEnabled {
		caCertificate := destConfig.CACertificate
		tlsConfig := NewTLSConfig(caCertificate)
		if tlsConfig != nil {
			config.Net.TLS.Config = tlsConfig
			config.Net.TLS.Enable = true
		}
	}

	producer, err := sarama.NewSyncProducer(hosts, config)

	return producer, err
}

// NewProducerForAzureEventHub creates a producer for Azure event hub based on destination config
func NewProducerForAzureEventHub(destinationConfig interface{}) (sarama.SyncProducer, error) {

	var destConfig = AzureEventHubConfig{}
	jsonConfig, err := json.Marshal(destinationConfig)
	if err != nil {
		return nil, fmt.Errorf("[Confluent Cloud] Error while marshaling destination Config %+v, with Error : %w", destinationConfig, err)
	}
	err = json.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return nil, fmt.Errorf("[Confluent Cloud] Error while UnMarshaling destination Config %+v, with Error : %w", destinationConfig, err)
	}

	hostName := destConfig.BootstrapServer
	hosts := []string{hostName}

	config := getDefaultConfiguration()

	config.Net.SASL.Enable = true
	config.Net.SASL.User = azureEventHubUser
	config.Net.SASL.Password = destConfig.EventHubsConnectionString
	config.Net.SASL.Mechanism = sarama.SASLTypePlaintext

	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tls.Config{
		InsecureSkipVerify: true,
		ClientAuth:         0,
	}

	producer, err := sarama.NewSyncProducer(hosts, config)

	return producer, err
}

// NewProducerForConfluentCloud creates a producer for Confluent cloud based on destination config
func NewProducerForConfluentCloud(destinationConfig interface{}) (sarama.SyncProducer, error) {

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

	config := getDefaultConfiguration()

	config.Net.SASL.Enable = true
	config.Net.SASL.User = destConfig.APIKey
	config.Net.SASL.Password = destConfig.APISecret
	config.Net.SASL.Mechanism = sarama.SASLTypePlaintext

	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tls.Config{
		InsecureSkipVerify: true,
		ClientAuth:         0,
	}

	producer, err := sarama.NewSyncProducer(hosts, config)

	return producer, err
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

// NewTLSConfig generates a TLS configuration used to authenticate on server with certificates.
func NewTLSConfig(caCertFile string) *tls.Config {

	tlsConfig := tls.Config{}
	if len(certificate.Certificate) > 0 {
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	caCert := []byte(caCertFile)
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig.RootCAs = caCertPool

	//tlsConfig.BuildNameToCertificate()
	tlsConfig.InsecureSkipVerify = true
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

	var config = Config{}
	jsonConfig, err := json.Marshal(destConfig)
	if err != nil {
		return makeErrorResponse(err) //returning 500 for retrying, in case of bad config
	}
	err = json.Unmarshal(jsonConfig, &config)
	if err != nil {
		return makeErrorResponse(err) //returning 500 for retrying, in case of bad config
	}
	//pkgLogger.Infof("Created Producer %v\n", producer)

	topic := config.Topic

	parsedJSON := gjson.ParseBytes(jsonData)
	data := parsedJSON.Get("message").Value().(interface{})
	timestamp := time.Now()
	value, err := json.Marshal(data)
	if err != nil {
		return makeErrorResponse(err)
	}
	userID := parsedJSON.Get("userId").Value().(string)
	message := prepareMessage(topic, userID, value, timestamp)

	partition, offset, err := kafkaProducer.SendMessage(message)
	if err != nil {
		return makeErrorResponse(err)
	}

	returnMessage := fmt.Sprintf("Message delivered at Offset: %v , Partition: %v for topic: %s", offset, partition, topic)
	//pkgLogger.Info(returnMessage)
	statusCode := 200
	errorMessage := returnMessage
	//producer.Close()

	return statusCode, returnMessage, errorMessage
}

func makeErrorResponse(err error) (int, string, string) {
	returnMessage := fmt.Sprintf("%s error occured.", err.Error())
	statusCode := GetStatusCodeFromError(err) //400
	errorMessage := err.Error()
	pkgLogger.Error(returnMessage)
	return statusCode, returnMessage, errorMessage
}

// GetStatusCodeFromError parses the error and returns the status so that event gets retried or failed.
func GetStatusCodeFromError(err error) int {
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
