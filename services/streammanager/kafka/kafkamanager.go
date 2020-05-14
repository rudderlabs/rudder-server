package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"strings"

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

var (
	clientCertFile, clientKeyFile string
	certificate                   tls.Certificate
)

func init() {
	loadConfig()
	loadCertificate()
}

func loadConfig() {
	clientCertFile = config.GetEnv("SSL_CERTIFICATE_FILE_PATH", "")
	clientKeyFile = config.GetEnv("SSL_KEY_FILE_PATH", "")
}

func loadCertificate() {
	if clientCertFile != "" && clientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			logger.Errorf("error loading certificate: ", err)
			return
		}
		certificate = cert
	}
}

// NewProducer creates a producer based on destination config
func NewProducer(destinationConfig interface{}) (sarama.SyncProducer, error) {

	var destConfig = Config{}
	jsonConfig, err := json.Marshal(destinationConfig)
	err = json.Unmarshal(jsonConfig, &destConfig)

	hostName := destConfig.HostName + ":" + destConfig.Port
	isSslEnabled := destConfig.SslEnabled

	hosts := []string{hostName}

	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewReferenceHashPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

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

func prepareMessage(topic string, key string, message []byte) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(message),
	}

	return msg
}

// NewTLSConfig generates a TLS configuration used to authenticate on server withccertificates.
func NewTLSConfig(caCertFile string) *tls.Config {

	tlsConfig := tls.Config{}
	if len(certificate.Certificate) > 0 {
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	caCert := []byte(caCertFile)
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig.RootCAs = caCertPool

	tlsConfig.BuildNameToCertificate()
	return &tlsConfig
}

// CloseProducer closes a given producer
func CloseProducer(producer interface{}) error {
	kafkaProducer, ok := producer.(sarama.SyncProducer)
	if ok {
		kafkaProducer.Close()
		return nil
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
	err = json.Unmarshal(jsonConfig, &config)

	//logger.Infof("Created Producer %v\n", producer)

	topic := config.Topic

	parsedJSON := gjson.ParseBytes(jsonData)
	if parsedJSON.Get("output").Exists() {
		parsedJSON = parsedJSON.Get("output")
	}
	data := parsedJSON.Get("message").Value().(interface{})
	value, err := json.Marshal(data)
	userID := parsedJSON.Get("userId").Value().(string)
	message := prepareMessage(topic, userID, value)

	var returnMessage string
	var statusCode int
	var errorMessage string
	partition, offset, err := kafkaProducer.SendMessage(message)
	if err != nil {
		returnMessage = fmt.Sprintf("%s error occured.", err.Error())
		statusCode = GetStatusCodeFromError(err) //400
		errorMessage = err.Error()
		logger.Error(returnMessage)
	} else {
		returnMessage = fmt.Sprintf("Message delivered at Offset: %v , Partition: %v for topic: %v", offset, partition, topic)
		//logger.Info(returnMessage)
		statusCode = 200
		errorMessage = returnMessage
	}
	//producer.Close()

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
