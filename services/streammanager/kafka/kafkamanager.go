package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"reflect"

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

type ProducerConfig struct {
	Config   Config
	Producer sarama.SyncProducer
}

var (
	clientCertFile, clientKeyFile string
	certificate                   tls.Certificate
	destinationConfigProducerMap  map[string]ProducerConfig
)

func init() {
	loadConfig()
	loadCertificate()
	destinationConfigProducerMap = make(map[string]ProducerConfig)
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

func NewProducer(jsonData json.RawMessage) (sarama.SyncProducer, error) {

	parsedJSON := gjson.ParseBytes(jsonData)
	if parsedJSON.Get("output").Exists() {
		parsedJSON = parsedJSON.Get("output")
	}
	configFromJSON, err := json.Marshal(parsedJSON.Get("config").Value())
	if err != nil {
		panic(fmt.Errorf("error getting config from payload"))
	}

	var destConfig Config
	json.Unmarshal(configFromJSON, &destConfig)

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

func newProducer(destConfig Config) (sarama.SyncProducer, error) {
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

func GetProducer(destID string, destConfig Config) (sarama.SyncProducer, error) {
	//producer := nil
	if destinationConfigProducerMap[destID] != (ProducerConfig{}) {
		producer := destinationConfigProducerMap[destID].Producer
		config := destinationConfigProducerMap[destID].Config
		if reflect.DeepEqual(config, destConfig) {
			logger.Infof("returning existing producer: %v for destination: %v", producer, destID)
			return producer, nil
		}
		logger.Infof("=========config changed ======== closing existing producer ======= for destination: %v", destID)
		producer.Close()
	}
	producer, err := newProducer(destConfig)
	if err == nil {
		producerConfig := ProducerConfig{Config: destConfig, Producer: producer}
		destinationConfigProducerMap[destID] = producerConfig
		logger.Infof("created new producer: %v for destination: ", producer, destID)
	}
	return producer, err
}

// Produce creates a producer and send data to Kafka.
func Produce(jsonData json.RawMessage, sourceID string, destinationID string) (int, string, string) {

	parsedJSON := gjson.ParseBytes(jsonData)
	if parsedJSON.Get("output").Exists() {
		parsedJSON = parsedJSON.Get("output")
	}
	configFromJSON, err := json.Marshal(parsedJSON.Get("config").Value())
	if err != nil {
		panic(fmt.Errorf("error getting config from payload"))
	}
	// messageID := parsedJSON.Get("message").Get("messageId").Value().(string)
	userID := parsedJSON.Get("userId").Value().(string)

	var config Config
	json.Unmarshal(configFromJSON, &config)

	logger.Info("in Kafka produce")

	producer, err := GetProducer(destinationID, config) //newProducer(config)
	if err != nil {
		logger.Errorf("Could not create producer: ", err)
		return 400, "Could not create producer", err.Error()
	}

	//logger.Infof("Created Producer %v\n", producer)

	topic := config.Topic

	data := parsedJSON.Get("message").Value().(interface{})
	value, err := json.Marshal(data)
	message := prepareMessage(topic, userID, value)

	var returnMessage string
	var statusCode int
	var errorMessage string
	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		returnMessage = fmt.Sprintf("%s error occured.", err)
		statusCode = 400
		errorMessage = err.Error()
		logger.Error(returnMessage)
	} else {
		returnMessage = fmt.Sprintf("Message delivered at Offset: %v , Partition: %v for topic: %v", offset, partition, topic)
		logger.Info(returnMessage)
		statusCode = 200
		errorMessage = returnMessage
	}
	//producer.Close()

	return statusCode, returnMessage, errorMessage
}
