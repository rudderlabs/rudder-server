package kafka

import (
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/tidwall/gjson"
)

// Config is the config that is required to send data to Kafka
type Config struct {
	Topic    string
	HostName string
}

func newProducer(hostName string) (sarama.SyncProducer, error) {
	hosts := []string{hostName}
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(hosts, config)

	return producer, err
}

func prepareMessage(topic string, message []byte) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	return msg
}

// Produce creates a producer and send data to Kafka.
func Produce(jsonData json.RawMessage) (int, string, string) {

	parsedJSON := gjson.ParseBytes(jsonData)
	if parsedJSON.Get("output").Exists() {
		parsedJSON = parsedJSON.Get("output")
	}
	configFromJSON, err := json.Marshal(parsedJSON.Get("config").Value())
	if err != nil {
		panic(fmt.Errorf("error getting config from payload"))
	}

	var config Config
	json.Unmarshal(configFromJSON, &config)

	fmt.Println("in Kafka produce")

	producer, err := newProducer(config.HostName)
	if err != nil {
		fmt.Println("Could not create producer: ", err)
		return 400, "Could not create producer", err.Error()
	}

	fmt.Printf("Created Producer %v\n", producer)

	topic := config.Topic

	data := parsedJSON.Get("message").Value().(interface{})
	value, err := json.Marshal(data)
	message := prepareMessage(topic, value)

	var returnMessage string
	var statusCode int
	var errorMessage string
	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		returnMessage = fmt.Sprintf("%s error occured.", err)
		statusCode = 400
		errorMessage = err.Error()
	} else {
		returnMessage = fmt.Sprintf("Message delivered at Offset: %v , Partition: %v", offset, partition)
		statusCode = 200
		errorMessage = returnMessage
	}
	producer.Close()

	return statusCode, returnMessage, errorMessage
}
