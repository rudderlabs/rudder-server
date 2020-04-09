package kinesis

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/tidwall/gjson"
)

// Config is the config that is required to send data to Kinesis
type Config struct {
	Region       string
	Stream       string
	AccessKeyID  string
	AccessKey    string
	UseMessageID bool
}

// Produce creates a producer and send data to Kinesis.
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

	var s *session.Session
	if config.AccessKeyID == "" || config.AccessKey == "" {
		s = session.Must(session.NewSession(&aws.Config{
			Region: aws.String(config.Region),
		}))
	} else {
		s = session.Must(session.NewSession(&aws.Config{
			Region:      aws.String(config.Region),
			Credentials: credentials.NewStaticCredentials(config.AccessKeyID, config.AccessKey, "")}))
	}

	kc := kinesis.New(s)

	streamName := aws.String(config.Stream)

	data := parsedJSON.Get("message").Value().(interface{})
	value, err := json.Marshal(data)
	userID := parsedJSON.Get("userId").Value().(string)

	partitionKey := aws.String(userID)

	if config.UseMessageID {
		messageID := parsedJSON.Get("message").Get("messageId").Value().(string)
		partitionKey = aws.String(messageID)
	}

	putOutput, err := kc.PutRecord(&kinesis.PutRecordInput{
		Data:         []byte(value),
		StreamName:   streamName,
		PartitionKey: partitionKey,
	})
	if err != nil {
		logger.Debugf("==== error in kinesis ==== %v", err.Error())
		return 500, err.Error(), ""
	}
	message := fmt.Sprintf("Message delivered at SequenceNumber: %v , shard Id: %v", putOutput.SequenceNumber, putOutput.ShardId)
	return 200, "Success", message
}
