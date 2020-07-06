package kinesis

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
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

func init() {
}

// NewProducer creates a producer based on destination config
func NewProducer(destinationConfig interface{}) (kinesis.Kinesis, error) {
	config := Config{}

	jsonConfig, err := json.Marshal(destinationConfig)
	err = json.Unmarshal(jsonConfig, &config)

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
	var kc *kinesis.Kinesis = kinesis.New(s)
	return *kc, err
}

// Produce creates a producer and send data to Kinesis.
func Produce(jsonData json.RawMessage, producer interface{}, destConfig interface{}) (int, string, string) {

	parsedJSON := gjson.ParseBytes(jsonData)

	kc, ok := producer.(kinesis.Kinesis)
	if !ok {
		return 400, "Could not create producer", "Could not create producer"
	}

	config := Config{}

	jsonConfig, err := json.Marshal(destConfig)
	err = json.Unmarshal(jsonConfig, &config)

	streamName := aws.String(config.Stream)

	data := parsedJSON.Get("message").Value().(interface{})
	value, err := json.Marshal(data)
	var userID string
	if userID, ok = parsedJSON.Get("userId").Value().(string); !ok {
		userID = fmt.Sprintf("%v", parsedJSON.Get("userId").Value())
	}

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
		logger.Errorf("error in kinesis :: %v", err.Error())

		// set default status code as 500
		statusCode := 500

		// fetching status code from response
		if awsErr, ok := err.(awserr.RequestFailure); ok {
			statusCode = awsErr.StatusCode()
		}

		return statusCode, err.Error(), err.Error()
	}
	message := fmt.Sprintf("Message delivered at SequenceNumber: %v , shard Id: %v", putOutput.SequenceNumber, putOutput.ShardId)
	return 200, "Success", message
}

