<<<<<<< HEAD
package firehose
=======
package kinesis
>>>>>>> 021c289d... changing for firehose

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
<<<<<<< HEAD
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
=======
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
>>>>>>> 021c289d... changing for firehose
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/tidwall/gjson"
)

<<<<<<< HEAD
// Config is the config that is required to send data to Firehose
type Config struct {
	Region      string
	AccessKeyID string
	AccessKey   string
	MapEvents   []map[string]string
}

// NewProducer creates a producer based on destination config
func NewProducer(destinationConfig interface{}) (firehose.Firehose, error) {
	var config Config
	jsonConfig, err := json.Marshal(destinationConfig)
	err = json.Unmarshal(jsonConfig, &config)
	var s *session.Session

=======
var abortableErrors = []string{}

// Config is the config that is required to send data to Kinesis
type Config struct {
	Region       string
	Stream       string
	AccessKeyID  string
	AccessKey    string
	UseMessageID bool
}

func init() {
	abortableErrors = []string{"AccessDeniedException", "IncompleteSignature", "InvalidAction", "InvalidClientTokenId", "InvalidParameterCombination",
		"InvalidParameterValue", "InvalidQueryParameter", "MissingAuthenticationToken", "MissingParameter", "InvalidArgumentException",
		"KMSAccessDeniedException", "KMSDisabledException", "KMSInvalidStateException", "KMSNotFoundException", "KMSOptInRequired",
		"ResourceNotFoundException", "UnrecognizedClientException", "ValidationError"}
}

// NewProducer creates a producer based on destination config
func NewProducer(destinationConfig interface{}) (kinesis.Kinesis, error) {
	config := Config{}

	jsonConfig, err := json.Marshal(destinationConfig)
	err = json.Unmarshal(jsonConfig, &config)

	var s *session.Session
>>>>>>> 021c289d... changing for firehose
	if config.AccessKeyID == "" || config.AccessKey == "" {
		s = session.Must(session.NewSession(&aws.Config{
			Region: aws.String(config.Region),
		}))
	} else {
		s = session.Must(session.NewSession(&aws.Config{
			Region:      aws.String(config.Region),
			Credentials: credentials.NewStaticCredentials(config.AccessKeyID, config.AccessKey, "")}))
	}
<<<<<<< HEAD
	var fh *firehose.Firehose = firehose.New(s)
	return *fh, err
}

// Produce creates a producer and send data to Firehose.
func Produce(jsonData json.RawMessage, producer interface{}, destConfig interface{}) (int, string, string) {

	parsedJSON := gjson.ParseBytes(jsonData)
	var putOutput *firehose.PutRecordOutput = nil
	var errorRec error
	var message string

	fh, ok := producer.(firehose.Firehose)
=======
	var kc *kinesis.Kinesis = kinesis.New(s)
	return *kc, err
}

// Produce creates a producer and send data to Kinesis.
func Produce(jsonData json.RawMessage, producer interface{}, destConfig interface{}) (int, string, string) {

	parsedJSON := gjson.ParseBytes(jsonData)

	kc, ok := producer.(kinesis.Kinesis)
>>>>>>> 021c289d... changing for firehose
	if !ok {
		return 400, "Could not create producer", "Could not create producer"
	}

<<<<<<< HEAD
	var config Config
	jsonConfig, err := json.Marshal(destConfig)
	err = json.Unmarshal(jsonConfig, &config)
	data := parsedJSON.Get("message").Value().(interface{})
	value, err := json.Marshal(data)

	if err != nil {
		logger.Errorf("error in firehose :: %v", err.Error())
		statusCode := 500
		return statusCode, err.Error(), err.Error()
	}

	if parsedJSON.Get("deliveryStreamMapTo").Value() != nil {
		deliveryStreamMapTo := parsedJSON.Get("deliveryStreamMapTo").Value().(interface{})
		deliveryStreamMapToInput, err := json.Marshal(deliveryStreamMapTo)
		if err != nil {
			logger.Errorf("error in firehose :: %v", err.Error())
			statusCode := 500
			return statusCode, err.Error(), err.Error()
		}

		deliveryStreamMapToInputString := strings.Trim(string(deliveryStreamMapToInput), "\"")

		putOutput, errorRec = fh.PutRecord(&firehose.PutRecordInput{
			DeliveryStreamName: aws.String(string(deliveryStreamMapToInputString)),
			Record:             &firehose.Record{Data: value},
		})

		if errorRec != nil {
			statusCode := 500
			if awsErr, ok := errorRec.(awserr.Error); ok {
				if reqErr, ok := errorRec.(awserr.RequestFailure); ok {
					logger.Errorf("error in firehose :: %v + %v", awsErr.Code(), reqErr.Error())
					statusCode = reqErr.StatusCode()
				}
			}
			return statusCode, errorRec.Error(), errorRec.Error()
		}

		if putOutput != nil {
			message = fmt.Sprintf("Message delivered with Record information %v", putOutput)
		}
	}
	return 200, "Success", message
=======
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
		statusCode := GetStatusCodeFromError(err)

		return statusCode, err.Error(), err.Error()
	}
	message := fmt.Sprintf("Message delivered at SequenceNumber: %v , shard Id: %v", putOutput.SequenceNumber, putOutput.ShardId)
	return 200, "Success", message
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
>>>>>>> 021c289d... changing for firehose
}
