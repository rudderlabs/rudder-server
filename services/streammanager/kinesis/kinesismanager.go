package kinesis

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/tidwall/gjson"
)

var abortableErrors = []string{}
var pkgLogger logger.LoggerI

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

	pkgLogger = logger.NewLogger().Child("streammanager").Child("kinesis")
}

// NewProducer creates a producer based on destination config
func NewProducer(destinationConfig interface{}) (kinesis.Kinesis, error) {
	config := Config{}

	jsonConfig, err := json.Marshal(destinationConfig)
	if err != nil {
		return kinesis.Kinesis{}, fmt.Errorf("[KinesisManager] Error while marshalling destination config %+v. Error: %w", destinationConfig, err)
	}
	err = json.Unmarshal(jsonConfig, &config)
	if err != nil {
		return kinesis.Kinesis{}, fmt.Errorf("[KinesisManager] Error while unmarshalling destination config. Error: %w", err)
	}

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
	if err != nil {
		outErr := fmt.Errorf("[KinesisManager] Error while Marshalling destination config %+v Error: %w", destConfig, err)
		return GetStatusCodeFromError(err), outErr.Error(), outErr.Error()
	}
	err = json.Unmarshal(jsonConfig, &config)
	if err != nil {
		outErr := fmt.Errorf("[KinesisManager] Error while Unmarshalling destination config: %w", err)
		return GetStatusCodeFromError(err), outErr.Error(), outErr.Error()
	}

	streamName := aws.String(config.Stream)

	data := parsedJSON.Get("message").Value().(interface{})
	value, err := json.Marshal(data)
	if err != nil {
		return GetStatusCodeFromError(err), err.Error(), err.Error()
	}
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
		pkgLogger.Errorf("error in kinesis :: %v", err.Error())
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
}
