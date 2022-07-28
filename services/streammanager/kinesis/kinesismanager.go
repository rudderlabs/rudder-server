package kinesis

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	abortableErrors = []string{}
	pkgLogger       logger.LoggerI
)

// Config is the config that is required to send data to Kinesis
type Config struct {
	Region       string
	Stream       string
	AccessKeyID  string
	AccessKey    string
	UseMessageID bool
}

type Opts struct {
	Timeout time.Duration
}

func init() {
	abortableErrors = []string{
		"AccessDeniedException", "IncompleteSignature", "InvalidAction", "InvalidClientTokenId", "InvalidParameterCombination",
		"InvalidParameterValue", "InvalidQueryParameter", "MissingAuthenticationToken", "MissingParameter", "InvalidArgumentException",
		"KMSAccessDeniedException", "KMSDisabledException", "KMSInvalidStateException", "KMSNotFoundException", "KMSOptInRequired",
		"ResourceNotFoundException", "UnrecognizedClientException", "ValidationError",
	}

	pkgLogger = logger.NewLogger().Child("streammanager").Child("kinesis")
}

type KinesisProducer struct {
	client *kinesis.Kinesis
}

// NewProducer creates a producer based on destination config
func NewProducer(destinationConfig interface{}, o Opts) (common.StreamProducer, error) {
	config := Config{}

	jsonConfig, err := json.Marshal(destinationConfig)
	if err != nil {
		return nil, fmt.Errorf("[KinesisManager] Error while marshalling destination config %+v. Error: %w", destinationConfig, err)
	}
	err = json.Unmarshal(jsonConfig, &config)
	if err != nil {
		return nil, fmt.Errorf("[KinesisManager] Error while unmarshalling destination config. Error: %w", err)
	}
	httpClient := &http.Client{
		Timeout: o.Timeout,
	}

	var s *session.Session
	if config.AccessKeyID == "" || config.AccessKey == "" {
		s = session.Must(session.NewSession(&aws.Config{
			HTTPClient: httpClient,
			Region:     aws.String(config.Region),
		}))
	} else {
		s = session.Must(session.NewSession(&aws.Config{
			HTTPClient:  httpClient,
			Region:      aws.String(config.Region),
			Credentials: credentials.NewStaticCredentials(config.AccessKeyID, config.AccessKey, ""),
		}))
	}
	return &KinesisProducer{client: kinesis.New(s)}, err
}

// Produce creates a producer and send data to Kinesis.
func (producer *KinesisProducer) Produce(jsonData json.RawMessage, destConfig interface{}) (int, string, string) {
	parsedJSON := gjson.ParseBytes(jsonData)
	client := producer.client 
	if client == nil {
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

	data := parsedJSON.Get("message").Value()
	value, err := json.Marshal(data)
	if err != nil {
		return GetStatusCodeFromError(err), err.Error(), err.Error()
	}
	var (
		userID string
		ok     bool
	)
	if userID, ok = parsedJSON.Get("userId").Value().(string); !ok {
		userID = fmt.Sprintf("%v", parsedJSON.Get("userId").Value())
	}

	partitionKey := aws.String(userID)

	if config.UseMessageID {
		messageID := parsedJSON.Get("message").Get("messageId").Value().(string)
		partitionKey = aws.String(messageID)
	}

	putOutput, err := client.PutRecord(&kinesis.PutRecordInput{
		Data:         value,
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
