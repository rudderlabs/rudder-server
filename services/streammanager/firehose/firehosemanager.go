package firehose

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/tidwall/gjson"
)

var abortableErrors = []string{}

// Config is the config that is required to send data to Firehose
type Config struct {
	Region      string
	AccessKeyID string
	AccessKey   string
	MapEvents   []mapEvents
}

type mapEvents struct {
	from string
	to   string
}

func init() {
	abortableErrors = []string{"AccessDeniedException", "IncompleteSignature", "InvalidAction", "InvalidClientTokenId", "InvalidParameterCombination",
		"InvalidParameterValue", "InvalidQueryParameter", "MissingAuthenticationToken", "MissingParameter", "InvalidArgumentException",
		"KMSAccessDeniedException", "KMSDisabledException", "KMSInvalidStateException", "KMSNotFoundException", "KMSOptInRequired",
		"ResourceNotFoundException", "UnrecognizedClientException", "ValidationError"}
}

// NewProducer creates a producer based on destination config
func NewProducer(destinationConfig interface{}) (firehose.Firehose, error) {
	var config Config
	jsonConfig, err := json.Marshal(destinationConfig)
	err = json.Unmarshal(jsonConfig, &config)
	config.AccessKeyID = "AKIAWTVBJHCTCA36AU3S"
	config.AccessKey = "odp1IJeuoztJf65EFprJnK4H+9n+X34YEKb+WtJa"
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
	var kc *firehose.Firehose = firehose.New(s)
	return *kc, err
}

// Produce creates a producer and send data to Firehose.
func Produce(jsonData json.RawMessage, producer interface{}, destConfig interface{}) (int, string, string) {

	parsedJSON := gjson.ParseBytes(jsonData)

	kc, ok := producer.(firehose.Firehose)
	if !ok {
		return 400, "Could not create producer", "Could not create producer"
	}

	var config Config

	jsonConfig, err := json.Marshal(destConfig)
	err = json.Unmarshal(jsonConfig, &config)

	deliveryStreamMap := config.MapEvents
	data := parsedJSON.Get("message").Value().(interface{})
	value, err := json.Marshal(data)

	putOutput, err := kc.PutRecord(&firehose.PutRecordInput{
		DeliveryStreamName: aws.String(deliveryStreamMap[0].to),
		Record:             &firehose.Record{Data: value},
	})

	if err != nil {
		logger.Errorf("error in firehose :: %v", err.Error())
		statusCode := GetStatusCodeFromError(err)

		return statusCode, err.Error(), err.Error()
	}

	message := fmt.Sprintf("Message delivered with RecordId: %v and encrypted: %v", putOutput.RecordId, putOutput.Encrypted)
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
