package firehose

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/tidwall/gjson"
)

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

	if config.AccessKeyID == "" || config.AccessKey == "" {
		s = session.Must(session.NewSession(&aws.Config{
			Region: aws.String(config.Region),
		}))
	} else {
		s = session.Must(session.NewSession(&aws.Config{
			Region:      aws.String(config.Region),
			Credentials: credentials.NewStaticCredentials(config.AccessKeyID, config.AccessKey, "")}))
	}
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
	if !ok {
		return 400, "Could not create producer", "Could not create producer"
	}

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
		deliveryStreamMapToInputString, ok := parsedJSON.Get("deliveryStreamMapTo").Value().(string)
		if !ok {
			var sendMessage string = "error in firehose :: Could not parse delivery stream to string"
			logger.Error(sendMessage)
			statusCode := 500
			return statusCode, sendMessage, sendMessage
		}

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
		return 200, "Success", message
	} else {
		return 400, "Delivery Stream not found", "Delivery Stream not found"
	}

}
