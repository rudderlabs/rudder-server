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
}

// NewProducer creates a producer based on destination config
func NewProducer(destinationConfig interface{}) (firehose.Firehose, error) {
	var config Config
	jsonConfig, err := json.Marshal(destinationConfig)
	err = json.Unmarshal(jsonConfig, &config)
	if err != nil {
		return firehose.Firehose{}, fmt.Errorf("[FireHose] error  :: error in firehose while unmarshelling destination config:: %w", err)
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
	var fh *firehose.Firehose = firehose.New(s)
	return *fh, nil
}

// Produce creates a producer and send data to Firehose.
func Produce(jsonData json.RawMessage, producer interface{}, destConfig interface{}) (statusCode int, errorCode string, responseMessage string) {

	parsedJSON := gjson.ParseBytes(jsonData)
	var putOutput *firehose.PutRecordOutput = nil
	var errorRec error

	fh, ok := producer.(firehose.Firehose)

	if !ok {
		errorCode = "Failure"
		responseMessage = "[FireHose] error :: Could not create producer"
		return 400, errorCode, responseMessage
	}
	var config Config
	jsonConfig, err := json.Marshal(destConfig)
	err = json.Unmarshal(jsonConfig, &config)
	if err != nil {
		errorCode = "Failure"
		responseMessage = "[FireHose] error  :: " + err.Error()
		logger.Errorf("[FireHose] error  :: %w", err)
		statusCode := 400
		return statusCode, errorCode, responseMessage
	}
	var data interface{}
	if parsedJSON.Get("message").Value() != nil {
		data = parsedJSON.Get("message").Value()
	} else {
		errorCode = "Failure"
		responseMessage = "[FireHose] error :: message from payload not found"
		return 400, errorCode, responseMessage
	}
	value, err := json.Marshal(data)

	if err != nil {
		errorCode = "Failure"
		responseMessage = "[FireHose] error  :: " + err.Error()
		logger.Errorf("[FireHose] error  :: %w", err)
		statusCode := 400
		return statusCode, errorCode, responseMessage
	}

	if parsedJSON.Get("deliveryStreamMapTo").Value() != nil {
		deliveryStreamMapToInputString, ok := parsedJSON.Get("deliveryStreamMapTo").Value().(string)
		if !ok {
			errorCode = "Failure"
			responseMessage = "[FireHose] error :: Could not parse delivery stream to string"
			logger.Error(responseMessage)
			statusCode := 400
			return statusCode, errorCode, responseMessage
		}
		if deliveryStreamMapToInputString == "" {
			errorCode = "Failure"
			responseMessage = "[FireHose] error :: empty delivery stream"
			return 400, errorCode, responseMessage
		}

		putOutput, errorRec = fh.PutRecord(&firehose.PutRecordInput{
			DeliveryStreamName: aws.String(deliveryStreamMapToInputString),
			Record:             &firehose.Record{Data: value},
		})

		if errorRec != nil {
			statusCode := 500
			errorCode = "Failure"
			responseMessage = "[FireHose] error  :: " + errorRec.Error()
			if awsErr, ok := errorRec.(awserr.Error); ok {
				if reqErr, ok := errorRec.(awserr.RequestFailure); ok {
					responseMessage = "[FireHose] error  :: " + reqErr.Error()
					statusCode = reqErr.StatusCode()
					logger.Errorf("[FireHose] error  :: %v + %v", awsErr.Code(), reqErr.Error())
				}
			}
			return statusCode, errorCode, responseMessage
		}

		if putOutput != nil {
			responseMessage = fmt.Sprintf("Message delivered with Record information %v", putOutput)
		}
		errorCode = "Success"
		logger.Info(responseMessage)
		return 200, errorCode, responseMessage
	} else {
		errorCode = "Failure"
		responseMessage = "[FireHose] error  :: Delivery Stream not found"
		return 400, errorCode, responseMessage
	}

}
