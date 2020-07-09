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

var putOutput *firehose.PutRecordOutput = nil
var errorRec error
var event, typeCall gjson.Result

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

	fh, ok := producer.(firehose.Firehose)
	if !ok {
		return 400, "Could not create producer", "Could not create producer"
	}

	var config Config

	jsonConfig, err := json.Marshal(destConfig)
	err = json.Unmarshal(jsonConfig, &config)
	deliveryStreamMap := config.MapEvents
	data := parsedJSON.Get("message").Value().(interface{})
	value, err := json.Marshal(data)

	event = parsedJSON.Get("message.event")
	typeCall = parsedJSON.Get("message.type")
	if err != nil {
		logger.Errorf("error in firehose :: %v", err.Error())
		statusCode := 500
		return statusCode, err.Error(), err.Error()
	}
	putOutput = nil
	var deliveryStreamMapTo string = ""
	var deliveryStreamMapFrom string = ""

	for i := 0; i < len(deliveryStreamMap); i++ {
		deliveryStreamMapFrom = deliveryStreamMap[i]["from"]
		if event.Value() == deliveryStreamMapFrom {
			deliveryStreamMapTo = deliveryStreamMap[i]["to"]
			break
		}
	}

	if deliveryStreamMapTo != "" {
		putOutput, errorRec = fh.PutRecord(&firehose.PutRecordInput{
			DeliveryStreamName: aws.String(deliveryStreamMapTo),
			Record:             &firehose.Record{Data: value},
		})

		if errorRec != nil {
			statusCode := 500
			if awsErr, ok := errorRec.(awserr.Error); ok {
				if reqErr, ok := errorRec.(awserr.RequestFailure); ok {
					logger.Errorf("error in firehose :: %v for event %v", awsErr.Code(), event.Value())
					statusCode = reqErr.StatusCode()
				}
			}
			return statusCode, errorRec.Error(), errorRec.Error()
		}
	}
	var message string
	if putOutput != nil {
		message = fmt.Sprintf("Message delivered for event %v and Record information %v", event, putOutput)
	} else {
		if event.Value() != nil {
			message = fmt.Sprintf("No delivery stream set for event %v", event)
		} else {
			message = fmt.Sprintf("No delivery stream set for this %v event", typeCall)
		}
	}
	logger.Infof(message)
	return 200, "Success", message
}
