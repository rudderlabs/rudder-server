<<<<<<< HEAD
<<<<<<< HEAD
package firehose
=======
package kinesis
>>>>>>> 021c289d... changing for firehose
=======
package firehose
>>>>>>> 4a2aa8f2... added sending simple events to firehose

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
<<<<<<< HEAD
<<<<<<< HEAD
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
=======
=======
	"github.com/aws/aws-sdk-go/aws/awserr"
>>>>>>> 0f2341ca... added error handling
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
<<<<<<< HEAD
	"github.com/aws/aws-sdk-go/service/kinesis"
>>>>>>> 021c289d... changing for firehose
=======
	"github.com/aws/aws-sdk-go/service/firehose"
>>>>>>> 4a2aa8f2... added sending simple events to firehose
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/tidwall/gjson"
)

<<<<<<< HEAD
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

=======
>>>>>>> 0f2341ca... added error handling
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 021c289d... changing for firehose
=======

>>>>>>> 17234333... updated for firehose
=======
>>>>>>> 98a2ae58... addressed review comments
=======

>>>>>>> 66c2d94a... changed logic
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
<<<<<<< HEAD
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
=======
	var kc *firehose.Firehose = firehose.New(s)
>>>>>>> 4a2aa8f2... added sending simple events to firehose
	return *kc, err
=======
	var fh *firehose.Firehose = firehose.New(s)
	return *fh, err
>>>>>>> dbb561bc... showing information to user for firehose
}

// Produce creates a producer and send data to Firehose.
func Produce(jsonData json.RawMessage, producer interface{}, destConfig interface{}) (int, string, string) {

	parsedJSON := gjson.ParseBytes(jsonData)

<<<<<<< HEAD
<<<<<<< HEAD
	kc, ok := producer.(kinesis.Kinesis)
>>>>>>> 021c289d... changing for firehose
=======
	kc, ok := producer.(firehose.Firehose)
>>>>>>> 4a2aa8f2... added sending simple events to firehose
=======
	fh, ok := producer.(firehose.Firehose)
>>>>>>> dbb561bc... showing information to user for firehose
	if !ok {
		return 400, "Could not create producer", "Could not create producer"
	}

<<<<<<< HEAD
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
=======
	var config Config
<<<<<<< HEAD
>>>>>>> 4a2aa8f2... added sending simple events to firehose

=======
>>>>>>> 66c2d94a... changed logic
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
	}
	return 200, "Success", message
}
<<<<<<< HEAD

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
=======
>>>>>>> 0f2341ca... added error handling
