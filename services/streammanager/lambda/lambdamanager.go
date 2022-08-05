package lambda

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	jsoniter "github.com/json-iterator/go"
	"github.com/tidwall/gjson"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

// Config is the config that is required to send data to Lambda
type Config struct {
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	InvocationType  string
	ClientContext   string
	Lambda          string
}

type Opts struct {
	Timeout time.Duration
}

var pkgLogger logger.LoggerI

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("lambda")
}

// NewProducer creates a producer based on destination config
func NewProducer(destinationConfig interface{}, o Opts) (*lambda.Lambda, error) {
	var config Config
	jsonConfig, err := jsonfast.Marshal(destinationConfig)
	if err != nil {
		return nil, fmt.Errorf("[Lambda] error :: error while marshalling destination config :: %w", err)
	}
	err = jsonfast.Unmarshal(jsonConfig, &config)
	if err != nil {
		return nil, fmt.Errorf("[Lambda] error :: error while unmarshelling destination config :: %w", err)
	}
	if config.Region == "" {
		return nil, fmt.Errorf("[Lambda] error :: Region is a required property")
	}

	var s *session.Session
	httpClient := &http.Client{
		Timeout: o.Timeout,
	}
	if config.AccessKeyID == "" || config.SecretAccessKey == "" {
		s, err = session.NewSession(&aws.Config{
			Region:     aws.String(config.Region),
			HTTPClient: httpClient,
		})
	} else {
		s, err = session.NewSession(&aws.Config{
			HTTPClient:  httpClient,
			Region:      aws.String(config.Region),
			Credentials: credentials.NewStaticCredentials(config.AccessKeyID, config.SecretAccessKey, ""),
		})
	}
	if err != nil {
		return nil, fmt.Errorf("[Lambda] error :: error while creating session :: %w", err)
	}

	return lambda.New(s), nil
}

// Produce creates a producer and send data to Lambda.
func Produce(jsonData json.RawMessage, producer, destConfig interface{}) (int, string, string) {
	client, ok := producer.(*lambda.Lambda)
	if !ok {
		return simpleErrorResponse("Could not create client")
	}
	data := gjson.GetBytes(jsonData, "payload").String()
	if data == "" {
		return simpleErrorResponse("Invalid payload")
	}

	var config Config
	jsonConfig, err := jsonfast.Marshal(destConfig)
	if err != nil {
		return simpleErrorResponse("error while marshalling destination config :: " + err.Error())
	}
	err = jsonfast.Unmarshal(jsonConfig, &config)
	if err != nil {
		return simpleErrorResponse("error while unmarshalling destination config :: " + err.Error())
	}
	if config.Lambda == "" {
		return simpleErrorResponse("Lambda function name is a required property")
	}
	if config.InvocationType == "" {
		return simpleErrorResponse("InvocationType is a required property")
	}

	var input lambda.InvokeInput
	input.FunctionName = aws.String(config.Lambda)
	input.Payload = []byte(data)
	input.InvocationType = aws.String(config.InvocationType)
	if config.ClientContext != "" {
		input.ClientContext = aws.String(config.ClientContext)
	}

	_, err = client.Invoke(&input)
	if err != nil {
		return invokeErrorResponse(err)
	}

	responseMessage := fmt.Sprintf("Event delivered to Lambda : %s", config.Lambda)
	return 200, "Success", responseMessage
}

func simpleErrorResponse(message string) (int, string, string) {
	respStatus := "Failure"
	returnMessage := "[Lambda] error :: " + message
	pkgLogger.Errorf(returnMessage)
	return 400, respStatus, returnMessage
}

func invokeErrorResponse(err error) (int, string, string) {
	if awsErr, ok := err.(awserr.Error); ok {
		switch awsErr.Code() {
		case lambda.ErrCodeTooManyRequestsException, lambda.ErrCodeEC2ThrottledException:
			return 429, awsErr.Code(), awsErr.Message()
		case lambda.ErrCodeServiceException, lambda.ErrCodeResourceNotReadyException,
			lambda.ErrCodeResourceConflictException, lambda.ErrCodeEFSMountTimeoutException,
			lambda.ErrCodeEFSMountConnectivityException:
			return 500, awsErr.Code(), awsErr.Message()
		default:
			return 400, awsErr.Code(), awsErr.Message()
		}
	}
	responseMessage := "Invocation error" + err.Error()
	return simpleErrorResponse(responseMessage)
}
