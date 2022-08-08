package lambda

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/lambda"
	jsoniter "github.com/json-iterator/go"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/utils/awsutils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/tidwall/gjson"
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

// Config is the config that is required to send data to Lambda
type Config struct {
	InvocationType string
	ClientContext  string
	Lambda         string
}

type LambdaProducer struct {
	client LambdaClient
}

type LambdaClient interface {
	Invoke(input *lambda.InvokeInput) (*lambda.InvokeOutput, error)
}

var pkgLogger logger.LoggerI

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child(lambda.ServiceName)
}

// NewProducer creates a producer based on destination config
func NewProducer(destinationConfig interface{}, o common.Opts) (*LambdaProducer, error) {
	sessionConfig, err := awsutils.NewSessionConfig(destinationConfig, o.Timeout, lambda.ServiceName)
	if err != nil {
		return nil, err
	}
	return &LambdaProducer{client: lambda.New(awsutils.CreateSession(sessionConfig))}, nil
}

// Produce creates a producer and send data to Lambda.
func (producer *LambdaProducer) Produce(jsonData json.RawMessage, destConfig interface{}) (int, string, string) {
	client := producer.client
	if client == nil {
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

	var invokeInput lambda.InvokeInput
	invokeInput.SetFunctionName(config.Lambda)
	invokeInput.SetPayload([]byte(data))
	invokeInput.SetInvocationType(config.InvocationType)
	if config.ClientContext != "" {
		invokeInput.SetClientContext(config.ClientContext)
	}

	if err = invokeInput.Validate(); err != nil {
		return simpleErrorResponse("Invalid invokeInput :: " + err.Error())
	}

	_, err = client.Invoke(&invokeInput)
	if err != nil {
		return invokeErrorResponse(err)
	}

	return 200, "Success", "Event delivered to Lambda :: " + config.Lambda
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
