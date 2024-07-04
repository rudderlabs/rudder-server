package wunderkind

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go/service/lambda"
	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-go-kit/awsutil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

const (
	INVOCATION_TYPE         = "RequestResponse"
	WUNDERKIND_REGION       = "WUNDERKIND_REGION"
	WUNDERKIND_IAM_ROLE_ARN = "WUNDERKIND_IAM_ROLE_ARN"
	WUNDERKIND_EXTERNAL_ID  = "WUNDERKIND_EXTERNAL_ID"
	WUNDERKIND_LAMBDA       = "WUNDERKIND_LAMBDA"
)

var (
	pkgLogger logger.Logger
	jsonfast  = jsoniter.ConfigCompatibleWithStandardLibrary
)

type inputData struct {
	Payload string `json:"payload"`
}

type WunderkindProducer struct {
	client LambdaClient
}

type LambdaClient interface {
	Invoke(input *lambda.InvokeInput) (*lambda.InvokeOutput, error)
}

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child(lambda.ServiceName)
}

// NewProducer creates a producer based on destination config
func NewProducer(destination *backendconfig.DestinationT, o common.Opts) (*WunderkindProducer, error) {
	sessionConfig := &awsutil.SessionConfig{
		Region:        config.GetEnv(WUNDERKIND_REGION),
		IAMRoleARN:    config.GetEnv(WUNDERKIND_IAM_ROLE_ARN),
		ExternalID:    config.GetEnv(WUNDERKIND_EXTERNAL_ID),
		RoleBasedAuth: true,
	}
	awsSession, err := awsutil.CreateSession(sessionConfig)
	if err != nil {
		return nil, err
	}

	return &WunderkindProducer{client: lambda.New(awsSession)}, nil
}

// Produce creates a producer and send data to Lambda.
func (producer *WunderkindProducer) Produce(jsonData json.RawMessage, destConfig interface{}) (int, string, string) {
	client := producer.client
	if client == nil {
		return 400, "Failure", "[Wunderkind] error :: Could not create client"
	}

	var input inputData
	err := jsonfast.Unmarshal(jsonData, &input)
	if err != nil {
		returnMessage := "[Wunderkind] error while unmarshalling jsonData :: " + err.Error()
		return 400, "Failure", returnMessage
	}
	if input.Payload == "" {
		return 400, "Failure", "[Wunderkind] error :: Invalid payload"
	}

	var invokeInput lambda.InvokeInput
	wunderKindLambda := config.GetEnv(WUNDERKIND_LAMBDA)
	invokeInput.SetFunctionName(wunderKindLambda)
	invokeInput.SetPayload([]byte(input.Payload))
	invokeInput.SetInvocationType(INVOCATION_TYPE)
	invokeInput.SetLogType("Tail")

	if err = invokeInput.Validate(); err != nil {
		return 400, "Failure", "[Wunderkind] error :: Invalid invokeInput :: " + err.Error()
	}

	response, err := client.Invoke(&invokeInput)
	if err != nil {
		statusCode, respStatus, responseMessage := common.ParseAWSError(err)
		pkgLogger.Errorf("[Wunderkind] Invocation error :: %d : %s : %s", statusCode, respStatus, responseMessage)
		return statusCode, respStatus, responseMessage
	}

	// handle a case where lambda invocation is successful, but there is an issue with the payload.
	if response.FunctionError != nil {
		statusCode := 400
		respStatus := "Failure"
		responseMessage := string(response.Payload)
		pkgLogger.Errorf("[Wunderkind] Function execution error :: %d : %s : %s", statusCode, respStatus, responseMessage)
		return statusCode, respStatus, responseMessage
	}

	return 200, "Success", "Event delivered to Wunderkind :: " + wunderKindLambda
}

func (*WunderkindProducer) Close() error {
	// no-op
	return nil
}
