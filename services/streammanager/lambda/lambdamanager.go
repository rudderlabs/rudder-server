//go:generate mockgen --build_flags=--mod=mod -destination=../../../mocks/services/streammanager/lambda/mock_lambda.go -package mock_lambda github.com/rudderlabs/rudder-server/services/streammanager/lambda LambdaClient

package lambda

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go/service/lambda"
	jsoniter "github.com/json-iterator/go"
	"github.com/mitchellh/mapstructure"

	"github.com/rudderlabs/rudder-go-kit/awsutil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/utils/awsutils"
)

// Config is the config that is required to send data to Lambda
type destinationConfig struct {
	InvocationType string `json:"invocationType"`
	ClientContext  string `json:"clientContext"`
	Lambda         string `json:"lambda"`
}

type inputData struct {
	Payload string `json:"payload"`
}

type LambdaProducer struct {
	client LambdaClient
}

type LambdaClient interface {
	Invoke(input *lambda.InvokeInput) (*lambda.InvokeOutput, error)
}

var (
	pkgLogger logger.Logger
	jsonfast  = jsoniter.ConfigCompatibleWithStandardLibrary
)

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child(lambda.ServiceName)
}

// NewProducer creates a producer based on destination config
func NewProducer(destination *backendconfig.DestinationT, o common.Opts) (*LambdaProducer, error) {
	sessionConfig, err := awsutils.NewSessionConfigForDestination(destination, o.Timeout, lambda.ServiceName)
	if err != nil {
		return nil, err
	}
	awsSession, err := awsutil.CreateSession(sessionConfig)
	if err != nil {
		return nil, err
	}
	return &LambdaProducer{client: lambda.New(awsSession)}, nil
}

// Produce creates a producer and send data to Lambda.
func (producer *LambdaProducer) Produce(jsonData json.RawMessage, destConfig interface{}) (int, string, string) {
	client := producer.client
	if client == nil {
		return 400, "Failure", "[Lambda] error :: Could not create client"
	}

	var input inputData
	err := jsonfast.Unmarshal(jsonData, &input)
	if err != nil {
		returnMessage := "[Lambda] error while unmarshalling jsonData :: " + err.Error()
		return 400, "Failure", returnMessage
	}
	if input.Payload == "" {
		return 400, "Failure", "[Lambda] error :: Invalid payload"
	}
	var config destinationConfig
	err = mapstructure.Decode(destConfig, &config)
	if err != nil {
		returnMessage := "[Lambda] error while unmarshalling destConfig :: " + err.Error()
		return 400, "Failure", returnMessage
	}
	if config.InvocationType == "" {
		config.InvocationType = "Event"
	}

	var invokeInput lambda.InvokeInput
	invokeInput.SetFunctionName(config.Lambda)
	invokeInput.SetPayload([]byte(input.Payload))
	invokeInput.SetInvocationType(config.InvocationType)
	if config.ClientContext != "" {
		invokeInput.SetClientContext(config.ClientContext)
	}

	if err = invokeInput.Validate(); err != nil {
		return 400, "Failure", "[Lambda] error :: Invalid invokeInput :: " + err.Error()
	}

	_, err = client.Invoke(&invokeInput)
	if err != nil {
		statusCode, respStatus, responseMessage := common.ParseAWSError(err)
		pkgLogger.Errorf("[Lambda] Invocation error :: %d : %s : %s", statusCode, respStatus, responseMessage)
		return statusCode, respStatus, responseMessage
	}

	return 200, "Success", "Event delivered to Lambda :: " + config.Lambda
}

func (*LambdaProducer) Close() error {
	// no-op
	return nil
}
