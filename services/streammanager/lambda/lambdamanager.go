//go:generate mockgen --build_flags=--mod=mod -destination=../../../mocks/services/streammanager/lambda/mock_lambda.go -package mock_lambda github.com/rudderlabs/rudder-server/services/streammanager/lambda LambdaClient

package lambda

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go/service/lambda"
	jsoniter "github.com/json-iterator/go"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/utils/awsutils"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

// Config is the config that is required to send data to Lambda
type destinationConfig struct {
	InvocationType string
	ClientContext  string
	Lambda         string
}

type inputConfig struct {
	Payload           string             `json:"payload"`
	DestinationConfig *destinationConfig `json:"destConfig"`
}

type LambdaProducer struct {
	client LambdaClient
}

type LambdaClient interface {
	Invoke(input *lambda.InvokeInput) (*lambda.InvokeOutput, error)
}

var (
	pkgLogger logger.Logger
	jsonfast  = jsoniter.Config{
		EscapeHTML:             true,
		SortMapKeys:            true,
		ValidateJsonRawMessage: true,
		UseNumber:              true,
	}.Froze()
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
	awsSession, err := awsutils.CreateSession(sessionConfig)
	if err != nil {
		return nil, err
	}
	return &LambdaProducer{client: lambda.New(awsSession)}, nil
}

// Produce creates a producer and send data to Lambda.
func (producer *LambdaProducer) Produce(jsonData json.RawMessage, _ interface{}) (int, string, string) {
	client := producer.client
	if client == nil {
		return 400, "Failure", "[Lambda] error :: Could not create client"
	}

	var input inputConfig
	err := jsonfast.Unmarshal(jsonData, &input)
	if err != nil {
		returnMessage := "[Lambda] error while unmarshalling jsonData :: " + err.Error()
		return 400, "Failure", returnMessage
	}
	if input.Payload == "" {
		return 400, "Failure", "[Lambda] error :: Invalid payload"
	}
	config := input.DestinationConfig
	if config == nil {
		return 400, "Failure", "[Lambda] error :: Invalid destination config"
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
