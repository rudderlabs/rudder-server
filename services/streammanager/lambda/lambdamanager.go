//go:generate mockgen --build_flags=--mod=mod -destination=../../../mocks/services/streammanager/lambda/mock_lambda.go -package mock_lambda github.com/rudderlabs/rudder-server/services/streammanager/lambda LambdaClient

package lambda

import (
	"encoding/json"
	"errors"

	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/utils/awsutils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/tidwall/gjson"
)

// Config is the config that is required to send data to Lambda
type destinationConfig struct {
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
func NewProducer(destinationConfig map[string]interface{}, o common.Opts) (*LambdaProducer, error) {
	sessionConfig, err := awsutils.NewSessionConfig(destinationConfig, o.Timeout, lambda.ServiceName)
	if err != nil {
		return nil, err
	}
	if sessionConfig.Region == "" {
		return nil, errors.New("could not find region configuration")
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
	data := gjson.GetBytes(jsonData, "payload").String()
	if data == "" {
		return 400, "Failure", "[Lambda] error :: Invalid payload"
	}
	destConfig := gjson.GetBytes(jsonData, "destConfig").String()
	if destConfig == "" {
		return 400, "Failure", "[Lambda] error :: Invalid Destination Config"
	}

	var config destinationConfig
	err := json.Unmarshal([]byte(destConfig), &config)
	if err != nil {
		returnMessage := "[Lambda] error while unmarshalling destination config :: " + err.Error()
		return 400, "Failure", returnMessage
	}

	var invokeInput lambda.InvokeInput
	invokeInput.SetFunctionName(config.Lambda)
	invokeInput.SetPayload([]byte(data))
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
