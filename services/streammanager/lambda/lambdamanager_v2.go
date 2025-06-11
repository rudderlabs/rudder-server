//go:generate mockgen --build_flags=--mod=mod -destination=../../../mocks/services/streammanager/lambda_v2/mock_lambda_v2.go -package mock_lambda_v2 github.com/rudderlabs/rudder-server/services/streammanager/lambda LambdaClientV2

package lambda

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/mitchellh/mapstructure"

	awsutil "github.com/rudderlabs/rudder-go-kit/awsutil_v2"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/utils/awsutils"
)

type LambdaProducerV2 struct {
	client LambdaClientV2
}

type LambdaClientV2 interface {
	Invoke(ctx context.Context, input *lambda.InvokeInput, opts ...func(*lambda.Options)) (*lambda.InvokeOutput, error)
}

// NewProducer creates a producer based on destination config
func NewProducerV2(destination *backendconfig.DestinationT, o common.Opts) (common.Producer, error) {
	sessionConfig, err := awsutils.NewSessionConfigForDestinationV2(destination, o.Timeout, "lambda")
	if err != nil {
		return nil, err
	}
	sessionConfig.MaxIdleConnsPerHost = config.GetIntVar(64, 1, "Router.LAMBDA.httpMaxIdleConnsPerHost", "Router.LAMBDA.noOfWorkers", "Router.noOfWorkers")
	awsConfig, err := awsutil.CreateAWSConfig(context.Background(), sessionConfig)
	if err != nil {
		return nil, err
	}
	return &LambdaProducerV2{client: lambda.NewFromConfig(awsConfig)}, nil
}

// Produce creates a producer and send data to Lambda.
func (producer *LambdaProducerV2) Produce(jsonData json.RawMessage, destConfig interface{}) (int, string, string) {
	client := producer.client
	if client == nil {
		return 400, "Failure", "[Lambda] error :: Could not create client"
	}

	var input inputData
	err := jsonrs.Unmarshal(jsonData, &input)
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
	invokeInput.FunctionName = &config.Lambda
	invokeInput.Payload = []byte(input.Payload)
	invokeInput.InvocationType = types.InvocationType(config.InvocationType)
	if config.ClientContext != "" {
		invokeInput.ClientContext = &config.ClientContext
	}

	_, err = client.Invoke(context.Background(), &invokeInput)
	if err != nil {
		statusCode, respStatus, responseMessage := common.ParseAWSErrorV2(err)
		pkgLogger.Errorf("[Lambda] Invocation error :: %d : %s : %s", statusCode, respStatus, responseMessage)
		return statusCode, respStatus, responseMessage
	}

	return 200, "Success", "Event delivered to Lambda :: " + config.Lambda
}

func (*LambdaProducerV2) Close() error {
	// no-op
	return nil
}
