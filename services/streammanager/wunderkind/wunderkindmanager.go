//go:generate mockgen --build_flags=--mod=mod -destination=../../../mocks/services/streammanager/wunderkind/mock_wunderkind.go -package mock_wunderkind github.com/rudderlabs/rudder-server/services/streammanager/wunderkind LambdaClient

package wunderkind

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/mitchellh/mapstructure"

	"github.com/rudderlabs/rudder-go-kit/awsutil"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

type Producer struct {
	client LambdaClient
	logger logger.Logger
	conf   *config.Config
}

type LambdaClient interface {
	Invoke(ctx context.Context, input *lambda.InvokeInput, opts ...func(*lambda.Options)) (*lambda.InvokeOutput, error)
}

// NewProducer creates a producer based on destination config
func NewProducer(conf *config.Config, destination *backendconfig.DestinationT, o common.Opts) (common.Producer, error) {
	region := conf.GetString(WunderkindRegion, "")
	iamRoleArn := conf.GetString(WunderkindIamRoleArn, "")
	externalID := conf.GetString(WunderkindExternalId, "")

	sessionConfig := &awsutil.SessionConfig{
		Region:        region,
		IAMRoleARN:    iamRoleArn,
		ExternalID:    externalID,
		RoleBasedAuth: true,
	}
	sessionConfig.MaxIdleConnsPerHost = config.GetIntVar(64, 1, "Router.WUNDERKIND.httpMaxIdleConnsPerHost", "Router.WUNDERKIND.noOfWorkers", "Router.noOfWorkers")
	awsConfig, err := awsutil.CreateAWSConfig(context.Background(), sessionConfig)
	if err != nil {
		return nil, err
	}
	return &Producer{
		conf:   conf,
		client: lambda.NewFromConfig(awsConfig),
		logger: logger.NewLogger().Child("streammanager").Child("wunderkind"),
	}, nil
}

// Produce creates a producer and send data to Lambda.
func (p *Producer) Produce(jsonData json.RawMessage, destConfig any) (int, string, string) {
	client := p.client
	var input inputData
	err := jsonrs.Unmarshal(jsonData, &input)
	if err != nil {
		returnMessage := "[Wunderkind] error while unmarshalling jsonData :: " + err.Error()
		return http.StatusBadRequest, "Failure", returnMessage
	}
	if input.Payload == "" {
		return http.StatusBadRequest, "Failure", "[Wunderkind] error :: Invalid payload"
	}

	var config destinationConfig
	err = mapstructure.Decode(destConfig, &config)
	if err != nil {
		returnMessage := "[Wunderkind] error while unmarshalling destConfig :: " + err.Error()
		return http.StatusBadRequest, "Failure", returnMessage
	}

	var invokeInput lambda.InvokeInput
	wunderKindLambda := p.conf.GetString(WunderkindLambda, "")
	invokeInput.FunctionName = &wunderKindLambda
	invokeInput.Payload = []byte(input.Payload)
	invokeInput.InvocationType = InvocationType
	invokeInput.LogType = "Tail"

	response, err := client.Invoke(context.Background(), &invokeInput)
	if err != nil {
		statusCode, respStatus, responseMessage := common.ParseAWSError(err)
		p.logger.Warnn("Invocation",
			logger.NewStringField("statusCode", fmt.Sprint(statusCode)),
			logger.NewStringField("respStatus", respStatus),
			logger.NewStringField("responseMessage", responseMessage),
			obskit.Error(err),
		)
		return statusCode, respStatus, responseMessage
	}

	// handle a case where lambda invocation is successful, but there is an issue with the payload.
	if response.FunctionError != nil {
		statusCode := http.StatusBadRequest
		respStatus := "Failure"
		responseMessage := string(response.Payload)
		p.logger.Warnn("Function execution",
			logger.NewStringField("statusCode", fmt.Sprint(statusCode)),
			logger.NewStringField("respStatus", respStatus),
			logger.NewStringField("responseMessage", responseMessage),
			logger.NewStringField("functionError", *response.FunctionError),
		)
		return statusCode, respStatus, responseMessage
	}

	return http.StatusOK, "Success", "Event delivered to Wunderkind :: " + wunderKindLambda
}

func (*Producer) Close() error {
	// no-op
	return nil
}
