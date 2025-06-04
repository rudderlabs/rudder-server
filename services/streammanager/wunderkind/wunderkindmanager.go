package wunderkind

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go/service/lambda"

	"github.com/rudderlabs/rudder-go-kit/awsutil"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

type ProducerV1 struct {
	conf   *config.Config
	client lambdaClientV1
	logger logger.Logger
}

type lambdaClientV1 interface {
	Invoke(input *lambda.InvokeInput) (*lambda.InvokeOutput, error)
}

// NewProducer creates a producer based on destination config
func NewProducerV1(conf *config.Config, log logger.Logger) (*ProducerV1, error) {
	if err := validate(conf); err != nil {
		return nil, fmt.Errorf("invalid environment config: %w", err)
	}
	sessionConfig := &awsutil.SessionConfig{
		Region:        conf.GetString(WunderkindRegion, ""),
		IAMRoleARN:    conf.GetString(WunderkindIamRoleArn, ""),
		ExternalID:    conf.GetString(WunderkindExternalId, ""),
		RoleBasedAuth: true,
	}
	awsSession, err := awsutil.CreateSession(sessionConfig)
	if err != nil {
		return nil, fmt.Errorf("creating session: %w", err)
	}

	return &ProducerV1{
		conf:   conf,
		client: lambda.New(awsSession),
		logger: log.Child("wunderkind"),
	}, nil
}

// Produce creates a producer and send data to Lambda.
func (p *ProducerV1) Produce(jsonData json.RawMessage, _ interface{}) (int, string, string) {
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

	var invokeInput lambda.InvokeInput
	wunderKindLambda := p.conf.GetString(WunderkindLambda, "")
	invokeInput.SetFunctionName(wunderKindLambda)
	invokeInput.SetPayload([]byte(input.Payload))
	invokeInput.SetInvocationType(InvocationType)
	invokeInput.SetLogType("Tail")

	if err = invokeInput.Validate(); err != nil {
		return http.StatusBadRequest, "Failure", "[Wunderkind] error :: Invalid invokeInput :: " + err.Error()
	}

	response, err := client.Invoke(&invokeInput)
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

func (*ProducerV1) Close() error {
	// no-op
	return nil
}
