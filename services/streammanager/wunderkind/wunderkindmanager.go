package wunderkind

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go/service/lambda"
	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-go-kit/awsutil"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

const (
	InvocationType       = "RequestResponse"
	WunderkindRegion     = "WUNDERKIND_REGION"
	WunderkindIamRoleArn = "WUNDERKIND_IAM_ROLE_ARN"
	WunderkindExternalId = "WUNDERKIND_EXTERNAL_ID"
	WunderkindLambda     = "WUNDERKIND_LAMBDA"
)

var (
	jsonFast = jsoniter.ConfigCompatibleWithStandardLibrary
)

type inputData struct {
	Payload string `json:"payload"`
}

type Producer struct {
	conf   *config.Config
	client lambdaClient
	logger logger.Logger
}

type lambdaClient interface {
	Invoke(input *lambda.InvokeInput) (*lambda.InvokeOutput, error)
}

// NewProducer creates a producer based on destination config
func NewProducer(conf *config.Config) (*Producer, error) {
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

	return &Producer{
		conf:   conf,
		client: lambda.New(awsSession),
		logger: logger.NewLogger().Child("streammanager").Child("wunderkind"),
	}, nil
}

// Produce creates a producer and send data to Lambda.
func (p *Producer) Produce(jsonData json.RawMessage, _ interface{}) (int, string, string) {
	client := p.client
	if client == nil {
		return http.StatusBadRequest, "Failure", "[Wunderkind] error :: Could not create client"
	}

	var input inputData
	err := jsonFast.Unmarshal(jsonData, &input)
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
		p.logger.Errorf("Invocation error :: %d : %s : %s", statusCode, respStatus, responseMessage)
		return statusCode, respStatus, responseMessage
	}

	// handle a case where lambda invocation is successful, but there is an issue with the payload.
	if response.FunctionError != nil {
		statusCode := http.StatusBadRequest
		respStatus := "Failure"
		responseMessage := string(response.Payload)
		p.logger.Errorf("Function execution error :: %d : %s : %s", statusCode, respStatus, responseMessage)
		return statusCode, respStatus, responseMessage
	}

	return http.StatusOK, "Success", "Event delivered to Wunderkind :: " + wunderKindLambda
}

func (*Producer) Close() error {
	// no-op
	return nil
}
