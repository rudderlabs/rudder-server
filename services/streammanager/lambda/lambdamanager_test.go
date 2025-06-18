package lambda

import (
	"errors"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger/mock_logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_lambda "github.com/rudderlabs/rudder-server/mocks/services/streammanager/lambda_v2"
	common "github.com/rudderlabs/rudder-server/services/streammanager/common"
)

var (
	sampleMessage       = "sample payload"
	sampleFunction      = "sample function"
	sampleClientContext = "sample client context"
	invocationType      = "Event"
)

func TestNewProducer(t *testing.T) {
	destinationConfig := map[string]interface{}{
		"Region":     "us-east-1",
		"IAMRoleARN": "sampleRoleArn",
		"ExternalID": "sampleExternalID",
	}
	destination := backendconfig.DestinationT{
		Config:      destinationConfig,
		WorkspaceID: "sampleWorkspaceID",
	}
	timeOut := 10 * time.Second
	producer, err := NewProducerV2(&destination, common.Opts{Timeout: timeOut})
	assert.Nil(t, err)
	assert.NotNil(t, producer)

	// Invalid Region
	destinationConfig = map[string]interface{}{
		"IAMRoleARN": "sampleRoleArn",
		"ExternalID": "sampleExternalID",
	}
	destination.Config = destinationConfig
	timeOut = 10 * time.Second
	producer, err = NewProducerV2(&destination, common.Opts{Timeout: timeOut})
	assert.Nil(t, producer)
	assert.Equal(t, "could not find region configuration", err.Error())
}

func TestProduceWithInvalidClient(t *testing.T) {
	producer := &LambdaProducerV2{}
	sampleEventJson := []byte("{}")
	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Equal(t, "[Lambda] error :: Could not create client", respMsg)
}

func TestProduceWithInvalidData(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_lambda.NewMockLambdaClientV2(ctrl)
	producer := &LambdaProducerV2{client: mockClient}
	mockLogger := mock_logger.NewMockLogger(ctrl)
	pkgLogger = mockLogger

	// Invalid input
	sampleEventJson := []byte("invalid json")
	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Contains(t, respMsg, "[Lambda] error while unmarshalling jsonData")

	// Empty payload
	sampleEventJson, _ = jsonrs.Marshal(map[string]interface{}{
		"payload": "",
	})
	statusCode, statusMsg, respMsg = producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Contains(t, respMsg, "[Lambda] error :: Invalid payload")
}

func TestProduceWithServiceResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_lambda.NewMockLambdaClientV2(ctrl)
	producer := &LambdaProducerV2{client: mockClient}
	mockLogger := mock_logger.NewMockLogger(ctrl)
	pkgLogger = mockLogger

	sampleEventJson, _ := jsonrs.Marshal(map[string]interface{}{
		"payload": sampleMessage,
	})

	destConfig := map[string]string{
		"lambda":        sampleFunction,
		"clientContext": sampleClientContext,
	}

	var sampleInput lambda.InvokeInput
	sampleInput.FunctionName = &sampleFunction
	sampleInput.Payload = []byte(sampleMessage)
	sampleInput.InvocationType = types.InvocationType(invocationType)
	sampleInput.ClientContext = &sampleClientContext

	mockClient.
		EXPECT().
		Invoke(gomock.Any(), &sampleInput).
		Return(&lambda.InvokeOutput{}, nil)
	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, destConfig)
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, "Success", statusMsg)
	assert.NotEmpty(t, respMsg)

	// return general Error
	errorCode := "errorCode"
	mockClient.
		EXPECT().
		Invoke(gomock.Any(), &sampleInput).
		Return(nil, errors.New(errorCode))
	mockLogger.EXPECT().Errorf(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
	statusCode, statusMsg, respMsg = producer.Produce(sampleEventJson, destConfig)
	assert.Equal(t, 500, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.NotEmpty(t, respMsg)

	mockClient.
		EXPECT().
		Invoke(gomock.Any(), &sampleInput).
		Return(nil, &smithy.GenericAPIError{
			Code:    errorCode,
			Message: errorCode,
			Fault:   smithy.FaultClient,
		})
	mockLogger.EXPECT().Errorf(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
	statusCode, statusMsg, respMsg = producer.Produce(sampleEventJson, destConfig)
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, errorCode, statusMsg)
	assert.NotEmpty(t, respMsg)
}
