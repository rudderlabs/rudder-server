package lambda

import (
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/golang/mock/gomock"
	mock_lambda "github.com/rudderlabs/rudder-server/mocks/services/streammanager/lambda"
	mock_logger "github.com/rudderlabs/rudder-server/mocks/utils/logger"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var (
	sampleMessage  = "sample payload"
	sampleFunction = "sample function"
)

func TestNewProducer(t *testing.T) {
	destinationConfig := map[string]interface{}{
		"Region":     "us-east-1",
		"IAMRoleARN": "sampleRoleArn",
		"ExternalID": "sampleExternalID",
	}
	timeOut := 10 * time.Second
	producer, err := NewProducer(destinationConfig, common.Opts{Timeout: timeOut})
	assert.Nil(t, err)
	assert.NotNil(t, producer)
	assert.NotNil(t, producer.client)

	// Invalid Region
	destinationConfig = map[string]interface{}{
		"IAMRoleARN": "sampleRoleArn",
		"ExternalID": "sampleExternalID",
	}
	timeOut = 10 * time.Second
	producer, err = NewProducer(destinationConfig, common.Opts{Timeout: timeOut})
	assert.Nil(t, producer)
	assert.Equal(t, "could not find region configuration", err.Error())
}

func TestProduceWithInvalidClient(t *testing.T) {
	producer := &LambdaProducer{}
	sampleEventJson := []byte("{}")
	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Equal(t, "[Lambda] error :: Could not create client", respMsg)
}

func TestProduceWithInvalidData(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_lambda.NewMockLambdaClient(ctrl)
	producer := &LambdaProducer{client: mockClient}
	mockLogger := mock_logger.NewMockLoggerI(ctrl)
	pkgLogger = mockLogger

	// Invalid Payload
	sampleEventJson := []byte("invalid json")
	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Equal(t, "[Lambda] error :: Invalid payload", respMsg)

	// Empty Payload
	sampleEventJson = []byte("invalid json")
	statusCode, statusMsg, respMsg = producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Equal(t, "[Lambda] error :: Invalid payload", respMsg)

	// Invalid Destination Config
	sampleEventJson, _ = json.Marshal(map[string]interface{}{
		"payload": sampleMessage,
	})
	statusCode, statusMsg, respMsg = producer.Produce(sampleEventJson, "invalid config")
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Contains(t, respMsg, "[Lambda] error :: error while decoding destination config")

	// Empty Destination Config
	sampleDestConfig := map[string]interface{}{}
	sampleEventJson, _ = json.Marshal(map[string]interface{}{
		"payload": sampleMessage,
	})
	statusCode, statusMsg, respMsg = producer.Produce(sampleEventJson, sampleDestConfig)
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Contains(t, respMsg, "[Lambda] error :: Invalid invokeInput")
}

func TestProduceWithServiceResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_lambda.NewMockLambdaClient(ctrl)
	producer := &LambdaProducer{client: mockClient}
	mockLogger := mock_logger.NewMockLoggerI(ctrl)
	pkgLogger = mockLogger

	sampleEventJson, _ := json.Marshal(map[string]string{
		"payload": sampleMessage,
	})

	var sampleInput lambda.InvokeInput
	sampleInput.SetFunctionName(sampleFunction)
	sampleInput.SetPayload([]byte(sampleMessage))
	sampleInput.SetInvocationType("")

	sampleDestConfig := map[string]interface{}{
		"Lambda": sampleFunction,
	}

	mockClient.
		EXPECT().
		Invoke(&sampleInput).
		Return(&lambda.InvokeOutput{}, nil)
	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, sampleDestConfig)
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, "Success", statusMsg)
	assert.NotEmpty(t, respMsg)

	// return general Error
	errorCode := "errorCode"
	mockClient.
		EXPECT().
		Invoke(&sampleInput).
		Return(nil, errors.New(errorCode))
	mockLogger.EXPECT().Errorf(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
	statusCode, statusMsg, respMsg = producer.Produce(sampleEventJson, sampleDestConfig)
	assert.Equal(t, 500, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.NotEmpty(t, respMsg)

	// return aws error
	mockClient.
		EXPECT().
		Invoke(&sampleInput).
		Return(nil, awserr.NewRequestFailure(
			awserr.New(errorCode, errorCode, errors.New(errorCode)), 400, "request-id",
		))
	mockLogger.EXPECT().Errorf(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
	statusCode, statusMsg, respMsg = producer.Produce(sampleEventJson, sampleDestConfig)
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, errorCode, statusMsg)
	assert.NotEmpty(t, respMsg)
}
