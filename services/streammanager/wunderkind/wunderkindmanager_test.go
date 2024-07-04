package wunderkind

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-go-kit/logger/mock_logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_lambda "github.com/rudderlabs/rudder-server/mocks/services/streammanager/lambda"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

var (
	sampleMessage    = "sample payload"
	sampleFunction   = "sampleLambdaFunction"
	sampleExternalID = "sampleExternalID"
	sampleIAMRoleARN = "sampleRoleArn"
	invocationType   = "RequestResponse"
)

func TestNewProducer(t *testing.T) {
	os.Setenv("WUNDERKIND_REGION", "us-east-1")
	os.Setenv("WUNDERKIND_IAM_ROLE_ARN", sampleIAMRoleARN)
	os.Setenv("WUNDERKIND_EXTERNAL_ID", sampleExternalID)
	os.Setenv("WUNDERKIND_LAMBDA", sampleFunction)

	defer os.Unsetenv("WUNDERKIND_REGION")
	defer os.Unsetenv("WUNDERKIND_IAM_ROLE_ARN")
	defer os.Unsetenv("WUNDERKIND_EXTERNAL_ID")
	defer os.Unsetenv("WUNDERKIND_LAMBDA")
	destinationConfig := map[string]interface{}{}
	destination := backendconfig.DestinationT{
		Config:      destinationConfig,
		WorkspaceID: "sampleWorkspaceID",
	}
	timeOut := 10 * time.Second
	producer, err := NewProducer(&destination, common.Opts{Timeout: timeOut})
	assert.Nil(t, err)
	assert.NotNil(t, producer)
	assert.NotNil(t, producer.client)

	// Invalid Region
	os.Setenv("WUNDERKIND_EXTERNAL_ID", "")
	destinationConfig = map[string]interface{}{}
	destination.Config = destinationConfig
	timeOut = 10 * time.Second
	producer, err = NewProducer(&destination, common.Opts{Timeout: timeOut})
	assert.Nil(t, producer)
	assert.Equal(t, "externalID is required for IAM role", err.Error())
}

func TestProduceWithInvalidClient(t *testing.T) {
	producer := &WunderkindProducer{}
	sampleEventJson := []byte("{}")
	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Equal(t, "[Wunderkind] error :: Could not create client", respMsg)
}

func TestProduceWithInvalidData(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_lambda.NewMockLambdaClient(ctrl)
	producer := &WunderkindProducer{client: mockClient}
	mockLogger := mock_logger.NewMockLogger(ctrl)
	pkgLogger = mockLogger

	// Invalid input
	sampleEventJson := []byte("invalid json")
	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Contains(t, respMsg, "[Wunderkind] error while unmarshalling jsonData ")

	// Empty payload
	sampleEventJson, _ = json.Marshal(map[string]interface{}{
		"payload": "",
	})
	statusCode, statusMsg, respMsg = producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Contains(t, respMsg, "[Wunderkind] error :: Invalid payload")
}

func TestProduceWithServiceResponse(t *testing.T) {
	os.Setenv("WUNDERKIND_REGION", "us-east-1")
	os.Setenv("WUNDERKIND_IAM_ROLE_ARN", sampleIAMRoleARN)
	os.Setenv("WUNDERKIND_EXTERNAL_ID", sampleExternalID)
	os.Setenv("WUNDERKIND_LAMBDA", sampleFunction)

	defer os.Unsetenv("WUNDERKIND_REGION")
	defer os.Unsetenv("WUNDERKIND_IAM_ROLE_ARN")
	defer os.Unsetenv("WUNDERKIND_EXTERNAL_ID")
	defer os.Unsetenv("WUNDERKIND_LAMBDA")

	ctrl := gomock.NewController(t)
	mockClient := mock_lambda.NewMockLambdaClient(ctrl)
	producer := &WunderkindProducer{client: mockClient}
	mockLogger := mock_logger.NewMockLogger(ctrl)
	pkgLogger = mockLogger

	sampleEventJson, _ := json.Marshal(map[string]interface{}{
		"payload": sampleMessage,
	})

	destConfig := map[string]string{}

	var sampleInput lambda.InvokeInput
	sampleInput.SetFunctionName(sampleFunction)
	sampleInput.SetPayload([]byte(sampleMessage))
	sampleInput.SetInvocationType(invocationType)
	sampleInput.SetLogType("Tail")

	mockClient.EXPECT().Invoke(&sampleInput).Return(&lambda.InvokeOutput{}, nil)
	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, destConfig)
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, "Success", statusMsg)
	assert.NotEmpty(t, respMsg)

	// return general Error
	errorCode := "errorCode"
	mockClient.EXPECT().Invoke(&sampleInput).Return(nil, errors.New(errorCode))
	mockLogger.EXPECT().Errorf(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
	statusCode, statusMsg, respMsg = producer.Produce(sampleEventJson, destConfig)
	assert.Equal(t, 500, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.NotEmpty(t, respMsg)

	// return an error when lambda invocation is successful, but there is an issue with the payload
	mockClient.EXPECT().Invoke(&sampleInput).Return(&lambda.InvokeOutput{
		StatusCode:      aws.Int64(200),
		FunctionError:   aws.String("Unhandled"),
		ExecutedVersion: aws.String("$LATEST"),
	}, nil)
	mockLogger.EXPECT().Errorf(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
	statusCode, statusMsg, _ = producer.Produce(sampleEventJson, destConfig)
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)

	// return aws error
	mockClient.EXPECT().Invoke(&sampleInput).Return(
		nil, awserr.NewRequestFailure(awserr.New(errorCode, errorCode, errors.New(errorCode)), 400, "request-id"))
	mockLogger.EXPECT().Errorf(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
	statusCode, statusMsg, respMsg = producer.Produce(sampleEventJson, destConfig)
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, errorCode, statusMsg)
	assert.NotEmpty(t, respMsg)
}
