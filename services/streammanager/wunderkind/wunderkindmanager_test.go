package wunderkind

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/logger/mock_logger"
	mock_lambda "github.com/rudderlabs/rudder-server/mocks/services/streammanager/lambda"
)

var (
	sampleMessage    = "sample payload"
	sampleFunction   = "sampleLambdaFunction"
	sampleExternalID = "sampleExternalID"
	sampleIAMRoleARN = "sampleRoleArn"
	invocationType   = "RequestResponse"
)

func TestNewProducer(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		conf := config.New()
		conf.Set("WUNDERKIND_REGION", "us-east-1")
		conf.Set("WUNDERKIND_IAM_ROLE_ARN", sampleIAMRoleARN)
		conf.Set("WUNDERKIND_EXTERNAL_ID", sampleExternalID)
		conf.Set("WUNDERKIND_LAMBDA", sampleFunction)
		producer, err := NewProducer(conf, logger.NOP)
		require.Nil(t, err)
		require.NotNil(t, producer)
		require.NotNil(t, producer.client)
	})

	t.Run("empty external id", func(t *testing.T) {
		conf := config.New()
		conf.Set("WUNDERKIND_REGION", "us-east-1")
		conf.Set("WUNDERKIND_IAM_ROLE_ARN", sampleIAMRoleARN)
		conf.Set("WUNDERKIND_EXTERNAL_ID", "")
		conf.Set("WUNDERKIND_LAMBDA", sampleFunction)
		producer, err := NewProducer(conf, logger.NOP)
		require.Nil(t, producer)
		require.Equal(t, "invalid environment config: external id cannot be empty", err.Error())
	})
}

func TestProduceWithInvalidData(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_lambda.NewMockLambdaClient(ctrl)
	producer := &Producer{client: mockClient}

	t.Run("Invalid input", func(t *testing.T) {
		sampleEventJson := []byte("invalid json")
		statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
		require.Equal(t, http.StatusBadRequest, statusCode)
		require.Equal(t, "Failure", statusMsg)
		require.Contains(t, respMsg, "[Wunderkind] error while unmarshalling jsonData ")
	})

	t.Run("Empty payload", func(t *testing.T) {
		sampleEventJson, _ := json.Marshal(map[string]interface{}{
			"payload": "",
		})
		statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
		require.Equal(t, http.StatusBadRequest, statusCode)
		require.Equal(t, "Failure", statusMsg)
		require.Contains(t, respMsg, "[Wunderkind] error :: Invalid payload")
	})
}

func TestProduceWithServiceResponse(t *testing.T) {
	conf := config.New()
	conf.Set("WUNDERKIND_REGION", "us-east-1")
	conf.Set("WUNDERKIND_IAM_ROLE_ARN", sampleIAMRoleARN)
	conf.Set("WUNDERKIND_EXTERNAL_ID", sampleExternalID)
	conf.Set("WUNDERKIND_LAMBDA", sampleFunction)

	ctrl := gomock.NewController(t)
	mockClient := mock_lambda.NewMockLambdaClient(ctrl)
	mockLogger := mock_logger.NewMockLogger(ctrl)
	producer := &Producer{conf: conf, client: mockClient, logger: mockLogger}

	sampleEventJson, _ := json.Marshal(map[string]interface{}{
		"payload": sampleMessage,
	})

	destConfig := map[string]string{}

	var sampleInput lambda.InvokeInput
	sampleInput.SetFunctionName(sampleFunction)
	sampleInput.SetPayload([]byte(sampleMessage))
	sampleInput.SetInvocationType(invocationType)
	sampleInput.SetLogType("Tail")

	t.Run("success", func(t *testing.T) {
		mockClient.EXPECT().Invoke(&sampleInput).Return(&lambda.InvokeOutput{}, nil)
		statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, destConfig)
		require.Equal(t, http.StatusOK, statusCode)
		require.Equal(t, "Success", statusMsg)
		require.NotEmpty(t, respMsg)
	})

	t.Run("general error", func(t *testing.T) {
		errorCode := "errorCode"
		mockClient.EXPECT().Invoke(&sampleInput).Return(nil, errors.New(errorCode))
		mockLogger.EXPECT().Warnn(gomock.Any(), gomock.Any()).Times(1)
		statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, destConfig)
		require.Equal(t, http.StatusInternalServerError, statusCode)
		require.Equal(t, "Failure", statusMsg)
		require.NotEmpty(t, respMsg)
	})

	t.Run("when lambda invocation is successful, but there is an issue with the payload", func(t *testing.T) {
		mockClient.EXPECT().Invoke(&sampleInput).Return(&lambda.InvokeOutput{
			StatusCode:      aws.Int64(http.StatusOK),
			FunctionError:   aws.String("Unhandled"),
			ExecutedVersion: aws.String("$LATEST"),
		}, nil)
		mockLogger.EXPECT().Warnn(gomock.Any(), gomock.Any()).Times(1)
		statusCode, statusMsg, _ := producer.Produce(sampleEventJson, destConfig)
		require.Equal(t, http.StatusBadRequest, statusCode)
		require.Equal(t, "Failure", statusMsg)
	})

	t.Run("aws error", func(t *testing.T) {
		errorCode := "errorCode"
		mockClient.EXPECT().Invoke(&sampleInput).Return(
			nil, awserr.NewRequestFailure(awserr.New(errorCode, errorCode, errors.New(errorCode)), http.StatusBadRequest, "request-id"))
		mockLogger.EXPECT().Warnn(gomock.Any(), gomock.Any()).Times(1)
		statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, destConfig)
		require.Equal(t, http.StatusBadRequest, statusCode)
		require.Equal(t, errorCode, statusMsg)
		require.NotEmpty(t, respMsg)
	})
}
