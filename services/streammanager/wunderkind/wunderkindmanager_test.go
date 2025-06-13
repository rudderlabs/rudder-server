package wunderkind

import (
	"errors"
	"net/http"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go/aws/awserr"
	lambda_v1 "github.com/aws/aws-sdk-go/service/lambda"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/logger/mock_logger"
	mock_lambda_v1 "github.com/rudderlabs/rudder-server/mocks/services/streammanager/lambda_v1"
	mock_lambda_v2 "github.com/rudderlabs/rudder-server/mocks/services/streammanager/lambda_v2"
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

func TestProduceWithInvalidDataV1(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_lambda_v1.NewMockLambdaClientV1(ctrl)
	producer := &ProducerV1{client: mockClient}

	t.Run("Invalid input", func(t *testing.T) {
		sampleEventJson := []byte("invalid json")
		statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
		require.Equal(t, http.StatusBadRequest, statusCode)
		require.Equal(t, "Failure", statusMsg)
		require.Contains(t, respMsg, "[Wunderkind] error while unmarshalling jsonData ")
	})

	t.Run("Empty payload", func(t *testing.T) {
		sampleEventJson, _ := jsonrs.Marshal(map[string]interface{}{
			"payload": "",
		})
		statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
		require.Equal(t, http.StatusBadRequest, statusCode)
		require.Equal(t, "Failure", statusMsg)
		require.Contains(t, respMsg, "[Wunderkind] error :: Invalid payload")
	})
}

func TestProduceWithServiceResponseV1(t *testing.T) {
	conf := config.New()
	conf.Set("WUNDERKIND_REGION", "us-east-1")
	conf.Set("WUNDERKIND_IAM_ROLE_ARN", sampleIAMRoleARN)
	conf.Set("WUNDERKIND_EXTERNAL_ID", sampleExternalID)
	conf.Set("WUNDERKIND_LAMBDA", sampleFunction)

	ctrl := gomock.NewController(t)
	mockClient := mock_lambda_v1.NewMockLambdaClientV1(ctrl)
	mockLogger := mock_logger.NewMockLogger(ctrl)
	producer := &ProducerV1{conf: conf, client: mockClient, logger: mockLogger}

	sampleEventJson, _ := jsonrs.Marshal(map[string]interface{}{
		"payload": sampleMessage,
	})

	destConfig := map[string]string{}

	var sampleInput lambda_v1.InvokeInput
	sampleInput.FunctionName = &sampleFunction
	sampleInput.Payload = []byte(sampleMessage)
	sampleInput.InvocationType = aws.String(invocationType)
	sampleInput.LogType = aws.String("Tail")

	t.Run("success", func(t *testing.T) {
		mockClient.EXPECT().Invoke(&sampleInput).Return(&lambda_v1.InvokeOutput{}, nil)
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
		mockClient.EXPECT().Invoke(&sampleInput).Return(&lambda_v1.InvokeOutput{
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

func TestProduceWithInvalidDataV2(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_lambda_v2.NewMockLambdaClientV2(ctrl)
	producer := &ProducerV2{client: mockClient}

	t.Run("Invalid input", func(t *testing.T) {
		sampleEventJson := []byte("invalid json")
		statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
		require.Equal(t, http.StatusBadRequest, statusCode)
		require.Equal(t, "Failure", statusMsg)
		require.Contains(t, respMsg, "[Wunderkind] error while unmarshalling jsonData ")
	})

	t.Run("Empty payload", func(t *testing.T) {
		sampleEventJson, _ := jsonrs.Marshal(map[string]interface{}{
			"payload": "",
		})
		statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
		require.Equal(t, http.StatusBadRequest, statusCode)
		require.Equal(t, "Failure", statusMsg)
		require.Contains(t, respMsg, "[Wunderkind] error :: Invalid payload")
	})
}

func TestProduceWithServiceResponseV2(t *testing.T) {
	conf := config.New()
	conf.Set("WUNDERKIND_REGION", "us-east-1")
	conf.Set("WUNDERKIND_IAM_ROLE_ARN", sampleIAMRoleARN)
	conf.Set("WUNDERKIND_EXTERNAL_ID", sampleExternalID)
	conf.Set("WUNDERKIND_LAMBDA", sampleFunction)

	ctrl := gomock.NewController(t)
	mockClient := mock_lambda_v2.NewMockLambdaClientV2(ctrl)
	mockLogger := mock_logger.NewMockLogger(ctrl)
	producer := &ProducerV2{conf: conf, client: mockClient, logger: mockLogger}

	sampleEventJson, _ := jsonrs.Marshal(map[string]interface{}{
		"payload": sampleMessage,
	})

	destConfig := map[string]string{}

	var sampleInput lambda.InvokeInput
	sampleInput.FunctionName = &sampleFunction
	sampleInput.Payload = []byte(sampleMessage)
	sampleInput.InvocationType = types.InvocationType(invocationType)
	sampleInput.LogType = types.LogTypeTail

	t.Run("success", func(t *testing.T) {
		mockClient.EXPECT().Invoke(gomock.Any(), &sampleInput).Return(&lambda.InvokeOutput{}, nil)
		statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, destConfig)
		require.Equal(t, http.StatusOK, statusCode)
		require.Equal(t, "Success", statusMsg)
		require.NotEmpty(t, respMsg)
	})

	t.Run("general error", func(t *testing.T) {
		errorCode := "errorCode"
		mockClient.EXPECT().Invoke(gomock.Any(), &sampleInput).Return(nil, errors.New(errorCode))
		mockLogger.EXPECT().Warnn(gomock.Any(), gomock.Any()).Times(1)
		statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, destConfig)
		require.Equal(t, http.StatusInternalServerError, statusCode)
		require.Equal(t, "Failure", statusMsg)
		require.NotEmpty(t, respMsg)
	})

	t.Run("when lambda invocation is successful, but there is an issue with the payload", func(t *testing.T) {
		mockClient.EXPECT().Invoke(gomock.Any(), &sampleInput).Return(&lambda.InvokeOutput{
			StatusCode:      *aws.Int32(http.StatusOK),
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
		mockClient.EXPECT().Invoke(gomock.Any(), &sampleInput).Return(
			nil, &smithy.GenericAPIError{
				Code:    errorCode,
				Message: errorCode,
				Fault:   smithy.FaultClient,
			})
		mockLogger.EXPECT().Warnn(gomock.Any(), gomock.Any()).Times(1)
		statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, destConfig)
		require.Equal(t, http.StatusBadRequest, statusCode)
		require.Equal(t, errorCode, statusMsg)
		require.NotEmpty(t, respMsg)
	})
}
