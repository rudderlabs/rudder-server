package firehose

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_firehose "github.com/rudderlabs/rudder-server/mocks/services/streammanager/firehose"

	"github.com/rudderlabs/rudder-go-kit/logger/mock_logger"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

var (
	sampleDeliveryStreamName = "sampleDeliveryStream"
	sampleMessage            = "sample respMsg"
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
	producer, err := NewProducer(&destination, common.Opts{Timeout: timeOut})
	assert.Nil(t, err)
	assert.NotNil(t, producer)
	assert.NotNil(t, producer.client)
}

func TestProduceWithInvalidClient(t *testing.T) {
	producer := &FireHoseProducer{}
	sampleEventJson := []byte("{}")
	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Equal(t, "[FireHose] error :: Could not create producer", respMsg)
}

func TestProduceWithInvalidData(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_firehose.NewMockFireHoseClient(ctrl)
	producer := &FireHoseProducer{client: mockClient}

	// Invalid Payload
	sampleEventJson := []byte("invalid json")
	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Equal(t, "[FireHose] error :: message from payload not found", respMsg)

	// Empty Payload
	sampleEventJson = []byte("{}")
	statusCode, statusMsg, respMsg = producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Equal(t, "[FireHose] error :: message from payload not found", respMsg)

	// Payload without deliveryStreamMapTo
	sampleEventJson, _ = jsonrs.Marshal(map[string]string{
		"message": sampleMessage,
	})
	statusCode, statusMsg, respMsg = producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Equal(t, "[FireHose] error  :: Delivery Stream not found", respMsg)

	// Payload with empty deliveryStreamMapTo
	sampleEventJson, _ = jsonrs.Marshal(map[string]interface{}{
		"message":             sampleMessage,
		"deliveryStreamMapTo": "",
	})
	statusCode, statusMsg, respMsg = producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Equal(t, "[FireHose] error :: empty delivery stream", respMsg)

	// Payload with invalid deliveryStreamMapTo
	sampleEventJson, _ = jsonrs.Marshal(map[string]interface{}{
		"message":             sampleMessage,
		"deliveryStreamMapTo": 1,
	})
	statusCode, statusMsg, respMsg = producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Equal(t, "[FireHose] error :: Could not parse delivery stream to string", respMsg)
}

func TestProduceWithServiceResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_firehose.NewMockFireHoseClient(ctrl)
	producer := &FireHoseProducer{client: mockClient}
	mockLogger := mock_logger.NewMockLogger(ctrl)
	pkgLogger = mockLogger

	sampleEventJson, _ := jsonrs.Marshal(map[string]string{
		"message":             sampleMessage,
		"deliveryStreamMapTo": sampleDeliveryStreamName,
	})

	sampleMessageJson, _ := jsonrs.Marshal(sampleMessage)

	sampleRecord := firehose.PutRecordInput{
		DeliveryStreamName: aws.String(sampleDeliveryStreamName),
		Record:             &types.Record{Data: sampleMessageJson},
	}

	mockClient.
		EXPECT().
		PutRecord(gomock.Any(), &sampleRecord, gomock.Any()).
		Return(&firehose.PutRecordOutput{}, nil)
	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, "Success", statusMsg)
	assert.NotEmpty(t, respMsg)

	// return general Error
	errorCode := "errorCode"
	mockClient.
		EXPECT().
		PutRecord(gomock.Any(), &sampleRecord, gomock.Any()).
		Return(nil, errors.New(errorCode))
	mockLogger.EXPECT().Errorf(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
	statusCode, statusMsg, respMsg = producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 500, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.NotEmpty(t, respMsg)

	// return aws error
	mockClient.
		EXPECT().
		PutRecord(gomock.Any(), &sampleRecord, gomock.Any()).
		Return(nil, &smithy.GenericAPIError{
			Code:    errorCode,
			Message: errorCode,
			Fault:   smithy.FaultClient,
		})
	mockLogger.EXPECT().Errorf(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
	statusCode, statusMsg, respMsg = producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, errorCode, statusMsg)
	assert.NotEmpty(t, respMsg)
}
