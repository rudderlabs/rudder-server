package kinesis

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-go-kit/logger/mock_logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_kinesis "github.com/rudderlabs/rudder-server/mocks/services/streammanager/kinesis"

	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

func TestKinesis(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Kinesis Suite")
}

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
	producer := &KinesisProducer{}
	sampleJsonPayload := []byte("{}")
	statusCode, statusMsg, respMsg := producer.Produce(sampleJsonPayload, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Could not create producer for Kinesis", statusMsg)
	assert.Equal(t, "Could not create producer for Kinesis", respMsg)
}

var validDestinationConfigUseMessageID = Config{
	Stream:       "stream",
	UseMessageID: true,
}

var validDestinationConfigNotUseMessageID = Config{
	Stream:       "stream",
	UseMessageID: false,
}

func TestProduceWithInvalidData(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_kinesis.NewMockKinesisClient(ctrl)
	producer := &KinesisProducer{client: mockClient}

	// Invalid destination config
	sampleJsonPayload := []byte("{}")
	statusCode, statusMsg, respMsg := producer.Produce(sampleJsonPayload, "invalid json")
	assert.Equal(t, 400, statusCode)
	assert.Contains(t, statusMsg, "Error while Unmarshalling destination config")
	assert.Contains(t, respMsg, "Error while Unmarshalling destination config")

	// Invalid Json
	sampleJsonPayload = []byte("invalid json")
	statusCode, statusMsg, respMsg = producer.Produce(sampleJsonPayload, validDestinationConfigUseMessageID)
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "InvalidPayload", statusMsg)
	assert.Equal(t, "Empty Payload", respMsg)

	// Empty Payload
	sampleJsonPayload = []byte("{}")
	statusCode, statusMsg, respMsg = producer.Produce(sampleJsonPayload, validDestinationConfigUseMessageID)
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "InvalidPayload", statusMsg)
	assert.Equal(t, "Empty Payload", respMsg)

	// Incomplete Payload
	sampleJsonPayload, _ = json.Marshal(map[string]string{
		"message": "{}",
	})
	statusCode, statusMsg, respMsg = producer.Produce(sampleJsonPayload, validDestinationConfigUseMessageID)
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "InvalidInput", statusMsg)
	assert.Contains(t, respMsg, "InvalidParameter")
}

func TestProduceWithServiceResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_kinesis.NewMockKinesisClient(ctrl)
	producer := &KinesisProducer{client: mockClient}
	mockLogger := mock_logger.NewMockLogger(ctrl)
	pkgLogger = mockLogger

	sampleData := "some data"
	sampleUserId := "someUser"
	sampleJsonPayload, _ := json.Marshal(map[string]string{
		"message": sampleData,
		"userId":  sampleUserId,
	})
	dataPayloadJson, _ := json.Marshal(sampleData)
	putRecordInput := kinesis.PutRecordInput{
		Data:         dataPayloadJson,
		StreamName:   &validDestinationConfigUseMessageID.Stream,
		PartitionKey: aws.String(sampleUserId),
	}

	// Return success response
	mockClient.EXPECT().PutRecord(&putRecordInput).Return(&kinesis.PutRecordOutput{
		SequenceNumber: aws.String("sequenceNumber"),
		ShardId:        aws.String("shardId"),
	}, nil)

	statusCode, statusMsg, respMsg := producer.Produce(sampleJsonPayload, validDestinationConfigUseMessageID)
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, "Success", statusMsg)
	assert.Contains(t, respMsg, "Message delivered")

	// Return service error
	errorCode := "someError"
	mockClient.EXPECT().PutRecord(&putRecordInput).Return(nil, awserr.NewRequestFailure(
		awserr.New(errorCode, errorCode, errors.New(errorCode)), 400, "request-id",
	))
	mockLogger.EXPECT().Errorf(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)

	statusCode, statusMsg, respMsg = producer.Produce(sampleJsonPayload, validDestinationConfigNotUseMessageID)
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, errorCode, statusMsg)
	assert.Contains(t, respMsg, errorCode)
}
