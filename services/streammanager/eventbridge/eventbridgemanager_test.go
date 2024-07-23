package eventbridge

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger/mock_logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_eventbridge "github.com/rudderlabs/rudder-server/mocks/services/streammanager/eventbridge"

	"go.uber.org/mock/gomock"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/eventbridge"
	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-server/services/streammanager/common"
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

var sampleEvent = eventbridge.PutEventsRequestEntry{
	Detail:       aws.String("detail"),
	DetailType:   aws.String("detailType"),
	EventBusName: aws.String("eventBus"),
	Source:       aws.String("source"),
	TraceHeader:  aws.String("header"),
}

func TestProduceHappyCase(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_eventbridge.NewMockEventBridgeClient(ctrl)
	producer := &EventBridgeProducer{client: mockClient}
	mockClient.
		EXPECT().
		PutEvents(&eventbridge.PutEventsInput{Entries: []*eventbridge.PutEventsRequestEntry{
			&sampleEvent,
		}}).
		Return(&eventbridge.PutEventsOutput{Entries: []*eventbridge.PutEventsResultEntry{{}}}, nil)
	sampleEventJson, _ := json.Marshal(sampleEvent)
	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, "Success", statusMsg)
	assert.NotEmpty(t, respMsg)
}

func TestProduceWithInvalidClient(t *testing.T) {
	producer := &EventBridgeProducer{}
	sampleEventJson := []byte("Invalid json")
	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Could not create producer for EventBridge", statusMsg)
	assert.NotEmpty(t, respMsg)
}

func TestProduceWithInvalidJson(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_eventbridge.NewMockEventBridgeClient(ctrl)
	producer := &EventBridgeProducer{client: mockClient}
	sampleEventJson := []byte("Invalid json")
	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "[EventBridge] Failed to create eventbridge event", statusMsg)
	assert.NotEmpty(t, respMsg)
}

func TestProduceWithBadResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockLogger := mock_logger.NewMockLogger(ctrl)
	pkgLogger = mockLogger
	mockClient := mock_eventbridge.NewMockEventBridgeClient(ctrl)
	producer := &EventBridgeProducer{client: mockClient}
	errorCode := "SomeError"
	// Failed response
	mockClient.
		EXPECT().
		PutEvents(&eventbridge.PutEventsInput{Entries: []*eventbridge.PutEventsRequestEntry{
			&sampleEvent,
		}}).
		Return(&eventbridge.PutEventsOutput{Entries: []*eventbridge.PutEventsResultEntry{
			{ErrorCode: aws.String(errorCode), ErrorMessage: aws.String(errorCode)},
		}}, nil)
	sampleEventJson, _ := json.Marshal(sampleEvent)
	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, errorCode, statusMsg)
	assert.Equal(t, errorCode, respMsg)

	// Empty Response
	mockClient.
		EXPECT().
		PutEvents(&eventbridge.PutEventsInput{Entries: []*eventbridge.PutEventsRequestEntry{
			&sampleEvent,
		}}).
		Return(&eventbridge.PutEventsOutput{Entries: []*eventbridge.PutEventsResultEntry{}}, nil)
	statusCode, statusMsg, respMsg = producer.Produce(sampleEventJson, map[string]string{})

	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failed to send event to eventbridge", statusMsg)
	assert.Equal(t, "Failed to send event to eventbridge", respMsg)

	// Return error
	mockClient.
		EXPECT().
		PutEvents(&eventbridge.PutEventsInput{Entries: []*eventbridge.PutEventsRequestEntry{
			&sampleEvent,
		}}).
		Return(nil, awserr.NewRequestFailure(
			awserr.New(errorCode, errorCode, errors.New(errorCode)), 400, "request-id",
		))
	mockLogger.EXPECT().Errorf(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)

	statusCode, statusMsg, respMsg = producer.Produce(sampleEventJson, map[string]string{})

	assert.Equal(t, 400, statusCode)
	assert.Equal(t, errorCode, statusMsg)
	assert.NotEmpty(t, respMsg)
}
