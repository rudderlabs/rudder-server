package personalize

import (
	"fmt"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/personalizeevents"
	"github.com/aws/aws-sdk-go-v2/service/personalizeevents/types"
	"github.com/aws/smithy-go"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger/mock_logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_personalize "github.com/rudderlabs/rudder-server/mocks/services/streammanager/personalize_v2"

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
}

func TestProduceWithInvalidClient(t *testing.T) {
	producer := &PersonalizeProducerV2{}
	sampleJsonPayload := []byte("{}")
	statusCode, statusMsg, respMsg := producer.Produce(sampleJsonPayload, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Could not create producer for Personalize", statusMsg)
	assert.Equal(t, "Could not create producer for Personalize", respMsg)
}

func TestProduceWithInvalidData(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_personalize.NewMockPersonalizeClientV2(ctrl)
	producer := &PersonalizeProducerV2{client: mockClient}

	// Invalid Json
	sampleJsonPayload := []byte("invalid json")
	statusCode, statusMsg, respMsg := producer.Produce(sampleJsonPayload, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.NotEmpty(t, statusMsg)
	assert.Equal(t, "Could not unmarshal jsonData according to PutEvents input structure", respMsg)
	choices := []string{"PutEvents", "PutItems", "PutUsers"}

	for _, choice := range choices {
		// Invalid Event payload
		sampleJsonPayload, _ = jsonrs.Marshal(map[string]string{
			"choice":  choice,
			"payload": "invalid json",
		})
		statusCode, statusMsg, respMsg = producer.Produce(sampleJsonPayload, map[string]string{})
		assert.Equal(t, 400, statusCode)
		assert.NotEmpty(t, statusMsg)
		assert.Equal(t, fmt.Sprintf("Could not unmarshal jsonData according to %s input structure", choice), respMsg)
	}
}

func TestProduceWithPutEventsWithServiceResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_personalize.NewMockPersonalizeClientV2(ctrl)
	producer := &PersonalizeProducerV2{client: mockClient}
	mockLogger := mock_logger.NewMockLogger(ctrl)
	pkgLogger = mockLogger
	sampleJsonPayload, _ := jsonrs.Marshal(map[string]interface{}{
		"choice": "PutEvents",
		"payload": personalizeevents.PutEventsInput{
			EventList: []types.Event{{
				EventId:   aws.String("eventId"),
				EventType: aws.String("eventType"),
				ItemId:    aws.String("itemId"),
				SentAt:    aws.Time(time.Now()),
			}},
			SessionId:  aws.String("sessionId"),
			TrackingId: aws.String("trackingId"),
			UserId:     aws.String("userId"),
		},
	})

	var putEventsInput personalizeevents.PutEventsInput
	parsedJSON := gjson.ParseBytes(sampleJsonPayload)
	eventPayload := []byte(parsedJSON.Get("payload").String())
	_ = jsonrs.Unmarshal(eventPayload, &putEventsInput)

	// PutEvents with event choice
	// Time struct is changing during marshalling and unmarshalling so we can't directly
	// define personalizeevents.PutEventsInput variable and use in expect
	mockClient.EXPECT().PutEvents(gomock.Any(), &putEventsInput, gomock.Any()).Return(&personalizeevents.PutEventsOutput{}, nil)
	statusCode, statusMsg, respMsg := producer.Produce(sampleJsonPayload, map[string]string{})
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, "Success", statusMsg)
	assert.NotEmpty(t, respMsg)

	// Put event without event choice in the payload so pull payload will be sent to PutEvents
	mockClient.EXPECT().PutEvents(gomock.Any(), &putEventsInput, gomock.Any()).Return(&personalizeevents.PutEventsOutput{}, nil)
	statusCode, statusMsg, respMsg = producer.Produce(eventPayload, map[string]string{})
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, "Success", statusMsg)
	assert.NotEmpty(t, respMsg)

	// Return service error
	errorCode := "someError"
	mockClient.EXPECT().PutEvents(gomock.Any(), &putEventsInput, gomock.Any()).Return(nil, &smithy.GenericAPIError{
		Code:    errorCode,
		Message: errorCode,
		Fault:   smithy.FaultClient,
	})
	mockLogger.EXPECT().Errorf(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
	statusCode, statusMsg, respMsg = producer.Produce(sampleJsonPayload, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, errorCode, statusMsg)
	assert.NotEmpty(t, respMsg)
}

func TestProduceWithPutUsersWithServiceResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_personalize.NewMockPersonalizeClientV2(ctrl)
	producer := &PersonalizeProducerV2{client: mockClient}
	mockLogger := mock_logger.NewMockLogger(ctrl)
	pkgLogger = mockLogger
	sampleJsonPayload, _ := jsonrs.Marshal(map[string]interface{}{
		"choice": "PutUsers",
		"payload": personalizeevents.PutUsersInput{
			DatasetArn: aws.String("datasetArn"),
			Users:      []types.User{{UserId: aws.String("userId")}},
		},
	})

	var putUsersInput personalizeevents.PutUsersInput
	parsedJSON := gjson.ParseBytes(sampleJsonPayload)
	eventPayload := []byte(parsedJSON.Get("payload").String())
	_ = jsonrs.Unmarshal(eventPayload, &putUsersInput)

	// Time struct is changing during marshalling and unmarshalling so we can't directly
	// define personalizeevents.PutUsersInput variable and use in expect
	mockClient.EXPECT().PutUsers(gomock.Any(), &putUsersInput, gomock.Any()).Return(&personalizeevents.PutUsersOutput{}, nil)
	statusCode, statusMsg, respMsg := producer.Produce(sampleJsonPayload, map[string]string{})
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, "Success", statusMsg)
	assert.NotEmpty(t, respMsg)

	errorCode := "someError"
	mockClient.EXPECT().PutUsers(gomock.Any(), &putUsersInput, gomock.Any()).Return(nil, &smithy.GenericAPIError{
		Code:    errorCode,
		Message: errorCode,
		Fault:   smithy.FaultClient,
	})
	mockLogger.EXPECT().Errorf(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
	statusCode, statusMsg, respMsg = producer.Produce(sampleJsonPayload, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, errorCode, statusMsg)
	assert.NotEmpty(t, respMsg)
}

func TestProduceWithPutItemsWithServiceResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_personalize.NewMockPersonalizeClientV2(ctrl)
	producer := &PersonalizeProducerV2{client: mockClient}
	mockLogger := mock_logger.NewMockLogger(ctrl)
	pkgLogger = mockLogger
	sampleJsonPayload, _ := jsonrs.Marshal(map[string]interface{}{
		"choice": "PutItems",
		"payload": personalizeevents.PutItemsInput{
			DatasetArn: aws.String("datasetArn"),
			Items:      []types.Item{{ItemId: aws.String("itemId")}},
		},
	})

	var putItemsInput personalizeevents.PutItemsInput
	parsedJSON := gjson.ParseBytes(sampleJsonPayload)
	eventPayload := []byte(parsedJSON.Get("payload").String())
	_ = jsonrs.Unmarshal(eventPayload, &putItemsInput)

	// Time struct is changing during marshalling and unmarshalling so we can't directly
	// define personalizeevents.PutItemsInput variable and use in expect
	mockClient.EXPECT().PutItems(gomock.Any(), &putItemsInput, gomock.Any()).Return(&personalizeevents.PutItemsOutput{}, nil)
	statusCode, statusMsg, respMsg := producer.Produce(sampleJsonPayload, map[string]string{})
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, "Success", statusMsg)
	assert.NotEmpty(t, respMsg)

	errorCode := "someError"
	mockClient.EXPECT().PutItems(gomock.Any(), &putItemsInput, gomock.Any()).Return(nil, &smithy.GenericAPIError{
		Code:    errorCode,
		Message: errorCode,
		Fault:   smithy.FaultClient,
	})
	mockLogger.EXPECT().Errorf(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
	statusCode, statusMsg, respMsg = producer.Produce(sampleJsonPayload, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, errorCode, statusMsg)
	assert.NotEmpty(t, respMsg)
}
