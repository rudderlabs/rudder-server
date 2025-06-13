//go:generate mockgen -destination=../../../mocks/services/streammanager/eventbridge_v2/mock_eventbridge_v2.go -package mock_eventbridge_v2 github.com/rudderlabs/rudder-server/services/streammanager/eventbridge EventBridgeClientV2

package eventbridge

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"

	awsutil "github.com/rudderlabs/rudder-go-kit/awsutil_v2"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	common "github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/utils/awsutils"
)

type EventBridgeProducerV2 struct {
	client EventBridgeClientV2
}

type EventBridgeClientV2 interface {
	PutEvents(ctx context.Context, input *eventbridge.PutEventsInput, opts ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error)
}

// NewProducer creates a producer based on destination config
func NewProducerV2(destination *backendconfig.DestinationT, o common.Opts) (common.Producer, error) {
	sessionConfig, err := awsutils.NewSessionConfigForDestinationV2(destination, o.Timeout, "eventbridge")
	if err != nil {
		return nil, err
	}
	sessionConfig.MaxIdleConnsPerHost = config.GetIntVar(64, 1, "Router.EVENTBRIDGE.httpMaxIdleConnsPerHost", "Router.EVENTBRIDGE.noOfWorkers", "Router.noOfWorkers")
	awsConfig, err := awsutil.CreateAWSConfig(context.Background(), sessionConfig)
	if err != nil {
		return nil, err
	}
	return &EventBridgeProducerV2{client: eventbridge.NewFromConfig(awsConfig)}, nil
}

// Produce creates a producer and send data to EventBridge.
func (producer *EventBridgeProducerV2) Produce(jsonData json.RawMessage, _ interface{}) (int, string, string) {
	// get producer
	client := producer.client
	if client == nil {
		// return 400 if producer is invalid
		return 400, "Could not create producer for EventBridge", "Could not create producer for EventBridge"
	}
	// create eventbridge event
	putRequestEntry := types.PutEventsRequestEntry{}
	err := jsonrs.Unmarshal(jsonData, &putRequestEntry)
	if err != nil {
		return 400, "[EventBridge] Failed to create eventbridge event", err.Error()
	}

	// create eventbridge request
	putRequestEntryList := []types.PutEventsRequestEntry{putRequestEntry}
	requestInput := eventbridge.PutEventsInput{
		Entries: putRequestEntryList,
	}

	// send request to event bridge
	putEventsOutput, err := client.PutEvents(context.Background(), &requestInput)
	if err != nil {
		statusCode, respStatus, responseMessage := common.ParseAWSErrorV2(err)
		pkgLogger.Errorf("[EventBridge] error  :: %d : %s : %s", statusCode, respStatus, responseMessage)
		return statusCode, respStatus, responseMessage
	}

	// Since we are sending only one event, Entries should have only one entry
	if len(putEventsOutput.Entries) != 1 {
		return 400, "Failed to send event to eventbridge", "Failed to send event to eventbridge"
	}

	// Considering only the first entry as we sent only one event
	outputEntry := putEventsOutput.Entries[0]

	// if one of the required fields(Detail, DetailType, Source) is missing, the error returned by PutEvents will be nil.
	// In this case, outputEntry will contain the error code and message
	errorCode := outputEntry.ErrorCode
	errorMessage := outputEntry.ErrorMessage
	if errorCode != nil && errorMessage != nil {
		// request has failed if errorCode and errorMessage are not nil
		return 400, *errorCode, *errorMessage
	}

	message := "Successfully sent event to eventbridge"
	if eventID := outputEntry.EventId; eventID != nil {
		message += fmt.Sprintf(",with eventID: %v", *eventID)
	}
	return 200, "Success", message
}

func (*EventBridgeProducerV2) Close() error {
	// no-op
	return nil
}
