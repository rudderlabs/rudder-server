//go:generate mockgen -destination=../../../mocks/services/streammanager/eventbridge/mock_eventbridge.go -package mock_eventbridge github.com/rudderlabs/rudder-server/services/streammanager/eventbridge EventBridgeClient

package eventbridge

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/service/eventbridge"

	"github.com/rudderlabs/rudder-go-kit/awsutil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/utils/awsutils"
)

var pkgLogger logger.Logger

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child(strings.ToLower(eventbridge.ServiceName))
}

type EventBridgeProducer struct {
	client EventBridgeClient
}

type EventBridgeClient interface {
	PutEvents(input *eventbridge.PutEventsInput) (*eventbridge.PutEventsOutput, error)
}

// NewProducer creates a producer based on destination config
func NewProducer(destination *backendconfig.DestinationT, o common.Opts) (*EventBridgeProducer, error) {
	sessionConfig, err := awsutils.NewSessionConfigForDestination(destination, o.Timeout, eventbridge.ServiceName)
	if err != nil {
		return nil, err
	}
	awsSession, err := awsutil.CreateSession(sessionConfig)
	if err != nil {
		return nil, err
	}
	return &EventBridgeProducer{client: eventbridge.New(awsSession)}, nil
}

// Produce creates a producer and send data to EventBridge.
func (producer *EventBridgeProducer) Produce(jsonData json.RawMessage, _ interface{}) (int, string, string) {
	// get producer
	client := producer.client
	if client == nil {
		// return 400 if producer is invalid
		return 400, "Could not create producer for EventBridge", "Could not create producer for EventBridge"
	}
	// create eventbridge event
	putRequestEntry := eventbridge.PutEventsRequestEntry{}
	err := json.Unmarshal(jsonData, &putRequestEntry)
	if err != nil {
		return 400, "[EventBridge] Failed to create eventbridge event", err.Error()
	}

	// create eventbridge request
	putRequestEntryList := []*eventbridge.PutEventsRequestEntry{&putRequestEntry}
	requestInput := eventbridge.PutEventsInput{}
	requestInput.SetEntries(putRequestEntryList)
	if err = requestInput.Validate(); err != nil {
		return 400, "InvalidInput", err.Error()
	}
	// send request to event bridge
	putEventsOutput, err := client.PutEvents(&requestInput)
	if err != nil {
		statusCode, respStatus, responseMessage := common.ParseAWSError(err)
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

func (*EventBridgeProducer) Close() error {
	// no-op
	return nil
}
