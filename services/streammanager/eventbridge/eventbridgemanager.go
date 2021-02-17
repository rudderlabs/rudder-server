package eventbridge

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/eventbridge"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

// Config is the config that is required to send data to EventBridge
type Config struct {
	Region      string
	AccessKeyID string
	AccessKey   string
}

var pkgLogger logger.LoggerI

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("eventbridge")

}

// NewProducer creates a producer based on destination config
func NewProducer(destinationConfig interface{}) (eventbridge.EventBridge, error) {
	config := Config{}

	jsonConfig, err := json.Marshal(destinationConfig)
	if err != nil {
		return eventbridge.EventBridge{}, fmt.Errorf("[EventBridge] Error while marshalling destination config :: %w", err)
	}
	err = json.Unmarshal(jsonConfig, &config)
	if err != nil {
		return eventbridge.EventBridge{}, fmt.Errorf("[EventBridge] Error while unmarshalling destination config :: %w", err)
	}

	var s *session.Session
	if config.AccessKeyID == "" || config.AccessKey == "" {
		s = session.Must(session.NewSession(&aws.Config{
			Region: aws.String(config.Region),
		}))
	} else {
		s = session.Must(session.NewSession(&aws.Config{
			Region:      aws.String(config.Region),
			Credentials: credentials.NewStaticCredentials(config.AccessKeyID, config.AccessKey, "")}))
	}
	var ebc *eventbridge.EventBridge = eventbridge.New(s)
	return *ebc, nil
}

// Produce creates a producer and send data to EventBridge.
func Produce(jsonData json.RawMessage, producer interface{}, destConfig interface{}) (int, string, string) {

	// get producer
	ebc, ok := producer.(eventbridge.EventBridge)
	if (!ok || ebc == eventbridge.EventBridge{}) {
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

	// send request to event bridge
	putEventsOutput, err := ebc.PutEvents(&requestInput)
	if err != nil {
		pkgLogger.Errorf("[EventBridge] Error while sending event :: %v", err)

		// set default status code as 500
		statusCode := 500

		// fetching status code from response
		if reqErr, ok := err.(awserr.RequestFailure); ok {
			statusCode = reqErr.StatusCode()
		}

		return statusCode, err.Error(), err.Error()
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
