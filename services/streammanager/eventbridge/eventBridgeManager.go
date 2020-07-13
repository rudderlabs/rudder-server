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
	Region       string
	AccessKeyID  string
	AccessKey    string
	EventBusName string
	ResourceID   []map[string]interface{}
	DetailType   string
}

// NewProducer creates a producer based on destination config
func NewProducer(destinationConfig interface{}) (eventbridge.EventBridge, error) {
	config := Config{}

	jsonConfig, err := json.Marshal(destinationConfig)
	err = json.Unmarshal(jsonConfig, &config)
	if err != nil {
		return eventbridge.EventBridge{}, fmt.Errorf("EventBridge: Error while unmarshalling destination config : %v", err.Error())
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
	return *ebc, err
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
	json.Unmarshal(jsonData, &putRequestEntry)

	// create eventbridge request
	putRequestEntryList := []*eventbridge.PutEventsRequestEntry{&putRequestEntry}
	requestInput := eventbridge.PutEventsInput{}
	requestInput.SetEntries(putRequestEntryList)

	// send request to event bridge
	putEventsOutput, err := ebc.PutEvents(&requestInput)
	if err != nil {
		logger.Errorf("Error while sending event to eventbridge :: %v", err.Error())

		// set default status code as 500
		statusCode := 500

		// fetching status code from response
		if reqErr, ok := err.(awserr.RequestFailure); ok {
			statusCode = reqErr.StatusCode()
		}

		return statusCode, err.Error(), err.Error()
	}

	message := "Successfully sent event to eventbridge"
	if len(putEventsOutput.Entries) > 0 {
		message += fmt.Sprintf(",with eventID: %v", *putEventsOutput.Entries[0].EventId)
	}

	return 200, "Success", message
}
