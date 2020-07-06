package eventbridge

import (
	"encoding/json"
	"fmt"

	// "fmt"
	// "strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/eventbridge"
	"github.com/rudderlabs/rudder-server/utils/logger"
	// "github.com/rudderlabs/rudder-server/utils/logger"
)

// TODO : add errors
var abortableErrors = []string{}

// Config is the config that is required to send data to EventBridge
// TODO : ResourceID -> List
type Config struct {
	Region       string
	AccessKeyID  string
	AccessKey    string
	EventBusName string
	ResourceID   string
	DetailType   string
}

func init() {
	// TODO
}

// NewProducer creates a producer based on destination config
func NewProducer(destinationConfig interface{}) (eventbridge.EventBridge, error) {
	config := Config{}

	// do we need to marshall and then unmarshall? can we not directly unmarshall
	jsonConfig, err := json.Marshal(destinationConfig)
	err = json.Unmarshal(jsonConfig, &config)

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
	if !ok {
		return 400, "Could not create producer", "Could not create producer for EventBridge"
	}

	// create eventbridge event
	putRequestEntry := eventbridge.PutEventsRequestEntry{}
	json.Unmarshal(jsonData, &putRequestEntry)

	// create eventbridge request
	putRequestEntryList := []*eventbridge.PutEventsRequestEntry{&putRequestEntry}
	requestInput := eventbridge.PutEventsInput{}
	requestInput.SetEntries(putRequestEntryList)

	// send request to event bridge
	// TODO : use putEventsOutput
	_, err := ebc.PutEvents(&requestInput)
	if err != nil {
		logger.Errorf("Error while sending request to eventbridge :: %v", err.Error())
	}

	// TODO : Modify message after handling errors
	message := fmt.Sprintf("Custom message for eventbridge")
	return 200, "Success", message
}
