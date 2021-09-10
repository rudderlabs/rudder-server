package personalize

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/personalizeevents"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/tidwall/gjson"
)

type Config struct {
	Region          string
	AccessKeyID     string
	SecretAccessKey string
}

var pkgLogger logger.LoggerI

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("personalize")
}

func NewProducer(destinationConfig interface{}) (personalizeevents.PersonalizeEvents, error) {
	var config Config
	jsonConfig, err := json.Marshal(destinationConfig) // produces json
	if err != nil {
		return personalizeevents.PersonalizeEvents{}, fmt.Errorf("[Personalize] Error while marshalling destination config :: %w", err)
	}
	err = json.Unmarshal(jsonConfig, &config)
	if err != nil {
		return personalizeevents.PersonalizeEvents{}, fmt.Errorf("[Personalize] Error while unmarshalling destination config :: %w", err)
	}
	var s *session.Session
	if config.AccessKeyID == "" || config.SecretAccessKey == "" {
		s = session.Must(session.NewSession(&aws.Config{
			Region: aws.String(config.Region),
		}))
	} else {
		s = session.Must(session.NewSession(&aws.Config{
			Region:      aws.String(config.Region),
			Credentials: credentials.NewStaticCredentials(config.AccessKeyID, config.SecretAccessKey, "")}))
	}
	var client *personalizeevents.PersonalizeEvents = personalizeevents.New(s)
	return *client, nil
}
func Produce(jsonData json.RawMessage, producer interface{}, destConfig interface{}) (statusCode int, respStatus string, responseMessag string) {

	var resEvent *personalizeevents.PutEventsOutput
	var resUser *personalizeevents.PutUsersOutput
	var resItem *personalizeevents.PutItemsOutput
	var err error
	var responseMessage string

	parsedJSON := gjson.ParseBytes(jsonData)
	eventChoice := parsedJSON.Get("choice").String()
	eventPayload := parsedJSON.Get("payload").String()

	client, ok := producer.(personalizeevents.PersonalizeEvents)
	if (!ok || client == personalizeevents.PersonalizeEvents{}) {
		// return 400 if producer is invalid
		return 400, "Could not create producer for Personalize", "Could not create producer for Personalize"
	}

	if len(eventChoice) > 0 {
		if eventChoice == "PutEvents" {
			input := personalizeevents.PutEventsInput{}
			bytes := []byte(eventPayload)
			err = json.Unmarshal(bytes, &input)
			if err != nil {
				return 400, err.Error(), "Could not unmarshal jsonData according to putEvents input structure"
			}
			resEvent, err = client.PutEvents(&input)

		} else if eventChoice == "PutUsers" {
			input := personalizeevents.PutUsersInput{}
			bytes := []byte(eventPayload)
			err = json.Unmarshal(bytes, &input)
			if err != nil {
				return 400, err.Error(), "Could not unmarshal jsonData according to putUsers input structure"
			}
			resUser, err = client.PutUsers(&input)

		} else {
			input := personalizeevents.PutItemsInput{}
			bytes := []byte(eventPayload)
			err = json.Unmarshal(bytes, &input)
			if err != nil {
				return 400, err.Error(), "Could not unmarshal jsonData according to putItems input structure"
			}
			resItem, err = client.PutItems(&input)
		}
	} else {
		input := personalizeevents.PutEventsInput{}
		bytes := []byte(jsonData)
		err = json.Unmarshal(bytes, &input)
		if err != nil {
			return 400, err.Error(), "Could not unmarshal jsonData according to putEvents input structure"
		}
		resEvent, err = client.PutEvents(&input)

	}
	if err != nil {
		pkgLogger.Errorf("Personalize Error while sending event :: %w", err)
		// set default status code as 500
		statusCode := 500
		// fetching status code from response
		if reqErr, ok := err.(awserr.RequestFailure); ok {
			statusCode = reqErr.StatusCode()
		}
		return statusCode, err.Error(), err.Error()
	} else {
		if resEvent != nil {
			responseMessage = fmt.Sprintf("Message delivered with Record information %v", resEvent)
		} else if resUser != nil {
			responseMessage = fmt.Sprintf("Message delivered with Record information %v", resUser)
		} else {
			responseMessage = fmt.Sprintf("Message delivered with Record information %v", resItem)
		}
		respStatus = "Success"
		return 200, respStatus, responseMessage
	}

}
