package personalize

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/personalizeevents"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/tidwall/gjson"
)

type Config struct {
	Region      string
	AccessKeyID string
	AccessKey   string
}

var pkgLogger logger.LoggerI

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("personalize")

}

func NewProducer(destinationConfig interface{}) (personalizeevents.PersonalizeEvents, error) {
	config := Config{}

	jsonConfig, err := json.Marshal(destinationConfig) // produces json
	if err != nil {
		return personalizeevents.PersonalizeEvents{}, fmt.Errorf("[Personalize] Error while marshalling destination config :: %w", err)
	}
	err = json.Unmarshal(jsonConfig, &config)
	if err != nil {
		return personalizeevents.PersonalizeEvents{}, fmt.Errorf("[Personalize] Error while unmarshalling destination config :: %w", err)
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
	var client *personalizeevents.PersonalizeEvents = personalizeevents.New(s)
	return *client, nil
}

func Produce(jsonData json.RawMessage, producer interface{}, destConfig interface{}) (int, string, string) {

	// get producer
	client, ok := producer.(personalizeevents.PersonalizeEvents)
	if (!ok || client == personalizeevents.PersonalizeEvents{}) {
		// return 400 if producer is invalid
		return 400, "Could not create producer for Personalize", "Could not create producer for Personalize"
	}
	input := personalizeevents.PutEventsInput{}
	dataJson := gjson.ParseBytes(jsonData)

	if dataJson.Get("userId").Value() != nil {
		userId := dataJson.Get("userId").Value().(string)
		input.SetUserId(userId)
	} else {
		respStatus := "Failure"
		responseMessage := "[Personalize] error :: message from payload not found"
		return 400, respStatus, responseMessage
	}

	if dataJson.Get("sessionId").Value() != nil {
		timeStamp := dataJson.Get("sessionId").Value().(string)
		input.SetSessionId(timeStamp)
	} else {
		respStatus := "Failure"
		responseMessage := "[Personalize] error :: message from payload not found"
		return 400, respStatus, responseMessage
	}

	if dataJson.Get("trackingId").Value() != nil {
		trackingId := dataJson.Get("trackingId").Value().(string)
		input.SetTrackingId(trackingId)
	} else {
		respStatus := "Failure"
		responseMessage := "[Personalize] error :: message from payload not found"
		return 400, respStatus, responseMessage
	}

	event := personalizeevents.Event{}

	if dataJson.Get("eventList").Value() != nil {
		eventList := dataJson.Get("eventList").Value().(string)
		eventJson := gjson.ParseBytes([]byte(eventList))

		if eventJson.Get("eventId").Value() != nil {
			eventId := dataJson.Get("eventId").Value().(string)
			event.SetEventId(eventId)
		}

		if eventJson.Get("eventType").Value() != nil {
			eventType := dataJson.Get("eventType").Value().(string)
			event.SetEventType(eventType)

		} else {
			respStatus := "Failure"
			responseMessage := "[Personalize] error :: message from payload not found"
			return 400, respStatus, responseMessage
		}

		if eventJson.Get("eventValue").Value() != nil {
			eventValue := dataJson.Get("eventValue").Value().(float64)
			event.SetEventValue(eventValue)

		}

		event.SetSentAt(time.Now())

		if eventJson.Get("properties").Value() != nil {
			property := dataJson.Get("eventType").Value().(string)
			bytes := []byte(property)
			properties := aws.JSONValue{}
			json.Unmarshal(bytes, &properties)
			event.SetProperties(properties)
		}

		if eventJson.Get("itemId").Value() != nil {
			itemId := dataJson.Get("itemId").Value().(string)
			event.SetItemId(itemId)

		} else {
			respStatus := "Failure"
			responseMessage := "[Personalize] error :: message from payload not found"
			return 400, respStatus, responseMessage
		}

		if eventJson.Get("recommendationId").Value() != nil {
			recommendationId := dataJson.Get("recommendationId").Value().(string)
			event.SetRecommendationId(recommendationId)

		}

	} else {
		respStatus := "Failure"
		responseMessage := "[Personalize] error :: message from payload not found"
		return 400, respStatus, responseMessage
	}

	eventList := make([]*personalizeevents.Event, 0)
	eventList = append(eventList, &event)

	_, err := client.PutEvents(&input)

	if err != nil {
		pkgLogger.Errorf("Personalize Error while sending event :: %w", err)

		// set default status code as 500
		statusCode := 500

		// fetching status code from response
		if reqErr, ok := err.(awserr.RequestFailure); ok {
			statusCode = reqErr.StatusCode()
		}

		return statusCode, err.Error(), err.Error()
	}

	return 200, "success", ""

}
