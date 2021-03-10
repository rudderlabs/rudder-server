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
	fmt.Printf("HERE IN NEW PRODUCER")
	jsonConfig, err := json.Marshal(destinationConfig) // produces json
	if err != nil {
		return personalizeevents.PersonalizeEvents{}, fmt.Errorf("[Personalize] Error while marshalling destination config :: %w", err)
	}
	err = json.Unmarshal(jsonConfig, &config)
	if err != nil {
		return personalizeevents.PersonalizeEvents{}, fmt.Errorf("[Personalize] Error while unmarshalling destination config :: %w", err)
	}
	fmt.Printf("*************************")
	fmt.Printf(config.AccessKeyID)
	fmt.Printf("#########################")
	fmt.Printf(config.SecretAccessKey)
	var s *session.Session
	if config.AccessKeyID == "" || config.SecretAccessKey == "" {
		fmt.Printf("HERE IN IF PORTION")
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
func Produce(jsonData json.RawMessage, producer interface{}, destConfig interface{}) (int, string, string) {
	fmt.Printf("****** HERE IN SERVER **********")
	// fmt.Printf(string(jsonData))
	// get producer
	client, ok := producer.(personalizeevents.PersonalizeEvents)
	if (!ok || client == personalizeevents.PersonalizeEvents{}) {
		// return 400 if producer is invalid
		return 400, "Could not create producer for Personalize", "Could not create producer for Personalize"
	}
	input := personalizeevents.PutEventsInput{}
	dataJson := gjson.ParseBytes(jsonData)
	if dataJson.Get("userId").Value() != nil {
		fmt.Printf("*****printing UserId******")
		userId := dataJson.Get("userId").Value().(string)
		fmt.Printf(userId)
		input.SetUserId(userId)
	} else {
		respStatus := "Failure"
		responseMessage := "[Personalize] error :: userId from payload not found"
		return 400, respStatus, responseMessage
	}
	if dataJson.Get("sessionId").Value() != nil {
		fmt.Printf("*****Printing sessionId******")
		timeStamp := dataJson.Get("sessionId").Value().(string)
		fmt.Printf(timeStamp)
		input.SetSessionId(timeStamp)
	} else {
		respStatus := "Failure"
		responseMessage := "[Personalize] error :: sessionId from payload not found"
		return 400, respStatus, responseMessage
	}
	if dataJson.Get("trackingId").Value() != nil {
		fmt.Printf("*****Printing trackingId******")
		trackingId := dataJson.Get("trackingId").Value().(string)
		input.SetTrackingId(trackingId)
	} else {
		respStatus := "Failure"
		responseMessage := "[Personalize] error :: trackingId from payload not found"
		return 400, respStatus, responseMessage
	}
	event := personalizeevents.Event{}
	if dataJson.Get("eventId").Value() != nil {
		fmt.Printf("*****Printing eventId******")
		eventId := dataJson.Get("eventId").Value().(string)
		event.SetEventId(eventId)
	}
	if dataJson.Get("eventType").Value() != nil {
		fmt.Printf("*****Printing eventType******")
		eventType := dataJson.Get("eventType").Value().(string)
		event.SetEventType(eventType)
	} else {
		respStatus := "Failure"
		responseMessage := "[Personalize] error :: eventType from payload not found"
		return 400, respStatus, responseMessage
	}
	if dataJson.Get("eventValue").Value() != nil {
		fmt.Printf("*****Printing eventValue******")
		eventValue := dataJson.Get("eventValue").Value().(float64)
		event.SetEventValue(eventValue)
	}
	event.SetSentAt(time.Now())
	if dataJson.Get("properties").Value() != nil {
		fmt.Printf("*****Printing Property******")
		property := dataJson.Get("eventType").Value().(string)
		bytes := []byte(property)
		properties := aws.JSONValue{}
		json.Unmarshal(bytes, &properties)
		event.SetProperties(properties)
	}
	if dataJson.Get("itemId").Value() != nil {
		fmt.Printf("*****Printing itemId******")
		itemId := dataJson.Get("itemId").Value().(string)
		event.SetItemId(itemId)
	}
	if dataJson.Get("recommendationId").Value() != nil {
		fmt.Printf("*****Printing recommId******")
		recommendationId := dataJson.Get("recommendationId").Value().(string)
		event.SetRecommendationId(recommendationId)
	}
	eventList := make([]*personalizeevents.Event, 0)
	eventList = append(eventList, &event)
	input.SetEventList(eventList)
	_, err := client.PutEvents(&input)
	if err != nil {
		fmt.Printf("*****put events error*****")
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
