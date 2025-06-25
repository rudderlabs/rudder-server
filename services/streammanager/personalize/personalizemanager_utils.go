package personalize

import (
	"encoding/json"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/personalizeevents"
	"github.com/aws/aws-sdk-go-v2/service/personalizeevents/types"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

var pkgLogger logger.Logger

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("personalize")
}

type PersonalizeEvent struct {
	EventList  []Event `json:"eventList"`
	SessionId  string  `json:"sessionId"`
	TrackingId string  `json:"trackingId"`
	UserId     string  `json:"userId"`
}

type Event struct {
	EventId           string             `json:"eventId"`
	EventType         string             `json:"eventType"`
	ItemId            string             `json:"itemId"`
	SentAt            time.Time          `json:"sentAt"`
	Properties        json.RawMessage    `json:"properties"`
	Impression        []string           `json:"impression"`
	RecommendationId  string             `json:"recommendationId"`
	EventValue        float32            `json:"eventValue"`
	MetricAttribution *MetricAttribution `json:"metricAttribution"`
}

type MetricAttribution struct {
	EventAttributionSource string `json:"eventAttributionSource"`
}

func (p *PersonalizeEvent) ToPutEventsInput() *personalizeevents.PutEventsInput {
	eventList := make([]types.Event, len(p.EventList))
	for i, event := range p.EventList {
		eventList[i] = types.Event{
			EventId:          aws.String(event.EventId),
			EventType:        aws.String(event.EventType),
			ItemId:           aws.String(event.ItemId),
			SentAt:           aws.Time(event.SentAt),
			Properties:       aws.String(string(event.Properties)),
			Impression:       event.Impression,
			RecommendationId: aws.String(event.RecommendationId),
			EventValue:       aws.Float32(event.EventValue),
			MetricAttribution: &types.MetricAttribution{
				EventAttributionSource: aws.String(event.MetricAttribution.EventAttributionSource),
			},
		}
	}
	return &personalizeevents.PutEventsInput{
		EventList:  eventList,
		SessionId:  aws.String(p.SessionId),
		TrackingId: aws.String(p.TrackingId),
		UserId:     aws.String(p.UserId),
	}
}

type Users struct {
	Users      []User `json:"users"`
	DatasetArn string `json:"datasetArn"`
}

type User struct {
	UserId     string `json:"userId"`
	Properties string `json:"properties"`
}

func (u *Users) ToPutUsersInput() *personalizeevents.PutUsersInput {
	users := make([]types.User, len(u.Users))
	for i, user := range u.Users {
		users[i] = types.User{
			UserId:     aws.String(user.UserId),
			Properties: aws.String(user.Properties),
		}
	}
	return &personalizeevents.PutUsersInput{
		Users:      users,
		DatasetArn: aws.String(u.DatasetArn),
	}
}

type Items struct {
	Items      []Item `json:"items"`
	DatasetArn string `json:"datasetArn"`
}

type Item struct {
	ItemId     string `json:"itemId"`
	Properties string `json:"properties"`
}

func (i *Items) ToPutItemsInput() *personalizeevents.PutItemsInput {
	items := make([]types.Item, len(i.Items))
	for i, item := range i.Items {
		items[i] = types.Item{
			ItemId:     aws.String(item.ItemId),
			Properties: aws.String(item.Properties),
		}
	}
	return &personalizeevents.PutItemsInput{
		Items:      items,
		DatasetArn: aws.String(i.DatasetArn),
	}
}
