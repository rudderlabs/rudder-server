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
	EventList  []Event
	SessionId  *string
	TrackingId *string
	UserId     *string
}

type Event struct {
	EventId           *string
	EventType         *string
	ItemId            *string
	SentAt            *time.Time
	Properties        *json.RawMessage
	Impression        []string
	RecommendationId  *string
	EventValue        *float32
	MetricAttribution *MetricAttribution
}

type MetricAttribution struct {
	EventAttributionSource *string
}

func (e *Event) ToAWSEvent() types.Event {
	return types.Event{
		EventId:          e.EventId,
		EventType:        e.EventType,
		ItemId:           e.ItemId,
		SentAt:           e.SentAt,
		Properties:       aws.String(string(*e.Properties)),
		Impression:       e.Impression,
		RecommendationId: e.RecommendationId,
		EventValue:       e.EventValue,
		MetricAttribution: &types.MetricAttribution{
			EventAttributionSource: e.MetricAttribution.EventAttributionSource,
		},
	}
}

func (p *PersonalizeEvent) ToPutEventsInput() *personalizeevents.PutEventsInput {
	eventList := make([]types.Event, len(p.EventList))
	for i, event := range p.EventList {
		eventList[i] = event.ToAWSEvent()
	}
	return &personalizeevents.PutEventsInput{
		EventList:  eventList,
		SessionId:  p.SessionId,
		TrackingId: p.TrackingId,
		UserId:     p.UserId,
	}
}

type Users struct {
	Users      []User
	DatasetArn *string
}

type User struct {
	UserId     *string
	Properties *json.RawMessage
}

func (u *User) ToAWSUser() types.User {
	return types.User{
		UserId:     u.UserId,
		Properties: aws.String(string(*u.Properties)),
	}
}

func (u *Users) ToPutUsersInput() *personalizeevents.PutUsersInput {
	users := make([]types.User, len(u.Users))
	for i, user := range u.Users {
		users[i] = user.ToAWSUser()
	}
	return &personalizeevents.PutUsersInput{
		Users:      users,
		DatasetArn: u.DatasetArn,
	}
}

type Items struct {
	Items      []Item
	DatasetArn *string
}

type Item struct {
	ItemId     *string
	Properties *json.RawMessage
}

func (i *Item) ToAWSItem() types.Item {
	return types.Item{
		ItemId:     i.ItemId,
		Properties: aws.String(string(*i.Properties)),
	}
}

func (i *Items) ToPutItemsInput() *personalizeevents.PutItemsInput {
	items := make([]types.Item, len(i.Items))
	for i, item := range i.Items {
		items[i] = item.ToAWSItem()
	}
	return &personalizeevents.PutItemsInput{
		Items:      items,
		DatasetArn: i.DatasetArn,
	}
}
