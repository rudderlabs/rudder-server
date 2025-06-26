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
	EventType         *string
	SentAt            *time.Time
	EventId           *string
	EventValue        *float32
	Impression        []string
	ItemId            *string
	MetricAttribution *MetricAttribution
	Properties        *json.RawMessage
	RecommendationId  *string
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
		Properties:       stringifyJsonRaw(e.Properties),
		Impression:       e.Impression,
		RecommendationId: e.RecommendationId,
		EventValue:       e.EventValue,
		MetricAttribution: &types.MetricAttribution{
			EventAttributionSource: e.MetricAttribution.EventAttributionSource,
		},
	}
}

func (p *PersonalizeEvent) ToPutEventsInput() *personalizeevents.PutEventsInput {
	return &personalizeevents.PutEventsInput{
		EventList:  lo.Map(p.EventList, func(e Event, _ int) types.Event { return e.ToAWSEvent() }),
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
		Properties: stringifyJsonRaw(u.Properties),
	}
}

func (u *Users) ToPutUsersInput() *personalizeevents.PutUsersInput {
	return &personalizeevents.PutUsersInput{
		Users:      lo.Map(u.Users, func(u User, _ int) types.User { return u.ToAWSUser() }),
		DatasetArn: u.DatasetArn,
	}
}

type Items struct {
	DatasetArn *string
	Items      []Item
}

type Item struct {
	ItemId     *string
	Properties *json.RawMessage
}

func (i *Item) ToAWSItem() types.Item {
	return types.Item{
		ItemId:     i.ItemId,
		Properties: stringifyJsonRaw(i.Properties),
	}
}

func (i *Items) ToPutItemsInput() *personalizeevents.PutItemsInput {
	return &personalizeevents.PutItemsInput{
		DatasetArn: i.DatasetArn,
		Items:      lo.Map(i.Items, func(item Item, _ int) types.Item { return item.ToAWSItem() }),
	}
}
