package testhelper

import (
	"context"
	"github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"time"
)

type TransformerResource struct {
	Url  string
	Port string
}

type MinioResource struct {
	MinioEndpoint   string
	MinioBucketName string
	Port            string
}

type EventsCountMap map[string]int

type WareHouseDestinationTest struct {
	Client                   *client.Client
	EventsCountMap           EventsCountMap
	WriteKey                 string
	UserId                   string
	Schema                   string
	BQContext                context.Context
	Tables                   []string
	PrimaryKeys              []string
	MessageId                string
	VerifyingTablesFrequency time.Duration
}

func (w *WareHouseDestinationTest) MsgId() string {
	if w.MessageId == "" {
		return uuid.Must(uuid.NewV4()).String()
	}
	return w.MessageId
}
