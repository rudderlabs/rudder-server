package testhelper

import (
	"database/sql"
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/iancoleman/strcase"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"strings"
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

type JobsDBResource struct {
	Credentials *postgres.CredentialsT
	DB          *sql.DB
}

type EventsCountMap map[string]int

type WareHouseDestinationTest struct {
	Client                   *client.Client
	WriteKey                 string
	Schema                   string
	VerifyingTablesFrequency time.Duration
	EventsCountMap           EventsCountMap
	UserId                   string
	Event                    string
	Tables                   []string
	PrimaryKeys              []string
	MessageId                string
}

func (w *WareHouseDestinationTest) MsgId() string {
	if w.MessageId == "" {
		return uuid.Must(uuid.NewV4()).String()
	}
	return w.MessageId
}

func (w *WareHouseDestinationTest) Reset(destType string, randomProduct bool) {
	randomness := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	w.UserId = fmt.Sprintf("userId_%s_%s", strings.ToLower(destType), randomness)

	if randomProduct {
		w.Event = fmt.Sprintf("Product Track %s", randomness)
	} else {
		w.Event = "Product Track"
	}
	w.EventsCountMap[strcase.ToSnake(w.Event)] = 1
	w.Tables = []string{"identifies", "users", "tracks", strcase.ToSnake(w.Event), "pages", "screens", "aliases", "groups"}
}
