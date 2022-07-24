package testhelper

import (
	"database/sql"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
)

type JobsDBResource struct {
	Credentials *postgres.CredentialsT
	DB          *sql.DB
}

type EventsCountMap map[string]int

type WareHouseTest struct {
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

type ISetup interface {
	TestConnection()
}
