package testhelper

import (
	bq "cloud.google.com/go/bigquery"
	"context"
	"database/sql"
	"github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/deltalake/databricks"
	"github.com/rudderlabs/rudder-server/warehouse/mssql"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"log"
	"time"
)

type ClickHouseTest struct {
	WriteKey           string
	Credentials        *clickhouse.CredentialsT
	DB                 *sql.DB
	EventsMap          EventsCountMap
	TableTestQueryFreq time.Duration
}

type ClickHouseClusterResource struct {
	Name        string
	HostName    string
	IPAddress   string
	Credentials *clickhouse.CredentialsT
	Port        string
	DB          *sql.DB
}

type ClickHouseClusterResources []*ClickHouseClusterResource

type ClickHouseClusterTest struct {
	Resources          ClickHouseClusterResources
	EventsMap          EventsCountMap
	WriteKey           string
	TableTestQueryFreq time.Duration
}

type PostgresTest struct {
	Credentials        *postgres.CredentialsT
	DB                 *sql.DB
	EventsMap          EventsCountMap
	WriteKey           string
	TableTestQueryFreq time.Duration
}

type MSSQLTest struct {
	Credentials        *mssql.CredentialsT
	DB                 *sql.DB
	EventsMap          EventsCountMap
	WriteKey           string
	TableTestQueryFreq time.Duration
}

type JobsDBResource struct {
	Credentials *postgres.CredentialsT
	DB          *sql.DB
}

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
	Client             *client.Client
	EventsCountMap     EventsCountMap
	WriteKey           string
	UserId             string
	Schema             string
	BQContext          context.Context
	Tables             []string
	PrimaryKeys        []string
	MessageId          string
	TableTestQueryFreq time.Duration
}

type BiqQueryTest struct {
	Credentials        *BigQueryCredentials
	DB                 *bq.Client
	Context            context.Context
	EventsMap          EventsCountMap
	WriteKey           string
	Tables             []string
	PrimaryKeys        []string
	TableTestQueryFreq time.Duration
}

type BigQueryCredentials struct {
	ProjectID          string            `json:"projectID"`
	Credentials        map[string]string `json:"credentials"`
	Location           string            `json:"location"`
	Bucket             string            `json:"bucketName"`
	CredentialsEscaped string
}

type SnowflakeCredentials struct {
	Account     string `json:"account"`
	Warehouse   string `json:"warehouse"`
	Database    string `json:"database"`
	User        string `json:"user"`
	Password    string `json:"password"`
	BucketName  string `json:"bucketName"`
	AccessKeyID string `json:"accessKeyID"`
	AccessKey   string `json:"accessKey"`
}

type SnowflakeTest struct {
	WriteKey           string
	Credentials        *SnowflakeCredentials
	DB                 *sql.DB
	EventsMap          EventsCountMap
	TableTestQueryFreq time.Duration
}

type RedshiftCredentials struct {
	Host        string `json:"host"`
	Port        string `json:"port"`
	Database    string `json:"database"`
	User        string `json:"user"`
	Password    string `json:"password"`
	BucketName  string `json:"bucketName"`
	AccessKeyID string `json:"accessKeyID"`
	AccessKey   string `json:"accessKey"`
}

type RedshiftTest struct {
	WriteKey           string
	Credentials        *RedshiftCredentials
	DB                 *sql.DB
	EventsMap          EventsCountMap
	TableTestQueryFreq time.Duration
}

type DatabricksCredentials struct {
	Host          string `json:"host"`
	Port          string `json:"port"`
	Path          string `json:"path"`
	Token         string `json:"token"`
	AccountName   string `json:"accountName"`
	AccountKey    string `json:"accountKey"`
	ContainerName string `json:"containerName"`
}

type DatabricksTest struct {
	WriteKey           string
	Credentials        *DatabricksCredentials
	DB                 *databricks.DBHandleT
	EventsMap          EventsCountMap
	TableTestQueryFreq time.Duration
}

func (w *WareHouseDestinationTest) MsgId() string {
	if w.MessageId == "" {
		return uuid.Must(uuid.NewV4()).String()
	}
	return w.MessageId
}

func (resources *ClickHouseClusterTest) GetResource() *ClickHouseClusterResource {
	if len(resources.Resources) == 0 {
		log.Panic("No such clickhouse cluster resource available.")
	}
	return resources.Resources[0]
}
