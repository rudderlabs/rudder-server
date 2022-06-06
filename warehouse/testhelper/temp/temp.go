package temp

import (
	"database/sql"
	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/warehouse"
	azuresynapse "github.com/rudderlabs/rudder-server/warehouse/azure-synapse"
	"github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/datalake"
	"github.com/rudderlabs/rudder-server/warehouse/deltalake"
	"github.com/rudderlabs/rudder-server/warehouse/mssql"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/redshift"
	"github.com/rudderlabs/rudder-server/warehouse/snowflake"
	"log"
)

func Init() {
	config.Load()
	admin.Init()
	stats.Init()
	stats.Setup()
	warehouse.Init()
	warehouse.Init2()
	warehouse.Init3()
	warehouse.Init4()
	warehouse.Init5()
	warehouse.Init6()
	azuresynapse.Init()
	bigquery.Init()
	clickhouse.Init()
	deltalake.Init()
	datalake.Init()
	mssql.Init()
	postgres.Init()
	redshift.Init()
	snowflake.Init()
}

func SetUpJobsDB() (jobsDB *JobsDBResource) {
	pgCredentials := &postgres.CredentialsT{
		DBName:   "jobsdb",
		Password: "password",
		User:     "rudder",
		Host:     "localhost",
		SSLMode:  "disable",
		Port:     "54328",
	}
	jobsDB = &JobsDBResource{}
	jobsDB.Credentials = pgCredentials

	var err error
	if jobsDB.DB, err = postgres.Connect(*pgCredentials); err != nil {
		log.Fatalf("could not connect to jobsDb with error: %s", err.Error())
	}
	if err = jobsDB.DB.Ping(); err != nil {
		log.Fatalf("could not connect to jobsDb while pinging with error: %s", err.Error())
	}
	return
}

type JobsDBResource struct {
	Credentials *postgres.CredentialsT
	DB          *sql.DB
}
