package warehouse_test

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/ory/dockertest/v3"

	"github.com/rudderlabs/rudder-server/testhelper/rand"
	"github.com/rudderlabs/rudder-server/warehouse/mssql"
)

type MSSQLTest struct {
	Resource           *dockertest.Resource
	Credentials        *mssql.CredentialsT
	DB                 *sql.DB
	EventsMap          EventsCountMap
	WriteKey           string
	TableTestQueryFreq time.Duration
}

// SetWHMssqlDestination setup warehouse mssql destination
func SetWHMssqlDestination(pool *dockertest.Pool) (cleanup func()) {
	Test.MSSQLTest = &MSSQLTest{
		WriteKey: rand.String(27),
		Credentials: &mssql.CredentialsT{
			DBName:   "master",
			Password: "reallyStrongPwd123",
			User:     "SA",
			Host:     "localhost",
			SSLMode:  "disable",
		},
		EventsMap: EventsCountMap{
			"identifies":    1,
			"users":         1,
			"tracks":        1,
			"product_track": 1,
			"pages":         1,
			"screens":       1,
			"aliases":       1,
			"groups":        1,
			"gateway":       6,
			"batchRT":       8,
		},
		TableTestQueryFreq: 100 * time.Millisecond,
	}
	mssqlTest := Test.MSSQLTest
	credentials := mssqlTest.Credentials
	cleanup = func() {}

	var err error
	if mssqlTest.Resource, err = pool.Run("mcr.microsoft.com/azure-sql-edge", "1.0.5", []string{
		fmt.Sprintf("ACCEPT_EULA=%s", "Y"),
		fmt.Sprintf("SA_PASSWORD=%s", credentials.Password),
		fmt.Sprintf("SA_DB=%s", credentials.DBName),
		fmt.Sprintf("SA_USER=%s", credentials.User),
	}); err != nil {
		panic(fmt.Errorf("could not create WareHouse Mssql: %s", err.Error()))
	}

	// Getting at which port the mssql container is running
	credentials.Port = mssqlTest.Resource.GetPort("1433/tcp")

	purgeResources := func() {
		if mssqlTest.Resource != nil {
			log.Println("Purging warehouse mssql resource")
			if err := pool.Purge(mssqlTest.Resource); err != nil {
				log.Println(fmt.Errorf("could not purge warehouse mssql resource: %s", err.Error()))
			}
		}
	}

	if err = pool.Retry(func() error {
		var err error
		mssqlTest.DB, err = mssql.Connect(*credentials)
		if err != nil {
			return err
		}
		return mssqlTest.DB.Ping()
	}); err != nil {
		defer purgeResources()
		panic(fmt.Errorf("could not connect to warehouse mssql with error: %s", err.Error()))
	}
	cleanup = purgeResources
	return
}
