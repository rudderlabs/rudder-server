package warehouse_test

import (
	"database/sql"
	"fmt"
	"github.com/ory/dockertest"
	"github.com/rudderlabs/rudder-server/warehouse/mssql"
	"log"
)

type MSSQLTest struct {
	Resource    *dockertest.Resource
	Credentials *mssql.CredentialsT
	DB          *sql.DB
	EventsMap   EventsCountMap
	WriteKey    string
}

// SetWHMssqlDestination setup warehouse mssql destination
func SetWHMssqlDestination(pool *dockertest.Pool) (cleanup func()) {
	Test.MSSQLTest = &MSSQLTest{
		WriteKey: randString(27),
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
	}
	mssqlTest := Test.MSSQLTest
	credentials := mssqlTest.Credentials
	cleanup = func() {}

	var err error
	if mssqlTest.Resource, err = pool.Run("mcr.microsoft.com/mssql/server", "2019-CU10-ubuntu-20.04", []string{
		fmt.Sprintf("ACCEPT_EULA=%s", "Y"),
		fmt.Sprintf("SA_PASSWORD=%s", credentials.Password),
		fmt.Sprintf("SA_DB=%s", credentials.DBName),
		fmt.Sprintf("SA_USER=%s", credentials.User),
	}); err != nil {
		panic(fmt.Errorf("Could not create WareHouse Mssql: %v\n", err))
	}

	// Getting at which port the mssql container is running
	credentials.Port = mssqlTest.Resource.GetPort("1433/tcp")

	purgeResources := func() {
		if mssqlTest.Resource != nil {
			if err := pool.Purge(mssqlTest.Resource); err != nil {
				log.Printf("Could not purge warehouse mssql resource: %s \n", err)
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
		panic(fmt.Errorf("Could not connect to warehouse mssql with error: %w\n", err))
	}
	cleanup = purgeResources
	return
}
