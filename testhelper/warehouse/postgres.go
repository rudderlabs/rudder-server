package warehouse_test

import (
	"database/sql"
	"fmt"
	"github.com/ory/dockertest"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"log"
)

type PostgresTest struct {
	Resource    *dockertest.Resource
	Credentials *postgres.CredentialsT
	DB          *sql.DB
	EventsMap   EventsCountMap
	WriteKey    string
}

// SetWHPostgresDestination setup warehouse postgres destination
func SetWHPostgresDestination(pool *dockertest.Pool) (cleanup func()) {
	Test.PGTest = &PostgresTest{
		WriteKey: randString(27),
		Credentials: &postgres.CredentialsT{
			DBName:   "rudderdb",
			Password: "rudder-password",
			User:     "rudder",
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
	pgTest := Test.PGTest
	credentials := pgTest.Credentials
	cleanup = func() {}

	var err error
	if pgTest.Resource, err = pool.Run("postgres", "11-alpine", []string{
		fmt.Sprintf("POSTGRES_DB=%s", credentials.DBName),
		fmt.Sprintf("POSTGRES_PASSWORD=%s", credentials.Password),
		fmt.Sprintf("POSTGRES_USER=%s", credentials.User),
	}); err != nil {
		panic(fmt.Errorf("Could not create WareHouse Postgres: %v\n", err))
	}

	// Getting at which port the postgres container is running
	credentials.Port = pgTest.Resource.GetPort("5432/tcp")

	purgeResources := func() {
		if pgTest.Resource != nil {
			log.Printf("Purging warehouse postgres resource: %s \n", err)
			if err := pool.Purge(pgTest.Resource); err != nil {
				log.Printf("Could not purge warehouse postgres resource: %s \n", err)
			}
		}
	}

	if err = pool.Retry(func() error {
		var err error
		pgTest.DB, err = postgres.Connect(*credentials)
		if err != nil {
			return err
		}
		return pgTest.DB.Ping()
	}); err != nil {
		defer purgeResources()
		panic(fmt.Errorf("Could not connect to warehouse postgres with error: %w\n", err))
	}
	cleanup = purgeResources
	return
}
