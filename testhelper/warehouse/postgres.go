package warehouse_test

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/ory/dockertest/v3"

	"github.com/rudderlabs/rudder-server/testhelper/rand"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
)

type PostgresTest struct {
	Resource           *dockertest.Resource
	Credentials        *postgres.CredentialsT
	DB                 *sql.DB
	EventsMap          EventsCountMap
	WriteKey           string
	TableTestQueryFreq time.Duration
}

// SetWHPostgresDestination setup warehouse postgres destination
func SetWHPostgresDestination(pool *dockertest.Pool) (cleanup func()) {
	Test.PGTest = &PostgresTest{
		WriteKey: rand.String(27),
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
		TableTestQueryFreq: 100 * time.Millisecond,
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
		panic(fmt.Errorf("could not create WareHouse Postgres: %s", err.Error()))
	}

	// Getting at which port the postgres container is running
	credentials.Port = pgTest.Resource.GetPort("5432/tcp")

	purgeResources := func() {
		if pgTest.Resource != nil {
			log.Println("Purging warehouse postgres resource")
			if err := pool.Purge(pgTest.Resource); err != nil {
				log.Println(fmt.Errorf("could not purge warehouse postgres resource: %s", err.Error()))
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
		panic(fmt.Errorf("could not connect to warehouse postgres with error: %s", err.Error()))
	}
	cleanup = purgeResources
	return
}
