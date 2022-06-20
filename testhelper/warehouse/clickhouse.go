package warehouse_test

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/ory/dockertest/v3"

	"github.com/rudderlabs/rudder-server/testhelper/rand"
	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
)

type ClickHouseTest struct {
	Resource           *dockertest.Resource
	Credentials        *clickhouse.CredentialsT
	DB                 *sql.DB
	EventsMap          EventsCountMap
	WriteKey           string
	TableTestQueryFreq time.Duration
}

// SetWHClickHouseDestination setup warehouse clickhouse destination
func SetWHClickHouseDestination(pool *dockertest.Pool) (cleanup func()) {
	Test.CHTest = &ClickHouseTest{
		WriteKey: rand.String(27),
		Credentials: &clickhouse.CredentialsT{
			Host:          "localhost",
			User:          "rudder",
			Password:      "rudder-password",
			DBName:        "rudderdb",
			Secure:        "false",
			SkipVerify:    "true",
			TLSConfigName: "",
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
	chTest := Test.CHTest
	credentials := chTest.Credentials
	cleanup = func() {}

	var err error
	if chTest.Resource, err = pool.Run("yandex/clickhouse-server", "21-alpine", []string{
		fmt.Sprintf("CLICKHOUSE_DB=%s", credentials.DBName),
		fmt.Sprintf("CLICKHOUSE_PASSWORD=%s", credentials.Password),
		fmt.Sprintf("CLICKHOUSE_USER=%s", credentials.User),
	}); err != nil {
		panic(fmt.Errorf("could not create WareHouse ClickHouse: %s", err.Error()))
	}

	// Getting at which port the clickhouse container is running
	credentials.Port = chTest.Resource.GetPort("9000/tcp")

	purgeResources := func() {
		if chTest.Resource != nil {
			log.Println("Purging warehouse clickhouse resource")
			if err := pool.Purge(chTest.Resource); err != nil {
				log.Println(fmt.Errorf("could not purge warehouse clickhouse resource: %s", err.Error()))
			}
		}
	}

	if err = pool.Retry(func() error {
		var err error
		chTest.DB, err = clickhouse.Connect(*credentials, true)
		if err != nil {
			return err
		}
		return chTest.DB.Ping()
	}); err != nil {
		defer purgeResources()
		panic(fmt.Errorf("could not connect to warehouse clickhouse with error: %s", err.Error()))
	}
	cleanup = purgeResources
	return
}
