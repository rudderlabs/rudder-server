package integration__tests

import (
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integration__tests/testhelper"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/postgres"
)

// SetUpPostgres setup warehouse postgres destination
func SetUpPostgres() (pgTest *testhelper.PostgresTest) {
	pgTest = &testhelper.PostgresTest{
		WriteKey: testhelper.RandString(27),
		Credentials: &postgres.CredentialsT{
			DBName:   "rudderdb",
			Password: "rudder-password",
			User:     "rudder",
			Host:     "localhost",
			SSLMode:  "disable",
			Port:     "54320",
		},
		EventsMap: testhelper.EventsCountMap{
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

	var err error
	if pgTest.DB, err = postgres.Connect(*pgTest.Credentials); err != nil {
		panic(fmt.Errorf("could not connect to warehouse postgres with error: %s", err.Error()))
	}
	if err = pgTest.DB.Ping(); err != nil {
		panic(fmt.Errorf("could not connect to warehouse postgres while pinging with error: %s", err.Error()))
	}
	return
}

func TestPostgres(t *testing.T) {
	t.Parallel()

	pgTest := PGTest
	randomness := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")

	whDestTest := &testhelper.WareHouseDestinationTest{
		Client: &client.Client{
			SQL:  pgTest.DB,
			Type: client.SQLClient,
		},
		EventsCountMap:     pgTest.EventsMap,
		WriteKey:           pgTest.WriteKey,
		UserId:             fmt.Sprintf("userId_postgres_%s", randomness),
		Schema:             "postgres_wh_integration",
		TableTestQueryFreq: pgTest.TableTestQueryFreq,
	}
	sendEvents(whDestTest)
	destinationTest(t, whDestTest)

	randomness = strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	whDestTest.UserId = fmt.Sprintf("userId_postgres_%s", randomness)
	sendUpdatedEvents(whDestTest)
	destinationTest(t, whDestTest)
}

func SetUpJobsDB() (jobTest *testhelper.JobsDBResource) {
	pgCredentials := &postgres.CredentialsT{
		DBName:   "jobsdb",
		Password: "password",
		User:     "rudder",
		Host:     "localhost",
		SSLMode:  "disable",
		Port:     "54328",
	}
	jobTest = &testhelper.JobsDBResource{}
	jobTest.Credentials = pgCredentials

	var err error
	if jobTest.DB, err = postgres.Connect(*pgCredentials); err != nil {
		panic(fmt.Errorf("could not connect to jobsDb with error: %s", err.Error()))
	}
	if err = jobTest.DB.Ping(); err != nil {
		panic(fmt.Errorf("could not connect to jobsDb while pinging with error: %s", err.Error()))
	}
	return
}
