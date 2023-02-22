package destination

import (
	"database/sql"
	_ "encoding/json"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
)

const (
	postgresDefaultDB       = "jobsdb"
	postgresDefaultUser     = "rudder"
	postgresDefaultPassword = "password"
)

type PostgresResource struct {
	DB       *sql.DB
	DBDsn    string
	Database string
	Password string
	User     string
	Host     string
	Port     string
}

func SetupPostgres(pool *dockertest.Pool, d cleaner, opts ...string) (*PostgresResource, error) {
	cmd := []string{"postgres"}
	for _, opt := range opts {
		cmd = append(cmd, "-c", opt)
	}
	// pulls an image, creates a container based on it and runs it
	postgresContainer, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "15-alpine",
		Env: []string{
			"POSTGRES_PASSWORD=" + postgresDefaultPassword,
			"POSTGRES_DB=" + postgresDefaultDB,
			"POSTGRES_USER=" + postgresDefaultUser,
		},
		Cmd: cmd,
	})
	if err != nil {
		return nil, err
	}

	d.Cleanup(func() {
		if err := pool.Purge(postgresContainer); err != nil {
			d.Log("Could not purge resource:", err)
		}
	})

	dbDSN := fmt.Sprintf(
		"postgres://%s:%s@localhost:%s/%s?sslmode=disable",
		postgresDefaultUser, postgresDefaultPassword, postgresContainer.GetPort("5432/tcp"), postgresDefaultDB,
	)
	var db *sql.DB
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	err = pool.Retry(func() (err error) {
		if db, err = sql.Open("postgres", dbDSN); err != nil {
			return err
		}
		return db.Ping()
	})
	if err != nil {
		return nil, err
	}
	return &PostgresResource{
		DB:       db,
		DBDsn:    dbDSN,
		Database: postgresDefaultDB,
		User:     postgresDefaultUser,
		Password: postgresDefaultPassword,
		Host:     "localhost",
		Port:     postgresContainer.GetPort("5432/tcp"),
	}, nil
}
