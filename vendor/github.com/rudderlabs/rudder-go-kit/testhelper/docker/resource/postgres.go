package resource

import (
	"database/sql"
	_ "encoding/json"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
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

func SetupPostgres(pool *dockertest.Pool, d cleaner, opts ...func(*postgres.Config)) (*PostgresResource, error) {
	c := &postgres.Config{
		Tag:     "15-alpine",
		ShmSize: 512 * bytesize.MB,
	}
	for _, opt := range opts {
		opt(c)
	}

	cmd := []string{"postgres"}
	for _, opt := range c.Options {
		cmd = append(cmd, "-c", opt)
	}
	// pulls an image, creates a container based on it and runs it
	postgresContainer, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        c.Tag,
		Env: []string{
			"POSTGRES_PASSWORD=" + postgresDefaultPassword,
			"POSTGRES_DB=" + postgresDefaultDB,
			"POSTGRES_USER=" + postgresDefaultUser,
		},
		Cmd: cmd,
	}, func(hc *dc.HostConfig) {
		hc.ShmSize = c.ShmSize
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
