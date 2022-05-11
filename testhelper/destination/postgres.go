package destination

import (
	"database/sql"
	_ "encoding/json"
	"fmt"
	"log"

	_ "github.com/Shopify/sarama"
	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
)

type PostgresResource struct {
	DB       *sql.DB
	DB_DSN   string
	Database string
	Password string
	User     string
	Host     string
	Port     string
}

func SetupPostgres(pool *dockertest.Pool, d deferer) (*PostgresResource, error) {
	database := "jobsdb"
	password := "password"
	user := "rudder"

	// pulls an image, creates a container based on it and runs it
	postgresContainer, err := pool.Run("postgres", "11-alpine", []string{
		"POSTGRES_PASSWORD=" + password,
		"POSTGRES_DB=" + database,
		"POSTGRES_USER=" + user,
	})
	if err != nil {
		return nil, err
	}

	d.Defer(func() error {
		if err := pool.Purge(postgresContainer); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
		return nil
	})

	db_dns := fmt.Sprintf("postgres://rudder:password@localhost:%s/%s?sslmode=disable", postgresContainer.GetPort("5432/tcp"), database)
	var db *sql.DB
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		db, err = sql.Open("postgres", db_dns)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		return nil, err
	}
	return &PostgresResource{
		DB:       db,
		DB_DSN:   db_dns,
		Database: database,
		Password: password,
		User:     user,
		Host:     "localhost",
		Port:     postgresContainer.GetPort("5432/tcp"),
	}, nil
}
