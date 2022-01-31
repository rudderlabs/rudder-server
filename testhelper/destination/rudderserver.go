package main_test

import (
	"database/sql"
	_ "encoding/json"
	"fmt"
	_ "github.com/Shopify/sarama"
	_ "github.com/lib/pq"
	"github.com/minio/minio-go"
	"github.com/ory/dockertest"
	dc "github.com/ory/dockertest/docker"
	"github.com/phayes/freeport"
	"log"
	"net/http"
	"strconv"
)

var (
	DB_DSN          = "root@tcp(127.0.0.1:3306)/service"
)
type ServerTest struct {
	db              *sql.DB
	minioEndpoint   string
	minioBucketName string
}
var (
	PostgresTest *ServerTest
)

func SetJobsDB() (*sql.DB, *dockertest.Resource) {
	PostgresTest = &ServerTest{}
	database := "jobsdb"
	// pulls an image, creates a container based on it and runs it
	resourcePostgres, err := Test.pool.Run("postgres", "11-alpine", []string{
		"POSTGRES_PASSWORD=password",
		"POSTGRES_DB=" + database,
		"POSTGRES_USER=rudder",
	})
	if err != nil {
		log.Println("Could not start resource Postgres: %w", err)
	}
	DB_DSN = fmt.Sprintf("postgres://rudder:password@localhost:%s/%s?sslmode=disable", resourcePostgres.GetPort("5432/tcp"), database)
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := Test.pool.Retry(func() error {
		var err error
		PostgresTest.db, err = sql.Open("postgres", DB_DSN)
		if err != nil {
			return err
		}
		return PostgresTest.db.Ping()
	}); err != nil {
		log.Println("Could not connect to postgres", DB_DSN, err)
	}
	fmt.Println("DB_DSN:", DB_DSN)
	return PostgresTest.db, resourcePostgres
}

func SetTransformer() *dockertest.Resource {
	// Set Rudder Transformer
	// pulls an image, creates a container based on it and runs it
	transformerRes, err := Test.pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "rudderlabs/rudder-transformer",
		Tag:          "latest",
		ExposedPorts: []string{"9090"},
		Env: []string{
			"CONFIG_BACKEND_URL=https://api.rudderlabs.com",
		},
	})
	if err != nil {
		log.Println("Could not start resource transformer: %w", err)
	}
	return transformerRes
}

func SetMINIO() (string, string, *dockertest.Resource) {
	minioPortInt, err := freeport.GetFreePort()
	if err != nil {
		fmt.Println(err)
	}
	minioPort := fmt.Sprintf("%s/tcp", strconv.Itoa(minioPortInt))
	log.Println("minioPort:", minioPort)
	// Setup MINIO
	var minioClient *minio.Client

	options := &dockertest.RunOptions{
		Repository: "minio/minio",
		Tag:        "latest",
		Cmd:        []string{"server", "/data"},
		PortBindings: map[dc.Port][]dc.PortBinding{
			"9000/tcp": {{HostPort: strconv.Itoa(minioPortInt)}},
		},
		Env: []string{"MINIO_ACCESS_KEY=MYACCESSKEY", "MINIO_SECRET_KEY=MYSECRETKEY"},
	}

	resource, err := Test.pool.RunWithOptions(options)
	if err != nil {
		log.Println("Could not start resource:", err)
	}

	PostgresTest.minioEndpoint = fmt.Sprintf("localhost:%s", resource.GetPort("9000/tcp"))

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	// the minio client does not do service discovery for you (i.e. it does not check if connection can be established), so we have to use the health check
	if err := Test.pool.Retry(func() error {
		url := fmt.Sprintf("http://%s/minio/health/live", PostgresTest.minioEndpoint)
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("status code not OK")
		}
		return nil
	}); err != nil {
		log.Printf("Could not connect to docker: %s", err)
	}
	// now we can instantiate minio client
	minioClient, err = minio.New(PostgresTest.minioEndpoint, "MYACCESSKEY", "MYSECRETKEY", false)
	if err != nil {
		log.Println("Failed to create minio client:", err)
		panic(err)
	}
	log.Printf("%#v\n", minioClient) // minioClient is now set up

	// Create bucket for MINIO
	// Create a bucket at region 'us-east-1' with object locking enabled.
	PostgresTest.minioBucketName = "devintegrationtest"
	err = minioClient.MakeBucket(PostgresTest.minioBucketName, "us-east-1")
	if err != nil {
		log.Println(err)
		panic(err)
	}
	return PostgresTest.minioEndpoint, PostgresTest.minioBucketName, resource
}
