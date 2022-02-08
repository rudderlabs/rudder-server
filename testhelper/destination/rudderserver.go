package destination

import (
	"database/sql"
	_ "encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	_ "github.com/Shopify/sarama"
	_ "github.com/lib/pq"
	"github.com/minio/minio-go"
	"github.com/ory/dockertest"
	dc "github.com/ory/dockertest/docker"
	"github.com/phayes/freeport"
)

type PostgresResource struct {
	DB       *sql.DB
	DB_DSN   string
	Database string
	Password string
	User     string
	Port string
}
type TransformerResource struct {
	TransformURL string
	Port string
}

type MINIOResource struct {
	MinioEndpoint   string
	MinioBucketName string
	Port string
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
	var db  *sql.DB
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
		DB    :   db,
		DB_DSN :  db_dns,
		Database: database,
		Password :password,
		User     :user,
		Port :postgresContainer.GetPort("5432/tcp"),
	}, nil
}

func SetupTransformer(pool *dockertest.Pool, d deferer) (*TransformerResource, error) {
	// Set Rudder Transformer
	// pulls an image, creates a container based on it and runs it
	transformerContainer, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "rudderlabs/rudder-transformer",
		Tag:          "latest",
		ExposedPorts: []string{"9090"},
		Env: []string{
			"CONFIG_BACKEND_URL=https://api.rudderlabs.com",
		},
	})
	if err != nil {
		return nil, err
	}

	d.Defer(func() error {
		if err := pool.Purge(transformerContainer); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
		return nil
	})

	return &TransformerResource{
		TransformURL    :   fmt.Sprintf("http://localhost:%s", transformerContainer.GetPort("9090/tcp")),
		Port :  transformerContainer.GetPort("9090/tcp"),
	}, nil
}

func SetupMINIO(pool *dockertest.Pool, d deferer) (*MINIOResource, error) {
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

	minioContainer, err := pool.RunWithOptions(options)
	if err != nil {
		return nil, err
	}
	d.Defer(func() error {
		if err := pool.Purge(minioContainer); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
		return nil
	})

	minioEndpoint := fmt.Sprintf("localhost:%s", minioContainer.GetPort("9000/tcp"))

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	// the minio client does not do service discovery for you (i.e. it does not check if connection can be established), so we have to use the health check
	if err := pool.Retry(func() error {
		url := fmt.Sprintf("http://%s/minio/health/live", minioEndpoint)
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("status code not OK")
		}
		return nil
	}); err != nil {
		return nil, err
	}
	// now we can instantiate minio client
	minioClient, err = minio.New(minioEndpoint, "MYACCESSKEY", "MYSECRETKEY", false)
	if err != nil {
		return nil, err
	}
	// Create bucket for MINIO
	// Create a bucket at region 'us-east-1' with object locking enabled.
	minioBucketName := "devintegrationtest"
	err = minioClient.MakeBucket(minioBucketName, "us-east-1")
	if err != nil {
		return nil, err
	}
	return &MINIOResource{
		MinioEndpoint    :   minioEndpoint,
		MinioBucketName : minioBucketName,
		Port :  minioContainer.GetPort("9000/tcp"),
	}, nil
}
