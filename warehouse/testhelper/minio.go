package testhelper

import (
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/minio/minio-go"
	"log"
	"net/http"
	"strconv"
)

func SetupMinio() *MinioResource {
	minioPort := strconv.Itoa(54329)
	minioEndpoint := fmt.Sprintf("localhost:%s", minioPort)

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	// the minio client does not do service discovery for you (i.e. it does not check if connection can be established), so we have to use the health check
	operation := func() error {
		url := fmt.Sprintf("http://%s/minio/health/live", minioEndpoint)
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("status code not OK")
		}
		return nil
	}
	if err := backoff.Retry(operation, backoff.NewExponentialBackOff()); err != nil {
		log.Panicf("Error while checking health status of minio with error: %s", err.Error())
	}

	var minioClient *minio.Client
	var err error

	// now we can instantiate minio client
	minioClient, err = minio.New(minioEndpoint, "MYACCESSKEY", "MYSECRETKEY", false)
	if err != nil {
		log.Panicf("Error while instantiate minio client with error: %s", err.Error())
	}

	// Create bucket for MINIO
	// Create a bucket at region 'us-east-1' with object locking enabled.
	minioBucketName := "devintegrationtest"
	err = minioClient.MakeBucket(minioBucketName, "us-east-1")
	if err != nil {
		var exists bool
		exists, err = minioClient.BucketExists(minioBucketName)
		if !exists || err != nil {
			log.Panicf("Error while making minio bucket with error: %s", err.Error())
		}
	}

	return &MinioResource{
		MinioEndpoint:   minioEndpoint,
		MinioBucketName: minioBucketName,
		Port:            minioPort,
	}
}
