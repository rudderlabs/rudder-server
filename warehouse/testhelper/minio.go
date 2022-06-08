package testhelper

import (
	"fmt"
	"github.com/minio/minio-go"
	"log"
	"strconv"
)

func SetupMinio() *MinioResource {
	minioPort := strconv.Itoa(54329)
	minioEndpoint := fmt.Sprintf("localhost:%s", minioPort)

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
