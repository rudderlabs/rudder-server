package filemanager

import (
	"errors"
	"github.com/minio/minio-go/v6"
	"github.com/rudderlabs/rudder-server/config"
	"os"
	"strconv"
	"strings"
)

func ObjectUrl(bucketName string, objectName string) string {
	return config.GetEnv("MINIO_PROTOCOL", "http") + "://" + config.GetEnv("MINIO_END_POINT", "localhost:9000") + "/" + bucketName + "/" + objectName
}

func (manager *MinioManager) Upload(file *os.File, prefixes ...string) (UploadOutput, error) {
	if manager.Config.Bucket == "" {
		return UploadOutput{}, errors.New("no storage bucket configured to uploader")
	}
	endPoint, accessKeyid, secretAccessKey, ssl := config.GetEnv("MINIO_END_POINT", "http://localhost:9000"), config.GetEnv("MINIO_ACCESS_KEY_ID", "minioadmin"), config.GetEnv("MINIO_SECRET_ACCESS_KEY", "minioadmin"), config.GetEnv("MINIO_SSL", "false")
	sslBool, err := strconv.ParseBool(ssl)
	if err != nil {
		sslBool = false
	}
	minioClient, err := minio.New(endPoint, accessKeyid, secretAccessKey, sslBool)
	if err != nil {
		return UploadOutput{}, err
	}
	if err = minioClient.MakeBucket(manager.Config.Bucket, "us-east-1"); err != nil {
		exists, err := minioClient.BucketExists(manager.Config.Bucket)
		if !(err == nil && exists) {
			return UploadOutput{}, err
		}
	}
	fileName := ""
	splitFileName := strings.Split(file.Name(), "/")
	if len(prefixes) > 0 {
		fileName = strings.Join(prefixes[:], "/") + "/"
	}
	fileName += splitFileName[len(splitFileName)-1]
	_, err = minioClient.FPutObject(manager.Config.Bucket, fileName, file.Name(), minio.PutObjectOptions{})
	if err!=nil {
		return UploadOutput{}, nil
	}

	return UploadOutput{Location: ObjectUrl(manager.Config.Bucket, fileName)}, nil
}

func (manager *MinioManager) Download(file *os.File, key string) error {
	endPoint, accessKeyid, secretAccessKey, ssl := config.GetEnv("MINIO_END_POINT", "http://localhost:9000"), config.GetEnv("MINIO_ACCESS_KEY_ID", "minioadmin"), config.GetEnv("MINIO_SECRET_ACCESS_KEY", "minioadmin"), config.GetEnv("MINIO_SSL", "false")
	sslBool, err := strconv.ParseBool(ssl)
	if err != nil {
		sslBool = false
	}
	minioClient, err := minio.New(endPoint, accessKeyid, secretAccessKey, sslBool)
	if err != nil {
		return err
	}
	err = minioClient.FGetObject(manager.Config.Bucket, key , file.Name(), minio.GetObjectOptions{})
	return err
}

func GetMinioConfig(config map[string]interface{}) *MinioConfig {
	return &MinioConfig{Bucket: config["bucketName"].(string)}
}

type MinioManager struct {
	Config *MinioConfig
}
type MinioConfig struct {
	Bucket string
}
