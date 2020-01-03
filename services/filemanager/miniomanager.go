package filemanager

import (
	"errors"
	"github.com/minio/minio-go/v6"
	"github.com/rudderlabs/rudder-server/config"
	"os"
	"strings"
)

func ObjectUrl(bucketName string, objectName string) string {
	return config.GetEnv("MINIO_PROTOCOL", "http") + "://" + config.GetEnv("MINIO_ENDPOINT", "localhost:9000") + "/" + bucketName + "/" + objectName
}

func (manager *MinioManager) Upload(file *os.File, prefixes ...string) (UploadOutput, error) {
	if manager.Config.Bucket == "" {
		return UploadOutput{}, errors.New("no storage bucket configured to uploader")
	}
	minioClient, err := minio.New(manager.Config.EndPoint, manager.Config.AccessKeyID, manager.Config.SecretAccessKey, manager.Config.UseSSL)
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
	minioClient, err := minio.New(manager.Config.EndPoint, manager.Config.AccessKeyID, manager.Config.SecretAccessKey, manager.Config.UseSSL)
	if err != nil {
		return err
	}
	err = minioClient.FGetObject(manager.Config.Bucket, key , file.Name(), minio.GetObjectOptions{})
	return err
}

func GetMinioConfig(config map[string]interface{}) *MinioConfig {
	var bucketName, endPoint, accessKeyID, secretAccessKey string
	if config["bucketName"] != nil {
		bucketName = config["bucketName"].(string)
	}
	if config["endPoint"] != nil {
		endPoint = config["endPoint"].(string)
	}
	if config["accessKeyID"] != nil {
		accessKeyID = config["accessKeyID"].(string)
	}
	if config["secretAccessKey"] != nil {
		secretAccessKey = config["secretAccessKey"].(string)
	}

	return &MinioConfig{
		Bucket: bucketName,
		EndPoint: endPoint,
		AccessKeyID: accessKeyID,
		SecretAccessKey: secretAccessKey,
		UseSSL: config["useSSL"].(bool),
	}
}

type MinioManager struct {
	Config *MinioConfig
}
type MinioConfig struct {
	Bucket string
	EndPoint string
	AccessKeyID string
	SecretAccessKey string
	UseSSL bool

}
