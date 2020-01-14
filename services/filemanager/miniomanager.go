package filemanager

import (
	"errors"
	"github.com/minio/minio-go/v6"
	"os"
	"strings"
)

func (manager *MinioManager) ObjectUrl(objectName string) string {
	return manager.Config.Protocol + "://" + manager.Config.EndPoint + "/" + manager.Config.Bucket + "/" + objectName
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
		if !exists {
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
	if err != nil {
		return UploadOutput{}, nil
	}

	return UploadOutput{Location: manager.ObjectUrl(fileName)}, nil
}

func (manager *MinioManager) Download(file *os.File, key string) error {
	minioClient, err := minio.New(manager.Config.EndPoint, manager.Config.AccessKeyID, manager.Config.SecretAccessKey, manager.Config.UseSSL)
	if err != nil {
		return err
	}
	err = minioClient.FGetObject(manager.Config.Bucket, key, file.Name(), minio.GetObjectOptions{})
	return err
}

func GetMinioConfig(config map[string]interface{}) *MinioConfig {
	var bucketName, folderName, endPoint, accessKeyID, secretAccessKey, protocol string
	var useSSL bool
	if config["bucketName"] != nil {
		bucketName = config["bucketName"].(string)
	}
	if config["folderName"] != nil {
		folderName = config["folderName"].(string)
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
	if config["useSSL"] != nil {
		useSSL = config["useSSL"].(bool)
	}
	if config["protocol"] != nil {
		protocol = config["protocol"].(string)
	}

	return &MinioConfig{
		Bucket:          bucketName,
		Folder:          folderName,
		EndPoint:        endPoint,
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		UseSSL:          useSSL,
		Protocol:        protocol,
	}
}

type MinioManager struct {
	Config *MinioConfig
}

type MinioConfig struct {
	Bucket          string
	Folder          string
	EndPoint        string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
	Protocol        string
}
