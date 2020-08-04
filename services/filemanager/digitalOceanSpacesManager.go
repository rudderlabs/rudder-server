package filemanager

import (
	"errors"
	"os"
	"strings"

	"github.com/minio/minio-go/v6"
)

func (manager *DOspacesManager) ObjectUrl(objectName string) string {
	var protocol = "http"
	if manager.Config.UseSSL == true {
		protocol = "https"
	}
	return protocol + "://" + manager.Config.EndPoint + "/" + manager.Config.Bucket + "/" + objectName
}

func (manager *DOspacesManager) Upload(file *os.File, prefixes ...string) (UploadOutput, error) {
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
	if manager.Config.Prefix != "" {
		if manager.Config.Prefix[len(manager.Config.Prefix)-1:] == "/" {
			fileName = manager.Config.Prefix + fileName
		} else {
			fileName = manager.Config.Prefix + "/" + fileName
		}
	}
	_, err = minioClient.FPutObject(manager.Config.Bucket, fileName, file.Name(), minio.PutObjectOptions{})
	if err != nil {
		return UploadOutput{}, nil
	}

	return UploadOutput{Location: manager.ObjectUrl(fileName), ObjectName: fileName}, nil
}

func (manager *DOspacesManager) Download(file *os.File, key string) error {
	minioClient, err := minio.New(manager.Config.EndPoint, manager.Config.AccessKeyID, manager.Config.SecretAccessKey, manager.Config.UseSSL)
	if err != nil {
		return err
	}
	err = minioClient.FGetObject(manager.Config.Bucket, key, file.Name(), minio.GetObjectOptions{})
	return err
}

/*
GetObjectNameFromLocation gets the object name/key name from the object location url
	https://minio-endpoint/bucket-name/key1 - >> key1
	http://minio-endpoint/bucket-name/key2 - >> key2
*/
func (manager *DOspacesManager) GetObjectNameFromLocation(location string) (string, error) {
	var baseURL string
	if manager.Config.UseSSL {
		baseURL += "https://"
	} else {
		baseURL += "http://"
	}
	baseURL += manager.Config.EndPoint + "/"
	baseURL += manager.Config.Bucket + "/"
	return location[len(baseURL):], nil
}

//TODO complete this
func (manager *DOspacesManager) GetDownloadKeyFromFileLocation(location string) string {
	return location
}

func GetDOspacesConfig(config map[string]interface{}) *DOspacesConfig {
	var bucketName, prefix, endPoint, accessKeyID, secretAccessKey string
	var useSSL, ok bool
	if config["bucketName"] != nil {
		bucketName = config["bucketName"].(string)
	}
	if config["prefix"] != nil {
		prefix = config["prefix"].(string)
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
		if useSSL, ok = config["useSSL"].(bool); !ok {
			useSSL = false
		}
	}

	return &DOspacesConfig{
		Bucket:          bucketName,
		Prefix:          prefix,
		EndPoint:        endPoint,
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		UseSSL:          useSSL,
	}
}

type DOspacesManager struct {
	Config *DOspacesConfig
}

type DOspacesConfig struct {
	Bucket          string
	Prefix          string
	EndPoint        string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
}
