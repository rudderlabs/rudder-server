package filemanager

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/minio/minio-go/v6"
)

func (manager *MinioManager) ObjectUrl(objectName string) string {
	var protocol = "http"
	if manager.Config.UseSSL {
		protocol = "https"
	}
	return protocol + "://" + manager.Config.EndPoint + "/" + manager.Config.Bucket + "/" + objectName
}

func (manager *MinioManager) Upload(ctx context.Context, file *os.File, prefixes ...string) (UploadOutput, error) {
	if manager.Config.Bucket == "" {
		return UploadOutput{}, errors.New("no storage bucket configured to uploader")
	}

	minioClient, err := manager.getClient()
	if err != nil {
		return UploadOutput{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, getSafeTimeout(manager.Timeout))
	defer cancel()

	if err = minioClient.MakeBucketWithContext(ctx, manager.Config.Bucket, "us-east-1"); err != nil {
		exists, errBucketExists := minioClient.BucketExists(manager.Config.Bucket)
		if !(errBucketExists == nil && exists) {
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

	_, err = minioClient.FPutObjectWithContext(ctx, manager.Config.Bucket, fileName, file.Name(), minio.PutObjectOptions{})
	if err != nil {
		return UploadOutput{}, err
	}

	return UploadOutput{Location: manager.ObjectUrl(fileName), ObjectName: fileName}, nil
}

func (manager *MinioManager) Download(ctx context.Context, file *os.File, key string) error {
	minioClient, err := manager.getClient()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, getSafeTimeout(manager.Timeout))
	defer cancel()

	err = minioClient.FGetObjectWithContext(ctx, manager.Config.Bucket, key, file.Name(), minio.GetObjectOptions{})
	return err
}

/*
GetObjectNameFromLocation gets the object name/key name from the object location url
	https://minio-endpoint/bucket-name/key1 - >> key1
	http://minio-endpoint/bucket-name/key2 - >> key2
*/
func (manager *MinioManager) GetObjectNameFromLocation(location string) (string, error) {
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

func (manager *MinioManager) GetDownloadKeyFromFileLocation(location string) string {
	parsedUrl, err := url.Parse(location)
	if err != nil {
		fmt.Println("error while parsing location url: ", err)
	}
	trimedUrl := strings.TrimLeft(parsedUrl.Path, "/")
	return strings.TrimPrefix(trimedUrl, fmt.Sprintf(`%s/`, manager.Config.Bucket))
}

func (manager *MinioManager) DeleteObjects(ctx context.Context, keys []string) (err error) {

	objectChannel := make(chan string, len(keys))
	for _, key := range keys {
		objectChannel <- key
	}
	close(objectChannel)

	minioClient, err := manager.getClient()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, getSafeTimeout(manager.Timeout))
	defer cancel()

	tmp := <-minioClient.RemoveObjectsWithContext(ctx, manager.Config.Bucket, objectChannel)
	return tmp.Err
}

func (manager *MinioManager) ListFilesWithPrefix(ctx context.Context, prefix string, maxItems int64) (fileObjects []*FileObject, err error) {
	fileObjects = make([]*FileObject, 0)

	// Created minio core
	core, err := minio.NewCore(manager.Config.EndPoint, manager.Config.AccessKeyID, manager.Config.SecretAccessKey, manager.Config.UseSSL)
	if err != nil {
		return
	}

	// List the Objects in the bucket
	bucket, err := core.ListObjects(manager.Config.Bucket, prefix, prefix, "", int(maxItems))
	if err != nil {
		return
	}

	for _, item := range bucket.Contents {
		fileObjects = append(fileObjects, &FileObject{item.Key, item.LastModified})
	}
	return
}

func (manager *MinioManager) getClient() (*minio.Client, error) {
	var err error
	if manager.client == nil {
		manager.client, err = minio.New(manager.Config.EndPoint, manager.Config.AccessKeyID, manager.Config.SecretAccessKey, manager.Config.UseSSL)
		if err != nil {
			return &minio.Client{}, err
		}
	}
	return manager.client, nil
}

func (manager *MinioManager) GetConfiguredPrefix() string {
	return manager.Config.Prefix
}

func GetMinioConfig(config map[string]interface{}) *MinioConfig {
	var bucketName, prefix, endPoint, accessKeyID, secretAccessKey string
	var useSSL, ok bool
	if config["bucketName"] != nil {
		tmp, ok := config["bucketName"].(string)
		if ok {
			bucketName = tmp
		}
	}
	if config["prefix"] != nil {
		tmp, ok := config["prefix"].(string)
		if ok {
			prefix = tmp
		}
	}
	if config["endPoint"] != nil {
		tmp, ok := config["endPoint"].(string)
		if ok {
			endPoint = tmp
		}
	}
	if config["accessKeyID"] != nil {
		tmp, ok := config["accessKeyID"].(string)
		if ok {
			accessKeyID = tmp
		}
	}
	if config["secretAccessKey"] != nil {
		tmp, ok := config["secretAccessKey"].(string)
		if ok {
			secretAccessKey = tmp
		}
	}
	if config["useSSL"] != nil {
		if useSSL, ok = config["useSSL"].(bool); !ok {
			useSSL = false
		}
	}

	return &MinioConfig{
		Bucket:          bucketName,
		Prefix:          prefix,
		EndPoint:        endPoint,
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		UseSSL:          useSSL,
	}
}

type MinioManager struct {
	Config  *MinioConfig
	client  *minio.Client
	Timeout *time.Duration
}

func (manager *MinioManager) SetTimeout(timeout *time.Duration) {
	manager.Timeout = timeout
}

type MinioConfig struct {
	Bucket          string
	Prefix          string
	EndPoint        string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
}
