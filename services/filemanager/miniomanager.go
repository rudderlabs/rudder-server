package filemanager

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func (manager *MinioManager) ObjectUrl(objectName string) string {
	protocol := "http"
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

	ctx, cancel := context.WithTimeout(ctx, manager.getTimeout())
	defer cancel()

	exists, err := minioClient.BucketExists(ctx, manager.Config.Bucket)
	if err != nil {
		return UploadOutput{}, fmt.Errorf("checking bucket: %w", err)
	}
	if !exists {
		if err = minioClient.MakeBucket(ctx, manager.Config.Bucket, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
			return UploadOutput{}, fmt.Errorf("creating bucket: %w", err)
		}
	}

	fileName := path.Join(manager.Config.Prefix, path.Join(prefixes...), path.Base(file.Name()))

	_, err = minioClient.FPutObject(ctx, manager.Config.Bucket, fileName, file.Name(), minio.PutObjectOptions{})
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

	ctx, cancel := context.WithTimeout(ctx, manager.getTimeout())
	defer cancel()

	err = minioClient.FGetObject(ctx, manager.Config.Bucket, key, file.Name(), minio.GetObjectOptions{})
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
	objectChannel := make(chan minio.ObjectInfo, len(keys))
	for _, key := range keys {
		objectChannel <- minio.ObjectInfo{Key: key}
	}
	close(objectChannel)

	minioClient, err := manager.getClient()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, manager.getTimeout())
	defer cancel()

	tmp := <-minioClient.RemoveObjects(ctx, manager.Config.Bucket, objectChannel, minio.RemoveObjectsOptions{})
	return tmp.Err
}

func (manager *MinioManager) ListFilesWithPrefix(_ context.Context, startAfter, prefix string, maxItems int64) (fileObjects []*FileObject, err error) {
	if !manager.Config.IsTruncated {
		pkgLogger.Infof("Manager is truncated: %v so returning here", manager.Config.IsTruncated)
		return
	}
	fileObjects = make([]*FileObject, 0)

	// Created minio core
	core, err := minio.NewCore(manager.Config.EndPoint, &minio.Options{
		Creds:  credentials.NewStaticV4(manager.Config.AccessKeyID, manager.Config.SecretAccessKey, ""),
		Secure: manager.Config.UseSSL,
	})
	if err != nil {
		return
	}

	// List the Objects in the bucket
	result, err := core.ListObjectsV2(manager.Config.Bucket, prefix, startAfter, manager.Config.ContinuationToken, "", int(maxItems))
	if err != nil {
		return
	}

	for idx := range result.Contents {
		fileObjects = append(fileObjects, &FileObject{result.Contents[idx].Key, result.Contents[idx].LastModified})
	}
	manager.Config.IsTruncated = result.IsTruncated
	manager.Config.ContinuationToken = result.NextContinuationToken
	return
}

func (manager *MinioManager) getClient() (*minio.Client, error) {
	var err error
	if manager.client == nil {
		manager.client, err = minio.New(manager.Config.EndPoint, &minio.Options{
			Creds:  credentials.NewStaticV4(manager.Config.AccessKeyID, manager.Config.SecretAccessKey, ""),
			Secure: manager.Config.UseSSL,
		})
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
	var bucketName, prefix, endPoint, accessKeyID, secretAccessKey, continuationToken string
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
		Bucket:            bucketName,
		Prefix:            prefix,
		EndPoint:          endPoint,
		AccessKeyID:       accessKeyID,
		SecretAccessKey:   secretAccessKey,
		UseSSL:            useSSL,
		ContinuationToken: continuationToken,
		IsTruncated:       true,
	}
}

type MinioManager struct {
	Config  *MinioConfig
	client  *minio.Client
	timeout time.Duration
}

func (manager *MinioManager) SetTimeout(timeout time.Duration) {
	manager.timeout = timeout
}

func (manager *MinioManager) getTimeout() time.Duration {
	if manager.timeout > 0 {
		return manager.timeout
	}

	return getBatchRouterTimeoutConfig("MINIO")
}

type MinioConfig struct {
	Bucket            string
	Prefix            string
	EndPoint          string
	AccessKeyID       string
	SecretAccessKey   string
	UseSSL            bool
	ContinuationToken string
	IsTruncated       bool
}
