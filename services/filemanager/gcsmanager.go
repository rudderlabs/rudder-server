package filemanager

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"google.golang.org/api/iterator"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

func (manager *GCSManager) objectURL(objAttrs *storage.ObjectAttrs) string {
	if manager.Config.EndPoint != nil && *manager.Config.EndPoint != "" {
		return fmt.Sprintf("%s/%s/%s", *manager.Config.EndPoint, objAttrs.Bucket, objAttrs.Name)
	}
	return fmt.Sprintf("https://storage.googleapis.com/%s/%s", objAttrs.Bucket, objAttrs.Name)
}

func (manager *GCSManager) Upload(ctx context.Context, file *os.File, prefixes ...string) (UploadOutput, error) {
	splitFileName := strings.Split(file.Name(), "/")
	fileName := ""
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

	client, err := manager.getClient(ctx)
	if err != nil {
		return UploadOutput{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, getSafeTimeout(manager.Timeout))
	defer cancel()

	obj := client.Bucket(manager.Config.Bucket).Object(fileName)
	w := obj.NewWriter(ctx)
	defer func() error {
		return w.Close()
	}()
	if _, err := io.Copy(w, file); err != nil {
		return UploadOutput{}, err
	}
	w.Close()

	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return UploadOutput{}, err
	}

	return UploadOutput{Location: manager.objectURL(attrs), ObjectName: fileName}, err
}

func (manager *GCSManager) ListFilesWithPrefix(ctx context.Context, prefix string, maxItems int64) (fileObjects []*FileObject, err error) {
	fileObjects = make([]*FileObject, 0)

	// Create GCS storage client
	client, err := manager.getClient(ctx)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, getSafeTimeout(manager.Timeout))
	defer cancel()

	// Create GCS Bucket handle
	it := client.Bucket(manager.Config.Bucket).Objects(ctx, &storage.Query{
		Prefix:    prefix,
		Delimiter: "",
	})
	for {
		if maxItems == 0 {
			break
		}
		attrs, err := it.Next()
		if err == iterator.Done || err != nil {
			break
		}
		fileObjects = append(fileObjects, &FileObject{attrs.Name, attrs.Updated})
		maxItems--
	}
	return
}

func (manager *GCSManager) getClient(ctx context.Context) (*storage.Client, error) {
	var err error

	ctx, cancel := context.WithTimeout(ctx, getSafeTimeout(manager.Timeout))
	defer cancel()

	if manager.client == nil {
		if manager.Config.EndPoint != nil && *manager.Config.EndPoint != "" {
			manager.client, err = storage.NewClient(ctx, option.WithEndpoint(*manager.Config.EndPoint))
		} else if manager.Config.Credentials == "" {
			manager.client, err = storage.NewClient(ctx)
		} else {
			manager.client, err = storage.NewClient(ctx, option.WithCredentialsJSON([]byte(manager.Config.Credentials)))
		}
	}

	if manager.client == nil {
		manager.client, err = storage.NewClient(ctx, option.WithCredentialsJSON([]byte(manager.Config.Credentials)))
	}
	return manager.client, err
}

func (manager *GCSManager) Download(ctx context.Context, output *os.File, key string) error {
	client, err := manager.getClient(ctx)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, getSafeTimeout(manager.Timeout))
	defer cancel()

	rc, err := client.Bucket(manager.Config.Bucket).Object(key).NewReader(ctx)
	if err != nil {
		return err
	}
	defer rc.Close()

	_, err = io.Copy(output, rc)
	return err
}

/*
GetObjectNameFromLocation gets the object name/key name from the object location url
	https://storage.googleapis.com/bucket-name/key - >> key
*/
func (manager *GCSManager) GetObjectNameFromLocation(location string) (string, error) {
	splitStr := strings.Split(location, manager.Config.Bucket)
	object := strings.TrimLeft(splitStr[len(splitStr)-1], "/")
	return object, nil
}

//TODO complete this
func (manager *GCSManager) GetDownloadKeyFromFileLocation(location string) string {
	splitStr := strings.Split(location, manager.Config.Bucket)
	key := strings.TrimLeft(splitStr[len(splitStr)-1], "/")
	return key
}

type GCSManager struct {
	Config  *GCSConfig
	client  *storage.Client
	Timeout *time.Duration
}

func GetGCSConfig(config map[string]interface{}) *GCSConfig {
	var bucketName, prefix, credentials string
	var endPoint *string
	var forcePathStyle, disableSSL *bool

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
	if config["credentials"] != nil {
		tmp, ok := config["credentials"].(string)
		if ok {
			credentials = tmp
		}
	}
	if config["endPoint"] != nil {
		tmp, ok := config["endPoint"].(string)
		if ok {
			endPoint = &tmp
		}
	}
	if config["forcePathStyle"] != nil {
		tmp, ok := config["forcePathStyle"].(bool)
		if ok {
			forcePathStyle = &tmp
		}
	}
	if config["disableSSL"] != nil {
		tmp, ok := config["disableSSL"].(bool)
		if ok {
			disableSSL = &tmp
		}
	}
	return &GCSConfig{
		Bucket:         bucketName,
		Prefix:         prefix,
		Credentials:    credentials,
		EndPoint:       endPoint,
		ForcePathStyle: forcePathStyle,
		DisableSSL:     disableSSL,
	}
}

type GCSConfig struct {
	Bucket         string
	Prefix         string
	Credentials    string
	EndPoint       *string
	ForcePathStyle *bool
	DisableSSL     *bool
}

func (manager *GCSManager) DeleteObjects(ctx context.Context, locations []string) (err error) {
	return
}

func (manager *GCSManager) GetConfiguredPrefix() string {
	return manager.Config.Prefix
}
