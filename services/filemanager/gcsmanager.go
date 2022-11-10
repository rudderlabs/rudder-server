package filemanager

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/utils/googleutils"
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
	fileName := path.Join(manager.Config.Prefix, path.Join(prefixes...), path.Base(file.Name()))

	client, err := manager.getClient(ctx)
	if err != nil {
		return UploadOutput{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, manager.getTimeout())
	defer cancel()

	obj := client.Bucket(manager.Config.Bucket).Object(fileName)
	w := obj.NewWriter(ctx)
	if _, err := io.Copy(w, file); err != nil {
		err = fmt.Errorf("copying file to GCS: %v", err)
		if closeErr := w.Close(); closeErr != nil {
			return UploadOutput{}, fmt.Errorf("closing writer: %q, while: %w", closeErr, err)
		}

		return UploadOutput{}, err
	}
	err = w.Close()
	if err != nil {
		return UploadOutput{}, fmt.Errorf("closing writer: %w", err)
	}

	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return UploadOutput{}, err
	}

	return UploadOutput{Location: manager.objectURL(attrs), ObjectName: fileName}, err
}

func (manager *GCSManager) ListFilesWithPrefix(ctx context.Context, startAfter, prefix string, maxItems int64) (fileObjects []*FileObject, err error) {
	fileObjects = make([]*FileObject, 0)

	// Create GCS storage client
	client, err := manager.getClient(ctx)
	if err != nil {
		return
	}

	// Create GCS Bucket handle
	if manager.Config.Iterator == nil {
		manager.Config.Iterator = client.Bucket(manager.Config.Bucket).Objects(ctx, &storage.Query{
			Prefix:      prefix,
			Delimiter:   "",
			StartOffset: startAfter,
		})
	}
	var attrs *storage.ObjectAttrs
	for {
		if maxItems <= 0 {
			break
		}
		attrs, err = manager.Config.Iterator.Next()
		if err == iterator.Done || err != nil {
			if err == iterator.Done {
				err = nil
			}
			break
		}
		fileObjects = append(fileObjects, &FileObject{attrs.Name, attrs.Updated})
		maxItems--
	}
	return
}

func (manager *GCSManager) getClient(ctx context.Context) (*storage.Client, error) {
	var err error

	ctx, cancel := context.WithTimeout(ctx, manager.getTimeout())
	defer cancel()
	if manager.client != nil {
		return manager.client, err
	}
	options := []option.ClientOption{}

	if manager.Config.EndPoint != nil && *manager.Config.EndPoint != "" {
		options = append(options, option.WithEndpoint(*manager.Config.EndPoint))
	}
	if !googleutils.ShouldSkipCredentialsInit(manager.Config.Credentials) {
		if err = googleutils.CompatibleGoogleCredentialsJSON([]byte(manager.Config.Credentials)); err != nil {
			return manager.client, err
		}
		options = append(options, option.WithCredentialsJSON([]byte(manager.Config.Credentials)))
	}

	manager.client, err = storage.NewClient(ctx, options...)
	return manager.client, err
}

func (manager *GCSManager) Download(ctx context.Context, output *os.File, key string) error {
	client, err := manager.getClient(ctx)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, manager.getTimeout())
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

// TODO complete this
func (manager *GCSManager) GetDownloadKeyFromFileLocation(location string) string {
	splitStr := strings.Split(location, manager.Config.Bucket)
	key := strings.TrimLeft(splitStr[len(splitStr)-1], "/")
	return key
}

type GCSManager struct {
	Config  *GCSConfig
	client  *storage.Client
	timeout time.Duration
}

func (manager *GCSManager) SetTimeout(timeout time.Duration) {
	manager.timeout = timeout
}

func (manager *GCSManager) getTimeout() time.Duration {
	if manager.timeout > 0 {
		return manager.timeout
	}

	return getBatchRouterTimeoutConfig("GCS")
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
	Iterator       *storage.ObjectIterator
}

func (*GCSManager) DeleteObjects(_ context.Context, _ []string) (err error) {
	return
}

func (manager *GCSManager) GetConfiguredPrefix() string {
	return manager.Config.Prefix
}
