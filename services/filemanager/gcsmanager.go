package filemanager

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"google.golang.org/api/iterator"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

func (manager *GCSManager) objectURL(objAttrs *storage.ObjectAttrs) string {
	if manager.Config.EndPoint != nil {
		return fmt.Sprintf("%s/%s/%s", *manager.Config.EndPoint, objAttrs.Bucket, objAttrs.Name)
	}
	return fmt.Sprintf("https://storage.googleapis.com/%s/%s", objAttrs.Bucket, objAttrs.Name)
}

func (manager *GCSManager) Upload(file *os.File, prefixes ...string) (UploadOutput, error) {

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

	client, err := manager.getClient()
	if err != nil {
		return UploadOutput{}, err
	}
	obj := client.Bucket(manager.Config.Bucket).Object(fileName)
	w := obj.NewWriter(context.Background())
	defer func() error {
		return w.Close()
	}()
	if _, err := io.Copy(w, file); err != nil {
		return UploadOutput{}, err
	}
	w.Close()
	attrs, err := obj.Attrs(context.Background())
	if err != nil {
		return UploadOutput{}, err
	}

	return UploadOutput{Location: manager.objectURL(attrs), ObjectName: fileName}, err
}

func (manager *GCSManager) ListFilesWithPrefix(prefix string, maxItems int64) (fileObjects []*FileObject, err error) {
	fileObjects = make([]*FileObject, 0)
	ctx := context.Background()

	// Create GCS storage client
	client, err := manager.getClient()
	if err != nil {
		return
	}
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

func (manager *GCSManager) getClient() (*storage.Client, error) {
	var err error
	if manager.client == nil {
		ctx := context.Background()
		if manager.Config.EndPoint != nil {
			manager.client, err = storage.NewClient(ctx, option.WithEndpoint(*manager.Config.EndPoint))
		} else if manager.Config.Credentials == "" {
			manager.client, err = storage.NewClient(ctx)
		} else {
			manager.client, err = storage.NewClient(ctx, option.WithCredentialsJSON([]byte(manager.Config.Credentials)))
		}
	}
	if manager.client == nil {
		ctx := context.Background()
		manager.client, err = storage.NewClient(ctx, option.WithCredentialsJSON([]byte(manager.Config.Credentials)))
	}
	return manager.client, err
}

func (manager *GCSManager) Download(output *os.File, key string) error {
	ctx := context.Background()

	client, err := manager.getClient()
	if err != nil {
		return err
	}
	rc, err := client.Bucket(manager.Config.Bucket).Object(key).NewReader(ctx)
	if err != nil {
		fmt.Println("error while getting new reader to read from server")
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
	Config *GCSConfig
	client *storage.Client
}

func GetGCSConfig(config map[string]interface{}) *GCSConfig {
	var bucketName, prefix, credentials string
	var endPoint *string
	var forcePathStyle, disableSSL *bool

	if config["bucketName"] != nil {
		bucketName = config["bucketName"].(string)
	}
	if config["prefix"] != nil {
		prefix = config["prefix"].(string)
	}
	if config["credentials"] != nil {
		credentials = config["credentials"].(string)
	}
	if config["endPoint"] != nil {
		tmp := config["endPoint"].(string)
		endPoint = &tmp
	}
	if config["forcePathStyle"] != nil {
		tmp := config["forcePathStyle"].(bool)
		forcePathStyle = &tmp
	}
	if config["disableSSL"] != nil {
		tmp := config["disableSSL"].(bool)
		disableSSL = &tmp
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

func (manager *GCSManager) DeleteObjects(locations []string) (err error) {
	return
}

func (manager *GCSManager) GetConfiguredPrefix() string {
	return manager.Config.Prefix
}
