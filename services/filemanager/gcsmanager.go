package filemanager

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

func objectURL(objAttrs *storage.ObjectAttrs) string {
	return fmt.Sprintf("https://storage.googleapis.com/%s/%s", objAttrs.Bucket, objAttrs.Name)
}

func (manager *GCSManager) Upload(file *os.File, prefixes ...string) (UploadOutput, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsJSON([]byte(manager.Config.Credentials)))
	if err != nil {
		return UploadOutput{}, err
	}

	splitFileName := strings.Split(file.Name(), "/")
	fileName := ""
	if len(prefixes) > 0 {
		fileName = strings.Join(prefixes[:], "/") + "/"
	}
	fileName += splitFileName[len(splitFileName)-1]
	if manager.Config.Folder != "" {
		fileName = manager.Config.Folder + "/" + fileName
	}
	bh := client.Bucket(manager.Config.Bucket)
	obj := bh.Object(fileName)
	w := obj.NewWriter(ctx)
	if _, err := io.Copy(w, file); err != nil {
		return UploadOutput{}, err
	}
	if err := w.Close(); err != nil {
		return UploadOutput{}, err
	}

	attrs, err := obj.Attrs(ctx)
	return UploadOutput{Location: objectURL(attrs)}, err
}

func (manager *GCSManager) Download(output *os.File, key string) error {
	ctx := context.Background()

	client, err := storage.NewClient(ctx, option.WithCredentialsJSON([]byte(manager.Config.Credentials)))

	if err != nil {
		return err
	}

	rc, err := client.Bucket(manager.Config.Bucket).Object(key).NewReader(ctx)
	if err != nil {
		return err
	}
	defer rc.Close()

	_, err = io.Copy(output, rc)
	return err
}

type GCSManager struct {
	Config *GCSConfig
}

func GetGCSConfig(config map[string]interface{}) *GCSConfig {
	var bucketName, folderName, credentials string
	if config["bucketName"] != nil {
		bucketName = config["bucketName"].(string)
	}
	if config["folderName"] != nil {
		folderName = config["folderName"].(string)
	}
	if config["credentials"] != nil {
		credentials = config["credentials"].(string)
	}
	return &GCSConfig{Bucket: bucketName, Folder: folderName, Credentials: credentials}
}

type GCSConfig struct {
	Bucket      string
	Folder      string
	Credentials string
}
