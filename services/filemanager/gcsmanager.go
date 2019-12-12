package filemanager

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func objectURL(objAttrs *storage.ObjectAttrs) string {
	return fmt.Sprintf("https://storage.googleapis.com/%s/%s", objAttrs.Bucket, objAttrs.Name)
}

func (manager *GCSManager) Upload(file *os.File, prefixes ...string) (UploadOutput, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		misc.AssertError(err)
	}

	splitFileName := strings.Split(file.Name(), "/")
	fileName := ""
	if len(prefixes) > 0 {
		fileName = strings.Join(prefixes[:], "/") + "/"
	}
	fileName += splitFileName[len(splitFileName)-1]

	bh := client.Bucket(manager.Bucket)
	obj := bh.Object(fileName)
	w := obj.NewWriter(ctx)
	if _, err := io.Copy(w, file); err != nil {
		misc.AssertError(err)
		return UploadOutput{}, err
	}
	if err := w.Close(); err != nil {
		misc.AssertError(err)
		return UploadOutput{}, err
	}

	attrs, err := obj.Attrs(ctx)
	return UploadOutput{Location: objectURL(attrs)}, err
}

func (manager *GCSManager) Download(output *os.File, key string) error {
	return nil
}

type GCSManager struct {
	Bucket string
}
