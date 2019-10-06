package fileuploader

import (
	"github.com/minio/minio-go/v6"
	"github.com/rudderlabs/rudder-server/config"
	"os"
	"path/filepath"
	"strings"
)

// Upload passed in file to Object Storage
func (uploader *MinIOUploader) Upload(file *os.File, prefixes ...string) error {

	// Initialize minio client object.
	minioClient, err := minio.New(uploader.endpoint, uploader.accessKeyID, uploader.secretAccessKey, uploader.useSSL)
	if err != nil {
		return err
	}

	exists, errBucketExists := minioClient.BucketExists(uploader.bucket)
	if nil != errBucketExists {
		return errBucketExists
	}

	if !exists {
		err = minioClient.MakeBucket(uploader.bucket, config.GetEnv("MINIO_LOCATION", ""))
		if err != nil {
			return err
		}
	}

	var objectName string
	if len(prefixes) > 0 {
		objectName = strings.Join(prefixes[:], "/") + "/"
	}
	objectName += filepath.Base(file.Name())

	// Upload the file with FPutObject
	_, putErr := minioClient.FPutObject(
		uploader.bucket,
		objectName,
		file.Name(),
		minio.PutObjectOptions{},
	)

	if putErr != nil {
		return putErr
	}

	return nil
}

// MinIOUploader contains config for uploading object min.io Object Storage
type MinIOUploader struct {
	bucket          string
	endpoint        string
	accessKeyID     string
	secretAccessKey string
	useSSL          bool
}
