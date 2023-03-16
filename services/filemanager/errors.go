package filemanager

import (
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/minio/minio-go/v7"
)

func suppressMinorErrors(err error) error {
	if err != nil {
		if azError, ok := err.(azblob.StorageError); ok { // This error is a Service-specific
			switch azError.ServiceCode() { // Compare serviceCode to ServiceCodeXxx constants
			case azblob.ServiceCodeContainerAlreadyExists:
				pkgLogger.Debug("Received 409. Container already exists")
				return nil
			}
		}
		if minioError, ok := err.(minio.ErrorResponse); ok {
			switch minioError.Code {
			case "BucketAlreadyOwnedByYou":
				pkgLogger.Debug("Received 409. Bucket already exists")
				return nil
			}
		}
	}
	return err
}
