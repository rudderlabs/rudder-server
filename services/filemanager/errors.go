package filemanager

import (
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/minio/minio-go/v7"
)

func suppressMinorErrors(err error) error {
	switch err := err.(type) {
	case azblob.StorageError:
		if err.ServiceCode() == azblob.ServiceCodeContainerAlreadyExists {
			pkgLogger.Debug("Received 409. Container already exists")
			return nil
		}
	case minio.ErrorResponse:
		if err.Code == "BucketAlreadyOwnedByYou" {
			pkgLogger.Debug("Received 409. Bucket already exists")
			return nil
		}
	}
	return err
}
