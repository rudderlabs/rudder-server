package filemanager

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

func supressMinorErrors(err error) error {
	if err != nil {
		if serr, ok := err.(azblob.StorageError); ok { // This error is a Service-specific
			switch serr.ServiceCode() { // Compare serviceCode to ServiceCodeXxx constants
			case azblob.ServiceCodeContainerAlreadyExists:
				logger.Debug("Received 409. Container already exists")
				return nil
			}
		}
	}
	return err
}

// Upload passed in file to Azure Blob Storage
func (manager *AzureBlobStorageManager) Upload(file *os.File, prefixes ...string) (UploadOutput, error) {
	if manager.Config.Container == "" {
		return UploadOutput{}, errors.New("no container configured to uploader")
	}
	accountName, accountKey := config.GetEnv("AZURE_STORAGE_ACCOUNT", ""), config.GetEnv("AZURE_STORAGE_ACCESS_KEY", "")
	if len(accountName) == 0 || len(accountKey) == 0 {
		return UploadOutput{}, errors.New("Either the AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_ACCESS_KEY environment variable is not set")
	}

	// Create a default request pipeline using your storage account name and account key.
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return UploadOutput{}, err
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// From the Azure portal, get your storage account blob service URL endpoint.
	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, manager.Config.Container))

	// Create a ContainerURL object that wraps the container URL and a request
	// pipeline to make requests.
	containerURL := azblob.NewContainerURL(*URL, p)
	ctx := context.Background()
	_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	err = supressMinorErrors(err)

	if err != nil {
		return UploadOutput{}, err
	}

	splitFileName := strings.Split(file.Name(), "/")
	fileName := ""
	if len(prefixes) > 0 {
		fileName = strings.Join(prefixes[:], "/") + "/"
	}
	fileName += splitFileName[len(splitFileName)-1]
	// Here's how to upload a blob.
	blobURL := containerURL.NewBlockBlobURL(fileName)

	_, err = azblob.UploadFileToBlockBlob(ctx, file, blobURL, azblob.UploadToBlockBlobOptions{
		BlockSize:   4 * 1024 * 1024,
		Parallelism: 16})

	if err != nil {
		return UploadOutput{}, err
	}

	return UploadOutput{Location: fileName}, nil
}

func (manager *AzureBlobStorageManager) Download(output *os.File, key string) error {
	if manager.Config.Container == "" {
		return errors.New("no container configured to downloader")
	}
	accountName, accountKey := config.GetEnv("AZURE_STORAGE_ACCOUNT", ""), config.GetEnv("AZURE_STORAGE_ACCESS_KEY", "")
	if len(accountName) == 0 || len(accountKey) == 0 {
		return errors.New("Either the AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_ACCESS_KEY environment variable is not set")
	}

	// Create a default request pipeline using your storage account name and account key.
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return err
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// From the Azure portal, get your storage account blob service URL endpoint.
	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, manager.Config.Container))

	containerURL := azblob.NewContainerURL(*URL, p)

	blobURL := containerURL.NewBlockBlobURL(key)
	ctx := context.Background()
	// Here's how to download the blob
	downloadResponse, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return err
	}

	// NOTE: automatically retries are performed if the connection fails
	bodyStream := downloadResponse.Body(azblob.RetryReaderOptions{MaxRetryRequests: 20})

	// read the body into a buffer
	downloadedData := bytes.Buffer{}
	_, err = downloadedData.ReadFrom(bodyStream)
	if err != nil {
		return err
	}

	_, err = output.Write(downloadedData.Bytes())
	return err
}

type AzureBlobStorageManager struct {
	Config *AzureBlobStorageConfig
}

func GetAzureBlogStorageConfig(config map[string]interface{}) *AzureBlobStorageConfig {
	return &AzureBlobStorageConfig{Container: config["containerName"].(string)}
}

type AzureBlobStorageConfig struct {
	Container string
}
