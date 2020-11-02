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
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var pkgLogger logger.LoggerI

func init() {
	pkgLogger = logger.NewLogger().Child("filemanager").Child("azureBlobStorage")
}

func supressMinorErrors(err error) error {
	if err != nil {
		if serr, ok := err.(azblob.StorageError); ok { // This error is a Service-specific
			switch serr.ServiceCode() { // Compare serviceCode to ServiceCodeXxx constants
			case azblob.ServiceCodeContainerAlreadyExists:
				pkgLogger.Debug("Received 409. Container already exists")
				return nil
			}
		}
	}
	return err
}

func (manager *AzureBlobStorageManager) getContainerURL() (azblob.ContainerURL, error) {
	if manager.Config.Container == "" {
		return azblob.ContainerURL{}, errors.New("no container configured")
	}

	accountName, accountKey := manager.Config.AccountName, manager.Config.AccountKey
	if len(accountName) == 0 || len(accountKey) == 0 {
		return azblob.ContainerURL{}, errors.New("Either the AccountName or AccountKey is not correct")
	}

	// Create a default request pipeline using your storage account name and account key.
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return azblob.ContainerURL{}, err
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// From the Azure portal, get your storage account blob service URL endpoint.
	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, manager.Config.Container))

	containerURL := azblob.NewContainerURL(*URL, p)

	return containerURL, nil
}

// Upload passed in file to Azure Blob Storage
func (manager *AzureBlobStorageManager) Upload(file *os.File, prefixes ...string) (UploadOutput, error) {
	containerURL, err := manager.getContainerURL()
	if err != nil {
		return UploadOutput{}, err
	}

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
	if manager.Config.Prefix != "" {
		if manager.Config.Prefix[len(manager.Config.Prefix)-1:] == "/" {
			fileName = manager.Config.Prefix + fileName
		} else {
			fileName = manager.Config.Prefix + "/" + fileName
		}
	}
	// Here's how to upload a blob.
	blobURL := containerURL.NewBlockBlobURL(fileName)

	_, err = azblob.UploadFileToBlockBlob(ctx, file, blobURL, azblob.UploadToBlockBlobOptions{
		BlockSize:   4 * 1024 * 1024,
		Parallelism: 16})

	if err != nil {
		return UploadOutput{}, err
	}

	return UploadOutput{Location: blobURL.String(), ObjectName: fileName}, nil
}

func (manager *AzureBlobStorageManager) Download(output *os.File, key string) error {
	containerURL, err := manager.getContainerURL()
	if err != nil {
		return err
	}

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

/*
GetObjectNameFromLocation gets the object name/key name from the object location url
	https://account-name.blob.core.windows.net/container-name/key - >> key
*/
func (manager *AzureBlobStorageManager) GetObjectNameFromLocation(location string) (string, error) {
	var baseURL string
	baseURL += "https://"
	baseURL += manager.Config.AccountName + ".blob.core.windows.net/"
	baseURL += manager.Config.Container + "/"
	return location[len(baseURL):], nil
}

//TODO complete this
func (manager *AzureBlobStorageManager) GetDownloadKeyFromFileLocation(location string) string {
	return location
}

type AzureBlobStorageManager struct {
	Config *AzureBlobStorageConfig
}

func GetAzureBlogStorageConfig(config map[string]interface{}) *AzureBlobStorageConfig {
	var containerName, accountName, accountKey, prefix string
	if config["containerName"] != nil {
		containerName = config["containerName"].(string)
	}
	if config["prefix"] != nil {
		prefix = config["prefix"].(string)
	}
	if config["accountName"] != nil {
		accountName = config["accountName"].(string)
	}
	if config["accountKey"] != nil {
		accountKey = config["accountKey"].(string)
	}
	return &AzureBlobStorageConfig{
		Container:   containerName,
		Prefix:      prefix,
		AccountName: accountName,
		AccountKey:  accountKey,
	}
}

type AzureBlobStorageConfig struct {
	Container   string
	Prefix      string
	AccountName string
	AccountKey  string
}
