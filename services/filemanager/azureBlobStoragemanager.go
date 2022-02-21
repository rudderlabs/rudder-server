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

func (manager *AzureBlobStorageManager) getBaseURL() *url.URL {
	protocol := "https"
	if manager.Config.DisableSSL != nil && *manager.Config.DisableSSL {
		protocol = "http"
	}

	endpoint := "blob.core.windows.net"
	if manager.Config.EndPoint != nil && *manager.Config.EndPoint != "" {
		endpoint = *manager.Config.EndPoint
	}

	baseURL := url.URL{
		Scheme: protocol,
		Host:   fmt.Sprintf("%s.%s", manager.Config.AccountName, endpoint),
		Path:   manager.Config.Container,
	}
	if manager.Config.ForcePathStyle != nil && *manager.Config.ForcePathStyle {
		baseURL.Host = endpoint
		baseURL.Path = fmt.Sprintf("/%s/%s/", manager.Config.AccountName, manager.Config.Container)
	}

	return &baseURL
}

func (manager *AzureBlobStorageManager) getContainerURLOld() (azblob.ContainerURL, error) {
	if manager.Config.Container == "" {
		return azblob.ContainerURL{}, errors.New("no container configured")
	}

	accountName, accountKey := manager.Config.AccountName, manager.Config.AccountKey
	if len(accountName) == 0 || len(accountKey) == 0 {
		return azblob.ContainerURL{}, errors.New("either the AccountName or AccountKey is not correct")
	}

	// Create a default request pipeline using your storage account name and account key.
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return azblob.ContainerURL{}, err
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// From the Azure portal, get your storage account blob service URL endpoint.
	baseURL := manager.getBaseURL()

	containerURL := azblob.NewContainerURL(*baseURL, p)

	return containerURL, nil
}

func (manager *AzureBlobStorageManager) getContainerURL() (azblob.ContainerURL, error) {
	if manager.Config.Container == "" {
		return azblob.ContainerURL{}, errors.New("no container configured")
	}

	accountName, accountKey := manager.Config.AccountName, manager.Config.AccountKey
	if len(accountName) == 0 || len(accountKey) == 0 {
		return azblob.ContainerURL{}, errors.New("either the AccountName or AccountKey is not correct")
	}

	// Create a default request pipeline using your storage account name and account key.
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return azblob.ContainerURL{}, err
	}

	// Create a request pipeline that is used to process HTTP(S) requests and responses. It requires
	// your account credentials. In more advanced scenarios, you can configure telemetry, retry policies,
	// logging, and other options. Also, you can configure multiple request pipelines for different scenarios.
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// From the Azure portal, get your Storage account blob service URL endpoint.
	// The URL typically looks like this:
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", accountName))

	// Create an ServiceURL object that wraps the service URL and a request pipeline.
	serviceURL := azblob.NewServiceURL(*u, p)

	// This example shows several common operations just to get you started.

	// Create a URL that references a to-be-created container in your Azure Storage account.
	// This returns a ContainerURL object that wraps the container's URL and a request pipeline (inherited from serviceURL)
	containerURL := serviceURL.NewContainerURL(manager.Config.Container) // Container names require lowercase

	return containerURL, nil
}

// Upload passed in file to Azure Blob Storage
func (manager *AzureBlobStorageManager) Upload(file *os.File, prefixes ...string) (UploadOutput, error) {
	ctx := context.Background()

	containerURL, err := manager.getContainerURL()
	if err != nil {
		return UploadOutput{}, err
	}

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

func (manager *AzureBlobStorageManager) ListFilesWithPrefix(prefix string, maxItems int64) (fileObjects []*FileObject, err error) {
	ctx := context.Background()

	containerURL, err := manager.getContainerURL()
	if err != nil {
		return []*FileObject{}, err
	}

	blobListingDetails := azblob.BlobListingDetails{
		Metadata: true,
	}
	segmentOptions := azblob.ListBlobsSegmentOptions{
		Details:    blobListingDetails,
		Prefix:     prefix,
		MaxResults: int32(maxItems),
	}

	// List the blobs in the container
	var marker string
	response, err := containerURL.ListBlobsFlatSegment(ctx, azblob.Marker{Val: &marker}, segmentOptions)
	if err != nil {
		return
	}

	fileObjects = make([]*FileObject, len(response.Segment.BlobItems))
	for idx := range response.Segment.BlobItems {
		fileObjects[idx] = &FileObject{response.Segment.BlobItems[idx].Name, response.Segment.BlobItems[idx].Properties.LastModified}
	}
	return
}

func (manager *AzureBlobStorageManager) Download(output *os.File, key string) error {
	ctx := context.Background()

	containerURL, err := manager.getContainerURL()
	if err != nil {
		return err
	}

	blobURL := containerURL.NewBlockBlobURL(key)

	// Here's how to download the blob
	downloadResponse, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
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
	baseURL := manager.getBaseURL()
	strToken := strings.Split(location, baseURL.Path)
	return strToken[len(strToken)-1], nil
}

func (manager *AzureBlobStorageManager) GetDownloadKeyFromFileLocation(location string) string {
	baseURL := manager.getBaseURL()
	str := strings.Split(location, baseURL.Path)
	return str[len(str)-1]
}

type AzureBlobStorageManager struct {
	Config *AzureBlobStorageConfig
}

func GetAzureBlogStorageConfig(config map[string]interface{}) *AzureBlobStorageConfig {
	var containerName, accountName, accountKey, prefix string
	var endPoint *string
	var forcePathStyle, disableSSL *bool
	if config["containerName"] != nil {
		tmp, ok := config["containerName"].(string)
		if ok {
			containerName = tmp
		}
	}
	if config["prefix"] != nil {
		tmp, ok := config["prefix"].(string)
		if ok {
			prefix = tmp
		}
	}
	if config["accountName"] != nil {
		tmp, ok := config["accountName"].(string)
		if ok {
			accountName = tmp
		}
	}
	if config["accountKey"] != nil {
		tmp, ok := config["accountKey"].(string)
		if ok {
			accountKey = tmp
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
	return &AzureBlobStorageConfig{
		Container:      containerName,
		Prefix:         prefix,
		AccountName:    accountName,
		AccountKey:     accountKey,
		EndPoint:       endPoint,
		ForcePathStyle: forcePathStyle,
		DisableSSL:     disableSSL,
	}
}

type AzureBlobStorageConfig struct {
	Container      string
	Prefix         string
	AccountName    string
	AccountKey     string
	EndPoint       *string
	ForcePathStyle *bool
	DisableSSL     *bool
}

func (manager *AzureBlobStorageManager) DeleteObjects(keys []string) (err error) {
	ctx := context.Background()

	containerURL, err := manager.getContainerURL()
	if err != nil {
		return err
	}
	for _, key := range keys {
		blobURL := containerURL.NewBlockBlobURL(key)
		_, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		if err != nil {
			return err
		}
	}
	return
}

func (manager *AzureBlobStorageManager) GetConfiguredPrefix() string {
	return manager.Config.Prefix
}
