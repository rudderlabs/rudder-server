package filemanager

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

type AzureBlobConfig struct {
	Container      string
	Prefix         string
	AccountName    string
	AccountKey     string
	SASToken       string
	EndPoint       *string
	ForcePathStyle *bool
	DisableSSL     *bool
	UseSASTokens   bool
}

// NewAzureBlobManager creates a new file manager for Azure Blob Storage
func NewAzureBlobManager(config map[string]interface{}, log logger.Logger, defaultTimeout func() time.Duration) (*azureBlobManager, error) {
	return &azureBlobManager{
		baseManager: &baseManager{
			logger:         log,
			defaultTimeout: defaultTimeout,
		},
		config: azureBlobConfig(config),
	}, nil
}

func (manager *azureBlobManager) ListFilesWithPrefix(ctx context.Context, startAfter, prefix string, maxItems int64) ListSession {
	return &azureBlobListSession{
		baseListSession: &baseListSession{
			ctx:        ctx,
			startAfter: startAfter,
			prefix:     prefix,
			maxItems:   maxItems,
		},
		manager: manager,
	}
}

// Upload passed in file to Azure Blob Storage
func (manager *azureBlobManager) Upload(ctx context.Context, file *os.File, prefixes ...string) (UploadedFile, error) {
	containerURL, err := manager.getContainerURL()
	if err != nil {
		return UploadedFile{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, manager.getTimeout())
	defer cancel()

	if manager.createContainer() {
		_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
		err = manager.suppressMinorErrors(err)
		if err != nil {
			return UploadedFile{}, err
		}
	}

	fileName := path.Join(manager.config.Prefix, path.Join(prefixes...), path.Base(file.Name()))

	// Here's how to upload a blob.
	blobURL := containerURL.NewBlockBlobURL(fileName)
	_, err = azblob.UploadFileToBlockBlob(ctx, file, blobURL, azblob.UploadToBlockBlobOptions{
		BlockSize:   4 * 1024 * 1024,
		Parallelism: 16,
	})
	if err != nil {
		return UploadedFile{}, err
	}

	return UploadedFile{Location: manager.blobLocation(&blobURL), ObjectName: fileName}, nil
}

func (manager *azureBlobManager) Download(ctx context.Context, output *os.File, key string) error {
	containerURL, err := manager.getContainerURL()
	if err != nil {
		return err
	}

	blobURL := containerURL.NewBlockBlobURL(key)

	ctx, cancel := context.WithTimeout(ctx, manager.getTimeout())
	defer cancel()

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

func (manager *azureBlobManager) Delete(ctx context.Context, keys []string) (err error) {
	containerURL, err := manager.getContainerURL()
	if err != nil {
		return err
	}

	for _, key := range keys {
		blobURL := containerURL.NewBlockBlobURL(key)

		_ctx, cancel := context.WithTimeout(ctx, manager.getTimeout())
		_, err := blobURL.Delete(_ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		if err != nil {
			cancel()
			return err
		}
		cancel()
	}
	return
}

func (manager *azureBlobManager) Prefix() string {
	return manager.config.Prefix
}

func (manager *azureBlobManager) GetObjectNameFromLocation(location string) (string, error) {
	strToken := strings.Split(location, fmt.Sprintf("%s/", manager.config.Container))
	return strToken[len(strToken)-1], nil
}

func (manager *azureBlobManager) GetDownloadKeyFromFileLocation(location string) string {
	str := strings.Split(location, fmt.Sprintf("%s/", manager.config.Container))
	return str[len(str)-1]
}

func (manager *azureBlobManager) suppressMinorErrors(err error) error {
	if err != nil {
		if storageError, ok := err.(azblob.StorageError); ok { // This error is a Service-specific
			switch storageError.ServiceCode() { // Compare serviceCode to ServiceCodeXxx constants
			case azblob.ServiceCodeContainerAlreadyExists:
				manager.logger.Debug("Received 409. Container already exists")
				return nil
			}
		}
	}
	return err
}

func (manager *azureBlobManager) getBaseURL() *url.URL {
	protocol := "https"
	if manager.config.DisableSSL != nil && *manager.config.DisableSSL {
		protocol = "http"
	}

	endpoint := "blob.core.windows.net"
	if manager.config.EndPoint != nil && *manager.config.EndPoint != "" {
		endpoint = *manager.config.EndPoint
	}

	baseURL := url.URL{
		Scheme: protocol,
		Host:   fmt.Sprintf("%s.%s", manager.config.AccountName, endpoint),
	}

	if manager.config.UseSASTokens {
		baseURL.RawQuery = manager.config.SASToken
	}

	if manager.config.ForcePathStyle != nil && *manager.config.ForcePathStyle {
		baseURL.Host = endpoint
		baseURL.Path = fmt.Sprintf("/%s/", manager.config.AccountName)
	}

	return &baseURL
}

func (manager *azureBlobManager) getContainerURL() (azblob.ContainerURL, error) {
	if manager.config.Container == "" {
		return azblob.ContainerURL{}, errors.New("no container configured")
	}

	credential, err := manager.getCredentials()
	if err != nil {
		return azblob.ContainerURL{}, err
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// From the Azure portal, get your storage account blob service URL endpoint.
	baseURL := manager.getBaseURL()
	serviceURL := azblob.NewServiceURL(*baseURL, p)
	containerURL := serviceURL.NewContainerURL(manager.config.Container)

	return containerURL, nil
}

func (manager *azureBlobManager) getCredentials() (azblob.Credential, error) {
	if manager.config.UseSASTokens {
		return azblob.NewAnonymousCredential(), nil
	}

	accountName, accountKey := manager.config.AccountName, manager.config.AccountKey
	if accountName == "" || accountKey == "" {
		return nil, errors.New("either accountName or accountKey is empty")
	}

	// Create a default request pipeline using your storage account name and account key.
	return azblob.NewSharedKeyCredential(accountName, accountKey)
}

func (manager *azureBlobManager) createContainer() bool {
	return !manager.config.UseSASTokens
}

func (manager *azureBlobManager) blobLocation(blobURL *azblob.BlockBlobURL) string {
	if !manager.config.UseSASTokens {
		return blobURL.String()
	}

	// Reset SAS Query parameters
	blobURLParts := azblob.NewBlobURLParts(blobURL.URL())
	blobURLParts.SAS = azblob.SASQueryParameters{}
	newBlobURL := blobURLParts.URL()
	return newBlobURL.String()
}

type azureBlobManager struct {
	*baseManager
	config *AzureBlobConfig
}

func azureBlobConfig(config map[string]interface{}) *AzureBlobConfig {
	var containerName, accountName, accountKey, sasToken, prefix string
	var endPoint *string
	var forcePathStyle, disableSSL *bool
	var useSASTokens bool
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
	if config["useSASTokens"] != nil {
		tmp, ok := config["useSASTokens"].(bool)
		if ok {
			useSASTokens = tmp
		}
	}
	if config["sasToken"] != nil {
		tmp, ok := config["sasToken"].(string)
		if ok {
			sasToken = strings.TrimPrefix(tmp, "?")
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
	return &AzureBlobConfig{
		Container:      containerName,
		Prefix:         prefix,
		AccountName:    accountName,
		AccountKey:     accountKey,
		UseSASTokens:   useSASTokens,
		SASToken:       sasToken,
		EndPoint:       endPoint,
		ForcePathStyle: forcePathStyle,
		DisableSSL:     disableSSL,
	}
}

type azureBlobListSession struct {
	*baseListSession
	manager *azureBlobManager

	Marker azblob.Marker
}

func (l *azureBlobListSession) Next() (fileObjects []*FileInfo, err error) {
	manager := l.manager
	maxItems := l.maxItems

	containerURL, err := manager.getContainerURL()
	if err != nil {
		return []*FileInfo{}, err
	}

	blobListingDetails := azblob.BlobListingDetails{
		Metadata: true,
	}
	segmentOptions := azblob.ListBlobsSegmentOptions{
		Details:    blobListingDetails,
		Prefix:     l.prefix,
		MaxResults: int32(l.maxItems),
	}

	ctx, cancel := context.WithTimeout(l.ctx, manager.getTimeout())
	defer cancel()

	// List the blobs in the container
	var response *azblob.ListBlobsFlatSegmentResponse

	// Checking if maxItems > 0 to avoid function calls which expect only maxItems to be returned and not more in the code
	for maxItems > 0 && l.Marker.NotDone() {
		response, err = containerURL.ListBlobsFlatSegment(ctx, l.Marker, segmentOptions)
		if err != nil {
			return
		}
		l.Marker = response.NextMarker

		fileObjects = make([]*FileInfo, 0)
		for idx := range response.Segment.BlobItems {
			if strings.Compare(response.Segment.BlobItems[idx].Name, l.startAfter) > 0 {
				fileObjects = append(fileObjects, &FileInfo{response.Segment.BlobItems[idx].Name, response.Segment.BlobItems[idx].Properties.LastModified})
				maxItems--
			}
		}
	}
	return
}
