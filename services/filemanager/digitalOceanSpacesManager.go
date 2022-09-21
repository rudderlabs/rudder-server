package filemanager

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	SpacesManager "github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func (manager *DOSpacesManager) getSession() (*session.Session, error) {
	var region string
	if manager.Config.Region != nil {
		region = *manager.Config.Region
	} else {
		region = misc.GetSpacesLocation(manager.Config.EndPoint)
	}
	return session.NewSession(&aws.Config{
		Region:           aws.String(region),
		Credentials:      credentials.NewStaticCredentials(manager.Config.AccessKeyID, manager.Config.AccessKey, ""),
		Endpoint:         aws.String(manager.Config.EndPoint),
		DisableSSL:       manager.Config.DisableSSL,
		S3ForcePathStyle: manager.Config.ForcePathStyle,
	})
}

// Upload passed in file to spaces
func (manager *DOSpacesManager) Upload(ctx context.Context, file *os.File, prefixes ...string) (UploadOutput, error) {
	if manager.Config.Bucket == "" {
		return UploadOutput{}, errors.New("no storage bucket configured to uploader")
	}

	fileName := path.Join(manager.Config.Prefix, path.Join(prefixes...), path.Base(file.Name()))

	uploadInput := &SpacesManager.UploadInput{
		ACL:    aws.String("bucket-owner-full-control"),
		Bucket: aws.String(manager.Config.Bucket),
		Key:    aws.String(fileName),
		Body:   file,
	}
	uploadSession, err := manager.getSession()
	if err != nil {
		return UploadOutput{}, fmt.Errorf("error starting Digital Ocean Spaces session: %w", err)
	}
	DOmanager := SpacesManager.NewUploader(uploadSession)

	ctx, cancel := context.WithTimeout(ctx, manager.getTimeout())
	defer cancel()

	output, err := DOmanager.UploadWithContext(ctx, uploadInput)
	if err != nil {
		if awsError, ok := err.(awserr.Error); ok && awsError.Code() == "MissingRegion" {
			err = fmt.Errorf(fmt.Sprintf(`Bucket '%s' not found.`, manager.Config.Bucket))
		}
		return UploadOutput{}, err
	}

	return UploadOutput{Location: output.Location, ObjectName: fileName}, err
}

func (manager *DOSpacesManager) Download(ctx context.Context, output *os.File, key string) error {
	downloadSession, err := manager.getSession()
	if err != nil {
		return fmt.Errorf("error starting Digital Ocean Spaces session: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, manager.getTimeout())
	defer cancel()

	downloader := SpacesManager.NewDownloader(downloadSession)
	_, err = downloader.DownloadWithContext(ctx, output,
		&s3.GetObjectInput{
			Bucket: aws.String(manager.Config.Bucket),
			Key:    aws.String(key),
		})

	return err
}

func (manager *DOSpacesManager) GetDownloadKeyFromFileLocation(location string) string {
	parsedUrl, err := url.Parse(location)
	if err != nil {
		fmt.Println("error while parsing location url: ", err)
	}
	trimedUrl := strings.TrimLeft(parsedUrl.Path, "/")
	if (manager.Config.ForcePathStyle != nil && *manager.Config.ForcePathStyle) || (!strings.Contains(parsedUrl.Host, manager.Config.Bucket)) {
		return strings.TrimPrefix(trimedUrl, fmt.Sprintf(`%s/`, manager.Config.Bucket))
	}
	return trimedUrl
}

/*
GetObjectNameFromLocation gets the object name/key name from the object location url

	https://rudder.sgp1.digitaloceanspaces.com/key - >> key
*/
func (manager *DOSpacesManager) GetObjectNameFromLocation(location string) (string, error) {
	parsedURL, err := url.Parse(location)
	if err != nil {
		return "", err
	}
	trimedUrl := strings.TrimLeft(parsedURL.Path, "/")
	if (manager.Config.ForcePathStyle != nil && *manager.Config.ForcePathStyle) || (!strings.Contains(parsedURL.Host, manager.Config.Bucket)) {
		return strings.TrimPrefix(trimedUrl, fmt.Sprintf(`%s/`, manager.Config.Bucket)), nil
	}
	return trimedUrl, nil
}

func (manager *DOSpacesManager) ListFilesWithPrefix(ctx context.Context, startAfter, prefix string, maxItems int64) (fileObjects []*FileObject, err error) {
	if !manager.Config.IsTruncated {
		pkgLogger.Infof("Manager is truncated: %v so returning here", manager.Config.IsTruncated)
		return
	}
	fileObjects = make([]*FileObject, 0)

	sess, err := manager.getSession()
	if err != nil {
		return []*FileObject{}, fmt.Errorf("error starting Digital Ocean Spaces session: %w", err)
	}

	// Create S3 service client
	svc := s3.New(sess)

	ctx, cancel := context.WithTimeout(ctx, manager.getTimeout())
	defer cancel()

	listObjectsV2Input := s3.ListObjectsV2Input{
		Bucket:  aws.String(manager.Config.Bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: &maxItems,
		// Delimiter: aws.String("/"),
	}
	// startAfter is to resume a paused task.
	if startAfter != "" {
		listObjectsV2Input.StartAfter = aws.String(startAfter)
	}
	if manager.Config.ContinuationToken != nil {
		listObjectsV2Input.ContinuationToken = manager.Config.ContinuationToken
	}

	// Get the list of items
	resp, err := svc.ListObjectsV2WithContext(ctx, &listObjectsV2Input)
	if err != nil {
		pkgLogger.Errorf("Error while listing Digital Ocean Spaces objects: %v", err)
		return
	}
	if resp.IsTruncated != nil {
		manager.Config.IsTruncated = *resp.IsTruncated
	}
	manager.Config.ContinuationToken = resp.NextContinuationToken
	for _, item := range resp.Contents {
		fileObjects = append(fileObjects, &FileObject{*item.Key, *item.LastModified})
	}
	return
}

func (manager *DOSpacesManager) DeleteObjects(ctx context.Context, keys []string) error {
	sess, err := manager.getSession()
	if err != nil {
		return fmt.Errorf("error starting Digital Ocean Spaces session: %w", err)
	}

	objects := make([]*s3.ObjectIdentifier, len(keys))
	for i, key := range keys {
		objects[i] = &s3.ObjectIdentifier{Key: aws.String(key)}
	}

	svc := s3.New(sess)

	batchSize := 1000 // max accepted by DeleteObjects API
	for i := 0; i < len(objects); i += batchSize {
		j := i + batchSize
		if j > len(objects) {
			j = len(objects)
		}
		input := &s3.DeleteObjectsInput{
			Bucket: aws.String(manager.Config.Bucket),
			Delete: &s3.Delete{
				Objects: objects[i:j],
			},
		}

		_ctx, cancel := context.WithTimeout(ctx, manager.getTimeout())
		_, err := svc.DeleteObjectsWithContext(_ctx, input)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				pkgLogger.Errorf(`Error while deleting digital ocean spaces objects: %v, error code: %v`, aerr.Error(), aerr.Code())
			} else {
				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
				pkgLogger.Errorf(`Error while deleting digital ocean spaces objects: %v`, aerr.Error())
			}
			cancel()
			return err
		}
		cancel()
	}
	return nil
}

type DOSpacesManager struct {
	Config  *DOSpacesConfig
	timeout time.Duration
}

func (manager *DOSpacesManager) SetTimeout(timeout time.Duration) {
	manager.timeout = timeout
}

func (manager *DOSpacesManager) getTimeout() time.Duration {
	if manager.timeout > 0 {
		return manager.timeout
	}

	return getBatchRouterTimeoutConfig("DIGITAL_OCEAN_SPACES")
}

func GetDOSpacesConfig(config map[string]interface{}) *DOSpacesConfig {
	var bucketName, prefix, endPoint, accessKeyID, accessKey string
	var continuationToken *string
	var region *string
	var forcePathStyle, disableSSL *bool
	if config["bucketName"] != nil {
		tmp, ok := config["bucketName"].(string)
		if ok {
			bucketName = tmp
		}
	}
	if config["prefix"] != nil {
		tmp, ok := config["prefix"].(string)
		if ok {
			prefix = tmp
		}
	}
	if config["endPoint"] != nil {
		tmp, ok := config["endPoint"].(string)
		if ok {
			endPoint = tmp
		}
	}
	if config["accessKeyID"] != nil {
		tmp, ok := config["accessKeyID"].(string)
		if ok {
			accessKeyID = tmp
		}
	}
	if config["accessKey"] != nil {
		tmp, ok := config["accessKey"].(string)
		if ok {
			accessKey = tmp
		}
	}
	if config["region"] != nil {
		tmp, ok := config["region"].(string)
		if ok {
			region = &tmp
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
	return &DOSpacesConfig{
		Bucket:            bucketName,
		EndPoint:          endPoint,
		Prefix:            prefix,
		AccessKeyID:       accessKeyID,
		AccessKey:         accessKey,
		Region:            region,
		ForcePathStyle:    forcePathStyle,
		DisableSSL:        disableSSL,
		ContinuationToken: continuationToken,
		IsTruncated:       true,
	}
}

type DOSpacesConfig struct {
	Bucket            string
	Prefix            string
	EndPoint          string
	AccessKeyID       string
	AccessKey         string
	Region            *string
	ForcePathStyle    *bool
	DisableSSL        *bool
	ContinuationToken *string
	IsTruncated       bool
}

func (manager *DOSpacesManager) GetConfiguredPrefix() string {
	return manager.Config.Prefix
}
