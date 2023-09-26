package filemanager

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/logger"

	SpacesManager "github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type DigitalOceanConfig struct {
	Bucket         string
	Prefix         string
	EndPoint       string
	AccessKeyID    string
	AccessKey      string
	Region         *string
	ForcePathStyle *bool
	DisableSSL     *bool
}

// NewDigitalOceanManager creates a new file manager for digital ocean spaces
func NewDigitalOceanManager(config map[string]interface{}, log logger.Logger, defaultTimeout func() time.Duration) (*digitalOceanManager, error) {
	return &digitalOceanManager{
		baseManager: &baseManager{
			logger:         log,
			defaultTimeout: defaultTimeout,
		},
		Config: digitalOceanConfig(config),
	}, nil
}

func (manager *digitalOceanManager) ListFilesWithPrefix(ctx context.Context, startAfter, prefix string, maxItems int64) ListSession {
	return &digitalOceanListSession{
		baseListSession: &baseListSession{
			ctx:        ctx,
			startAfter: startAfter,
			prefix:     prefix,
			maxItems:   maxItems,
		},
		manager:     manager,
		isTruncated: true,
	}
}

func (manager *digitalOceanManager) Download(ctx context.Context, output *os.File, key string) error {
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

func (manager *digitalOceanManager) Upload(ctx context.Context, file *os.File, prefixes ...string) (UploadedFile, error) {
	if manager.Config.Bucket == "" {
		return UploadedFile{}, errors.New("no storage bucket configured to uploader")
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
		return UploadedFile{}, fmt.Errorf("error starting Digital Ocean Spaces session: %w", err)
	}
	DOmanager := SpacesManager.NewUploader(uploadSession)

	ctx, cancel := context.WithTimeout(ctx, manager.getTimeout())
	defer cancel()

	output, err := DOmanager.UploadWithContext(ctx, uploadInput)
	if err != nil {
		if awsError, ok := err.(awserr.Error); ok && awsError.Code() == "MissingRegion" {
			err = fmt.Errorf(fmt.Sprintf(`Bucket '%s' not found.`, manager.Config.Bucket))
		}
		return UploadedFile{}, err
	}

	return UploadedFile{Location: output.Location, ObjectName: fileName}, err
}

func (manager *digitalOceanManager) Delete(ctx context.Context, keys []string) error {
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
	chunks := lo.Chunk(objects, batchSize)
	for _, chunk := range chunks {
		input := &s3.DeleteObjectsInput{
			Bucket: aws.String(manager.Config.Bucket),
			Delete: &s3.Delete{
				Objects: chunk,
			},
		}

		_ctx, cancel := context.WithTimeout(ctx, manager.getTimeout())
		_, err := svc.DeleteObjectsWithContext(_ctx, input)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				manager.logger.Errorf(`Error while deleting digital ocean spaces objects: %v, error code: %v`, aerr.Error(), aerr.Code())
			} else {
				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
				manager.logger.Errorf(`Error while deleting digital ocean spaces objects: %v`, aerr.Error())
			}
			cancel()
			return err
		}
		cancel()
	}
	return nil
}

func (manager *digitalOceanManager) Prefix() string {
	return manager.Config.Prefix
}

func (manager *digitalOceanManager) GetDownloadKeyFromFileLocation(location string) string {
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
func (manager *digitalOceanManager) GetObjectNameFromLocation(location string) (string, error) {
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

func (manager *digitalOceanManager) getSession() (*session.Session, error) {
	var region string
	if manager.Config.Region != nil {
		region = *manager.Config.Region
	} else {
		region = getSpacesLocation(manager.Config.EndPoint)
	}
	return session.NewSession(&aws.Config{
		Region:           aws.String(region),
		Credentials:      credentials.NewStaticCredentials(manager.Config.AccessKeyID, manager.Config.AccessKey, ""),
		Endpoint:         aws.String(manager.Config.EndPoint),
		DisableSSL:       manager.Config.DisableSSL,
		S3ForcePathStyle: manager.Config.ForcePathStyle,
	})
}

func getSpacesLocation(location string) (region string) {
	r, _ := regexp.Compile(`\.*.*\.digitaloceanspaces\.com`) // skipcq: GO-S1009
	subLocation := r.FindString(location)
	regionTokens := strings.Split(subLocation, ".")
	if len(regionTokens) == 3 {
		region = regionTokens[0]
	}
	return region
}

type digitalOceanManager struct {
	*baseManager
	Config *DigitalOceanConfig
}

func digitalOceanConfig(config map[string]interface{}) *DigitalOceanConfig {
	var bucketName, prefix, endPoint, accessKeyID, accessKey string
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
	return &DigitalOceanConfig{
		Bucket:         bucketName,
		EndPoint:       endPoint,
		Prefix:         prefix,
		AccessKeyID:    accessKeyID,
		AccessKey:      accessKey,
		Region:         region,
		ForcePathStyle: forcePathStyle,
		DisableSSL:     disableSSL,
	}
}

type digitalOceanListSession struct {
	*baseListSession
	manager *digitalOceanManager

	continuationToken *string
	isTruncated       bool
}

func (l *digitalOceanListSession) Next() (fileObjects []*FileInfo, err error) {
	manager := l.manager
	if !l.isTruncated {
		manager.logger.Infof("Manager is truncated: %v so returning here", l.isTruncated)
		return
	}
	fileObjects = make([]*FileInfo, 0)

	sess, err := manager.getSession()
	if err != nil {
		return []*FileInfo{}, fmt.Errorf("error starting Digital Ocean Spaces session: %w", err)
	}

	// Create S3 service client
	svc := s3.New(sess)

	ctx, cancel := context.WithTimeout(l.ctx, manager.getTimeout())
	defer cancel()

	listObjectsV2Input := s3.ListObjectsV2Input{
		Bucket:  aws.String(manager.Config.Bucket),
		Prefix:  aws.String(l.prefix),
		MaxKeys: &l.maxItems,
	}
	// startAfter is to resume a paused task.
	if l.startAfter != "" {
		listObjectsV2Input.StartAfter = aws.String(l.startAfter)
	}
	if l.continuationToken != nil {
		listObjectsV2Input.ContinuationToken = l.continuationToken
	}

	// Get the list of items
	resp, err := svc.ListObjectsV2WithContext(ctx, &listObjectsV2Input)
	if err != nil {
		manager.logger.Errorf("Error while listing Digital Ocean Spaces objects: %v", err)
		return
	}
	if resp.IsTruncated != nil {
		l.isTruncated = *resp.IsTruncated
	}
	l.continuationToken = resp.NextContinuationToken
	for _, item := range resp.Contents {
		fileObjects = append(fileObjects, &FileInfo{*item.Key, *item.LastModified})
	}
	return
}
