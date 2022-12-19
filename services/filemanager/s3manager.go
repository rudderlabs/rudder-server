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
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	awsS3Manager "github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/mitchellh/mapstructure"
	appConfig "github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/awsutils"
)

// Upload passed in file to s3
func (manager *S3Manager) Upload(ctx context.Context, file *os.File, prefixes ...string) (UploadOutput, error) {
	fileName := path.Join(manager.Config.Prefix, path.Join(prefixes...), path.Base(file.Name()))

	uploadInput := &awsS3Manager.UploadInput{
		ACL:    aws.String("bucket-owner-full-control"),
		Bucket: aws.String(manager.Config.Bucket),
		Key:    aws.String(fileName),
		Body:   file,
	}
	if manager.Config.EnableSSE {
		uploadInput.ServerSideEncryption = aws.String("AES256")
	}

	uploadSession, err := manager.getSession(ctx)
	if err != nil {
		return UploadOutput{}, fmt.Errorf("error starting S3 session: %w", err)
	}
	s3manager := awsS3Manager.NewUploader(uploadSession)

	ctx, cancel := context.WithTimeout(ctx, manager.getTimeout())
	defer cancel()

	output, err := s3manager.UploadWithContext(ctx, uploadInput)
	if err != nil {
		if awsError, ok := err.(awserr.Error); ok && awsError.Code() == "MissingRegion" {
			err = fmt.Errorf(fmt.Sprintf(`Bucket '%s' not found.`, manager.Config.Bucket))
		}
		return UploadOutput{}, err
	}

	return UploadOutput{Location: output.Location, ObjectName: fileName}, err
}

func (manager *S3Manager) Download(ctx context.Context, output *os.File, key string) error {
	sess, err := manager.getSession(ctx)
	if err != nil {
		return fmt.Errorf("error starting S3 session: %w", err)
	}

	downloader := awsS3Manager.NewDownloader(sess)

	ctx, cancel := context.WithTimeout(ctx, manager.getTimeout())
	defer cancel()

	_, err = downloader.DownloadWithContext(ctx, output,
		&s3.GetObjectInput{
			Bucket: aws.String(manager.Config.Bucket),
			Key:    aws.String(key),
		})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == ErrKeyNotFound.Error() {
			return ErrKeyNotFound
		}
		return err
	}
	return nil
}

/*
GetObjectNameFromLocation gets the object name/key name from the object location url

	https://bucket-name.s3.amazonaws.com/key - >> key
*/
func (manager *S3Manager) GetObjectNameFromLocation(location string) (string, error) {
	parsedUrl, err := url.Parse(location)
	if err != nil {
		return "", err
	}
	trimedUrl := strings.TrimLeft(parsedUrl.Path, "/")
	if (manager.Config.S3ForcePathStyle != nil && *manager.Config.S3ForcePathStyle) || (!strings.Contains(parsedUrl.Host, manager.Config.Bucket)) {
		return strings.TrimPrefix(trimedUrl, fmt.Sprintf(`%s/`, manager.Config.Bucket)), nil
	}
	return trimedUrl, nil
}

func (manager *S3Manager) GetDownloadKeyFromFileLocation(location string) string {
	parsedURL, err := url.Parse(location)
	if err != nil {
		fmt.Println("error while parsing location url: ", err)
	}
	trimmedURL := strings.TrimLeft(parsedURL.Path, "/")
	if (manager.Config.S3ForcePathStyle != nil && *manager.Config.S3ForcePathStyle) || (!strings.Contains(parsedURL.Host, manager.Config.Bucket)) {
		return strings.TrimPrefix(trimmedURL, fmt.Sprintf(`%s/`, manager.Config.Bucket))
	}
	return trimmedURL
}

func (manager *S3Manager) DeleteObjects(ctx context.Context, keys []string) (err error) {
	sess, err := manager.getSession(ctx)
	if err != nil {
		return fmt.Errorf("error starting S3 session: %w", err)
	}

	var objects []*s3.ObjectIdentifier
	for _, key := range keys {
		objects = append(objects, &s3.ObjectIdentifier{Key: aws.String(key)})
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
		defer cancel()

		_, err := svc.DeleteObjectsWithContext(_ctx, input)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				pkgLogger.Errorf(`Error while deleting S3 objects: %v, error code: %v`, aerr.Error(), aerr.Code())
			} else {
				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
				pkgLogger.Errorf(`Error while deleting S3 objects: %v`, aerr.Error())
			}
			return err
		}
	}
	return nil
}

func (manager *S3Manager) getSession(ctx context.Context) (*session.Session, error) {
	if manager.session != nil {
		return manager.session, nil
	}

	if manager.Config.Bucket == "" {
		return nil, errors.New("no storage bucket configured to downloader")
	}
	if !manager.Config.UseGlue || manager.Config.Region == nil {
		getRegionSession, err := session.NewSession()
		if err != nil {
			return nil, err
		}

		ctx, cancel := context.WithTimeout(ctx, manager.getTimeout())
		defer cancel()

		region, err := awsS3Manager.GetBucketRegion(ctx, getRegionSession, manager.Config.Bucket, manager.Config.RegionHint)
		if err != nil {
			pkgLogger.Errorf("Failed to fetch AWS region for bucket %s. Error %v", manager.Config.Bucket, err)
			/// Failed to Get Region probably due to VPC restrictions, Will proceed to try with AccessKeyID and AccessKey
		}
		manager.Config.Region = aws.String(region)
		manager.SessionConfig.Region = region
	}

	var err error
	manager.session, err = awsutils.CreateSession(manager.SessionConfig)
	if err != nil {
		return nil, err
	}
	return manager.session, err
}

// IMPT NOTE: `ListFilesWithPrefix` support Continuation Token. So, if you want same set of files (says 1st 1000 again)
// then create a new S3Manager & not use the existing one. Since, using the existing one will by default return next 1000 files.
func (manager *S3Manager) ListFilesWithPrefix(ctx context.Context, startAfter, prefix string, maxItems int64) (fileObjects []*FileObject, err error) {
	if !manager.Config.IsTruncated {
		pkgLogger.Infof("Manager is truncated: %v so returning here", manager.Config.IsTruncated)
		return
	}
	fileObjects = make([]*FileObject, 0)

	sess, err := manager.getSession(ctx)
	if err != nil {
		return []*FileObject{}, fmt.Errorf("error starting S3 session: %w", err)
	}
	// Create S3 service client
	svc := s3.New(sess)
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

	ctx, cancel := context.WithTimeout(ctx, manager.getTimeout())
	defer cancel()

	// Get the list of items
	resp, err := svc.ListObjectsV2WithContext(ctx, &listObjectsV2Input)
	if err != nil {
		pkgLogger.Errorf("Error while listing S3 objects: %v", err)
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

func (manager *S3Manager) GetConfiguredPrefix() string {
	return manager.Config.Prefix
}

type S3Manager struct {
	Config        *S3Config
	SessionConfig *awsutils.SessionConfig
	session       *session.Session
	timeout       time.Duration
}

func (manager *S3Manager) SetTimeout(timeout time.Duration) {
	manager.timeout = timeout
}

func (manager *S3Manager) getTimeout() time.Duration {
	if manager.timeout > 0 {
		return manager.timeout
	}

	return getBatchRouterTimeoutConfig("S3")
}

func NewS3Manager(config map[string]interface{}) (*S3Manager, error) {
	var s3Config S3Config
	if err := mapstructure.Decode(config, &s3Config); err != nil {
		return nil, err
	}
	regionHint := appConfig.GetString("AWS_S3_REGION_HINT", "us-east-1")
	s3Config.RegionHint = regionHint
	s3Config.IsTruncated = true
	sessionConfig, err := awsutils.NewSimpleSessionConfig(config, s3.ServiceName)
	if err != nil {
		return nil, err
	}
	return &S3Manager{
		Config:        &s3Config,
		SessionConfig: sessionConfig,
	}, nil
}

type S3Config struct {
	Bucket            string  `mapstructure:"bucketName"`
	Prefix            string  `mapstructure:"Prefix"`
	Region            *string `mapstructure:"region"`
	Endpoint          *string `mapstructure:"endpoint"`
	S3ForcePathStyle  *bool   `mapstructure:"s3ForcePathStyle"`
	DisableSSL        *bool   `mapstructure:"disableSSL"`
	EnableSSE         bool    `mapstructure:"enableSSE"`
	RegionHint        string  `mapstructure:"regionHint"`
	ContinuationToken *string `mapstructure:"continuationToken"`
	IsTruncated       bool    `mapstructure:"isTruncated"`
	UseGlue           bool    `mapstructure:"useGlue"`
}
