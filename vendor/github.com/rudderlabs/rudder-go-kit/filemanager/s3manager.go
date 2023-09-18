package filemanager

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	awsS3Manager "github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/mitchellh/mapstructure"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/awsutil"
	appConfig "github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

type S3Config struct {
	Bucket           string  `mapstructure:"bucketName"`
	Prefix           string  `mapstructure:"Prefix"`
	Region           *string `mapstructure:"region"`
	Endpoint         *string `mapstructure:"endpoint"`
	S3ForcePathStyle *bool   `mapstructure:"s3ForcePathStyle"`
	DisableSSL       *bool   `mapstructure:"disableSSL"`
	EnableSSE        bool    `mapstructure:"enableSSE"`
	RegionHint       string  `mapstructure:"regionHint"`
	UseGlue          bool    `mapstructure:"useGlue"`
}

// NewS3Manager creates a new file manager for S3
func NewS3Manager(
	config map[string]interface{}, log logger.Logger, defaultTimeout func() time.Duration,
) (*S3Manager, error) {
	var s3Config S3Config
	if err := mapstructure.Decode(config, &s3Config); err != nil {
		return nil, err
	}

	sessionConfig, err := awsutil.NewSimpleSessionConfig(config, s3.ServiceName)
	if err != nil {
		return nil, err
	}

	s3Config.RegionHint = appConfig.GetString("AWS_S3_REGION_HINT", "us-east-1")

	return &S3Manager{
		baseManager: &baseManager{
			logger:         log,
			defaultTimeout: defaultTimeout,
		},
		config:        &s3Config,
		sessionConfig: sessionConfig,
	}, nil
}

func (m *S3Manager) ListFilesWithPrefix(ctx context.Context, startAfter, prefix string, maxItems int64) ListSession {
	return &s3ListSession{
		baseListSession: &baseListSession{
			ctx:        ctx,
			startAfter: startAfter,
			prefix:     prefix,
			maxItems:   maxItems,
		},
		manager:     m,
		isTruncated: true,
	}
}

// Download downloads a file from S3
func (m *S3Manager) Download(ctx context.Context, output *os.File, key string) error {
	sess, err := m.getSession(ctx)
	if err != nil {
		return fmt.Errorf("error starting S3 session: %w", err)
	}

	downloader := awsS3Manager.NewDownloader(sess)

	ctx, cancel := context.WithTimeout(ctx, m.getTimeout())
	defer cancel()

	_, err = downloader.DownloadWithContext(ctx, output,
		&s3.GetObjectInput{
			Bucket: aws.String(m.config.Bucket),
			Key:    aws.String(key),
		})
	if err != nil {
		if codeErr, ok := err.(codeError); ok && codeErr.Code() == "NoSuchKey" {
			return ErrKeyNotFound
		}
		return err
	}
	return nil
}

// Upload uploads a file to S3
func (m *S3Manager) Upload(ctx context.Context, file *os.File, prefixes ...string) (UploadedFile, error) {
	fileName := path.Join(m.config.Prefix, path.Join(prefixes...), path.Base(file.Name()))

	uploadInput := &awsS3Manager.UploadInput{
		ACL:    aws.String("bucket-owner-full-control"),
		Bucket: aws.String(m.config.Bucket),
		Key:    aws.String(fileName),
		Body:   file,
	}
	if m.config.EnableSSE {
		uploadInput.ServerSideEncryption = aws.String("AES256")
	}

	uploadSession, err := m.getSession(ctx)
	if err != nil {
		return UploadedFile{}, fmt.Errorf("error starting S3 session: %w", err)
	}
	s3manager := awsS3Manager.NewUploader(uploadSession)

	ctx, cancel := context.WithTimeout(ctx, m.getTimeout())
	defer cancel()

	output, err := s3manager.UploadWithContext(ctx, uploadInput)
	if err != nil {
		if codeErr, ok := err.(codeError); ok && codeErr.Code() == "MissingRegion" {
			err = fmt.Errorf(fmt.Sprintf(`Bucket '%s' not found.`, m.config.Bucket))
		}
		return UploadedFile{}, err
	}

	return UploadedFile{Location: output.Location, ObjectName: fileName}, err
}

func (m *S3Manager) Delete(ctx context.Context, keys []string) (err error) {
	sess, err := m.getSession(ctx)
	if err != nil {
		return fmt.Errorf("error starting S3 session: %w", err)
	}

	var objects []*s3.ObjectIdentifier
	for _, key := range keys {
		objects = append(objects, &s3.ObjectIdentifier{Key: aws.String(key)})
	}

	svc := s3.New(sess)

	batchSize := 1000 // max accepted by DeleteObjects API
	chunks := lo.Chunk(objects, batchSize)
	for _, chunk := range chunks {
		input := &s3.DeleteObjectsInput{
			Bucket: aws.String(m.config.Bucket),
			Delete: &s3.Delete{
				Objects: chunk,
			},
		}

		deleteCtx, cancel := context.WithTimeout(ctx, m.getTimeout())
		_, err := svc.DeleteObjectsWithContext(deleteCtx, input)
		cancel()

		if err != nil {
			if codeErr, ok := err.(codeError); ok {
				m.logger.Errorf(`Error while deleting S3 objects: %v, error code: %v`, err.Error(), codeErr.Code())
			} else {
				m.logger.Errorf(`Error while deleting S3 objects: %v`, err.Error())
			}
			return err
		}
	}
	return nil
}

func (m *S3Manager) Prefix() string {
	return m.config.Prefix
}

/*
GetObjectNameFromLocation gets the object name/key name from the object location url

	https://bucket-name.s3.amazonaws.com/key - >> key
*/
func (m *S3Manager) GetObjectNameFromLocation(location string) (string, error) {
	parsedUrl, err := url.Parse(location)
	if err != nil {
		return "", err
	}
	trimmedURL := strings.TrimLeft(parsedUrl.Path, "/")
	if (m.config.S3ForcePathStyle != nil && *m.config.S3ForcePathStyle) ||
		(!strings.Contains(parsedUrl.Host, m.config.Bucket)) {
		return strings.TrimPrefix(trimmedURL, fmt.Sprintf(`%s/`, m.config.Bucket)), nil
	}
	return trimmedURL, nil
}

func (m *S3Manager) GetDownloadKeyFromFileLocation(location string) string {
	parsedURL, err := url.Parse(location)
	if err != nil {
		fmt.Println("error while parsing location url: ", err)
	}
	trimmedURL := strings.TrimLeft(parsedURL.Path, "/")
	if (m.config.S3ForcePathStyle != nil && *m.config.S3ForcePathStyle) ||
		(!strings.Contains(parsedURL.Host, m.config.Bucket)) {
		return strings.TrimPrefix(trimmedURL, fmt.Sprintf(`%s/`, m.config.Bucket))
	}
	return trimmedURL
}

func (m *S3Manager) getSession(ctx context.Context) (*session.Session, error) {
	m.sessionMu.Lock()
	defer m.sessionMu.Unlock()

	if m.session != nil {
		return m.session, nil
	}

	if m.config.Bucket == "" {
		return nil, errors.New("no storage bucket configured to downloader")
	}

	if !m.config.UseGlue || m.config.Region == nil {
		getRegionSession, err := session.NewSession()
		if err != nil {
			return nil, err
		}

		ctx, cancel := context.WithTimeout(ctx, m.getTimeout())
		defer cancel()

		region, err := awsS3Manager.GetBucketRegion(ctx, getRegionSession, m.config.Bucket, m.config.RegionHint)
		if err != nil {
			m.logger.Errorf("Failed to fetch AWS region for bucket %s. Error %v", m.config.Bucket, err)
			// Failed to get Region probably due to VPC restrictions
			// Will proceed to try with AccessKeyID and AccessKey
		}
		m.config.Region = aws.String(region)
		m.sessionConfig.Region = region
	}

	var err error
	m.session, err = awsutil.CreateSession(m.sessionConfig)
	if err != nil {
		return nil, err
	}
	return m.session, err
}

type S3Manager struct {
	*baseManager
	config *S3Config

	sessionConfig *awsutil.SessionConfig
	session       *session.Session
	sessionMu     sync.Mutex
}

func (m *S3Manager) getTimeout() time.Duration {
	if m.timeout > 0 {
		return m.timeout
	}
	if m.defaultTimeout != nil {
		return m.defaultTimeout()
	}
	return defaultTimeout
}

type s3ListSession struct {
	*baseListSession
	manager *S3Manager

	continuationToken *string
	isTruncated       bool
}

func (l *s3ListSession) Next() (fileObjects []*FileInfo, err error) {
	manager := l.manager
	if !l.isTruncated {
		manager.logger.Infof("Manager is truncated: %v so returning here", l.isTruncated)
		return
	}
	fileObjects = make([]*FileInfo, 0)

	sess, err := manager.getSession(l.ctx)
	if err != nil {
		return []*FileInfo{}, fmt.Errorf("error starting S3 session: %w", err)
	}
	// Create S3 service client
	svc := s3.New(sess)
	listObjectsV2Input := s3.ListObjectsV2Input{
		Bucket:  aws.String(manager.config.Bucket),
		Prefix:  aws.String(l.prefix),
		MaxKeys: &l.maxItems,
		// Delimiter: aws.String("/"),
	}
	// startAfter is to resume a paused task.
	if l.startAfter != "" {
		listObjectsV2Input.StartAfter = aws.String(l.startAfter)
	}

	if l.continuationToken != nil {
		listObjectsV2Input.ContinuationToken = l.continuationToken
	}

	ctx, cancel := context.WithTimeout(l.ctx, manager.getTimeout())
	defer cancel()

	// Get the list of items
	resp, err := svc.ListObjectsV2WithContext(ctx, &listObjectsV2Input)
	if err != nil {
		manager.logger.Errorf("Error while listing S3 objects: %v", err)
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

type codeError interface {
	Code() string
}
