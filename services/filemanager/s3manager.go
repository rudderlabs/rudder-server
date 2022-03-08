package filemanager

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	awsS3Manager "github.com/aws/aws-sdk-go/service/s3/s3manager"
	appConfig "github.com/rudderlabs/rudder-server/config"
)

// Upload passed in file to s3
func (manager *S3Manager) Upload(ctx context.Context, file *os.File, prefixes ...string) (UploadOutput, error) {
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
		return UploadOutput{}, fmt.Errorf(`error starting S3 session: %v`, err)
	}
	s3manager := awsS3Manager.NewUploader(uploadSession)

	ctxWithTimeout, cancel := context.WithTimeout(ctx, *manager.Timeout)
	defer cancel()

	output, err := s3manager.UploadWithContext(ctxWithTimeout, uploadInput)
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
		return fmt.Errorf(`error starting S3 session: %v`, err)
	}

	downloader := s3manager.NewDownloader(sess)

	ctxWithTimeout, cancel := context.WithTimeout(ctx, *manager.Timeout)
	defer cancel()

	_, err = downloader.DownloadWithContext(ctxWithTimeout, output,
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
	parsedUrl, err := url.Parse(location)
	if err != nil {
		fmt.Println("error while parsing location url: ", err)
	}
	trimedUrl := strings.TrimLeft(parsedUrl.Path, "/")
	if (manager.Config.S3ForcePathStyle != nil && *manager.Config.S3ForcePathStyle) || (!strings.Contains(parsedUrl.Host, manager.Config.Bucket)) {
		return strings.TrimPrefix(trimedUrl, fmt.Sprintf(`%s/`, manager.Config.Bucket))
	}
	return trimedUrl
}

func (manager *S3Manager) DeleteObjects(ctx context.Context, keys []string) (err error) {
	sess, err := manager.getSession(ctx)
	if err != nil {
		return fmt.Errorf(`error starting S3 session: %v`, err)
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

		ctxWithTimeout, cancel := context.WithTimeout(ctx, *manager.Timeout)
		defer cancel()

		_, err := svc.DeleteObjectsWithContext(ctxWithTimeout, input)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				default:
					pkgLogger.Errorf(`Error while deleting S3 objects: %v, error code: %v`, aerr.Error(), aerr.Code())
				}
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
	var region string
	var err error
	if manager.Config.Region == nil {
		getRegionSession := session.Must(session.NewSession())

		ctxWithTimeout, cancel := context.WithTimeout(ctx, *manager.Timeout)
		defer cancel()

		region, err = awsS3Manager.GetBucketRegion(ctxWithTimeout, getRegionSession, manager.Config.Bucket, manager.Config.RegionHint)
		if err != nil {
			pkgLogger.Errorf("Failed to fetch AWS region for bucket %s. Error %v", manager.Config.Bucket, err)
			/// Failed to Get Region probably due to VPC restrictions, Will proceed to try with AccessKeyID and AccessKey
		}
	} else {
		region = *manager.Config.Region
	}
	var sess *session.Session
	if manager.Config.AccessKeyID == "" || manager.Config.AccessKey == "" {
		pkgLogger.Debug("Credentials not found in the destination's config. Using the host credentials instead")
		sess = session.Must(session.NewSession(&aws.Config{
			Region:                        aws.String(region),
			CredentialsChainVerboseErrors: aws.Bool(true),
			Endpoint:                      manager.Config.Endpoint,
			S3ForcePathStyle:              manager.Config.S3ForcePathStyle,
			DisableSSL:                    manager.Config.DisableSSL,
		}))
	} else {
		pkgLogger.Debug("Credentials found in the destination's config.")
		sess = session.Must(session.NewSession(&aws.Config{
			Region:                        aws.String(region),
			Credentials:                   credentials.NewStaticCredentials(manager.Config.AccessKeyID, manager.Config.AccessKey, ""),
			CredentialsChainVerboseErrors: aws.Bool(true),
			Endpoint:                      manager.Config.Endpoint,
			S3ForcePathStyle:              manager.Config.S3ForcePathStyle,
			DisableSSL:                    manager.Config.DisableSSL,
		}))
	}
	return sess, nil
}

//IMPT NOTE: `ListFilesWithPrefix` support Continuation Token. So, if you want same set of files (says 1st 1000 again)
//then create a new S3Manager & not use the existing one. Since, using the existing one will by default return next 1000 files.
func (manager *S3Manager) ListFilesWithPrefix(ctx context.Context, prefix string, maxItems int64) (fileObjects []*FileObject, err error) {
	if !manager.Config.IsTruncated {
		return
	}
	fileObjects = make([]*FileObject, 0)

	sess, err := manager.getSession(ctx)
	if err != nil {
		return []*FileObject{}, fmt.Errorf(`error starting S3 session: %v`, err)
	}
	// Create S3 service client
	svc := s3.New(sess)
	listObjectsV2Input := s3.ListObjectsV2Input{
		Bucket:  aws.String(manager.Config.Bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: &maxItems,
		// Delimiter: aws.String("/"),
	}
	//startAfter is to resume a paused task.
	if manager.Config.StartAfter != "" {
		listObjectsV2Input.StartAfter = aws.String(manager.Config.StartAfter)
	}
	listObjectsV2Input.ContinuationToken = manager.Config.ContinuationToken

	ctxWithTimeout, cancel := context.WithTimeout(ctx, *manager.Timeout)
	defer cancel()

	// Get the list of items
	resp, err := svc.ListObjectsV2WithContext(ctxWithTimeout, &listObjectsV2Input)
	if err != nil {
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
	Config  *S3Config
	session *session.Session
	Timeout *time.Duration
}

func GetS3Config(config map[string]interface{}) *S3Config {
	var bucketName, prefix, accessKeyID, accessKey, startAfter string
	var continuationToken, endPoint, region *string
	var enableSSE, ok bool
	var s3ForcePathStyle, disableSSL *bool
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
	if config["enableSSE"] != nil {
		if enableSSE, ok = config["enableSSE"].(bool); !ok {
			enableSSE = false
		}
	}
	if config["startAfter"] != nil {
		tmp, ok := config["startAfter"].(string)
		if ok {
			startAfter = tmp
		}
	}
	if config["endPoint"] != nil {
		tmp, ok := config["endPoint"].(string)
		if ok {
			endPoint = &tmp
		}
	}
	if config["s3ForcePathStyle"] != nil {
		tmp, ok := config["s3ForcePathStyle"].(bool)
		if ok {
			s3ForcePathStyle = &tmp
		}
	}
	if config["disableSSL"] != nil {
		tmp, ok := config["disableSSL"].(bool)
		if ok {
			disableSSL = &tmp
		}
	}
	if config["region"] != nil {
		tmp, ok := config["region"].(string)
		if ok {
			region = &tmp
		}
	}
	regionHint := appConfig.GetEnv("AWS_S3_REGION_HINT", "us-east-1")
	return &S3Config{
		Endpoint:          endPoint,
		Bucket:            bucketName,
		Prefix:            prefix,
		AccessKeyID:       accessKeyID,
		AccessKey:         accessKey,
		EnableSSE:         enableSSE,
		Region:            region,
		RegionHint:        regionHint,
		ContinuationToken: continuationToken,
		StartAfter:        startAfter,
		IsTruncated:       true,
		S3ForcePathStyle:  s3ForcePathStyle,
		DisableSSL:        disableSSL,
	}
}

type S3Config struct {
	Bucket            string
	Prefix            string
	AccessKeyID       string
	AccessKey         string
	EnableSSE         bool
	Region            *string
	RegionHint        string
	ContinuationToken *string
	StartAfter        string
	IsTruncated       bool
	Endpoint          *string
	S3ForcePathStyle  *bool
	DisableSSL        *bool
}
