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
	SpacesManager "github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func (manager *DOSpacesManager) getSession() *session.Session {

	var region string
	if manager.Config.Region != nil {
		region = *manager.Config.Region
	} else {
		region = misc.GetSpacesLocation(manager.Config.EndPoint)
	}
	return session.Must(session.NewSession(&aws.Config{
		Region:           aws.String(region),
		Credentials:      credentials.NewStaticCredentials(manager.Config.AccessKeyID, manager.Config.AccessKey, ""),
		Endpoint:         aws.String(manager.Config.EndPoint),
		DisableSSL:       manager.Config.DisableSSL,
		S3ForcePathStyle: manager.Config.ForcePathStyle,
	}))
}

// Upload passed in file to spaces
func (manager *DOSpacesManager) Upload(ctx context.Context, file *os.File, prefixes ...string) (UploadOutput, error) {
	if manager.Config.Bucket == "" {
		return UploadOutput{}, errors.New("no storage bucket configured to uploader")
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

	uploadInput := &SpacesManager.UploadInput{
		ACL:    aws.String("bucket-owner-full-control"),
		Bucket: aws.String(manager.Config.Bucket),
		Key:    aws.String(fileName),
		Body:   file,
	}
	uploadSession := manager.getSession()
	DOmanager := SpacesManager.NewUploader(uploadSession)

	ctx, cancel := context.WithTimeout(ctx, getSafeTimeout(manager.Timeout))
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

	downloadSession := manager.getSession()

	ctx, cancel := context.WithTimeout(ctx, getSafeTimeout(manager.Timeout))
	defer cancel()

	downloader := SpacesManager.NewDownloader(downloadSession)
	_, err := downloader.DownloadWithContext(ctx, output,
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

func (manager *DOSpacesManager) ListFilesWithPrefix(ctx context.Context, prefix string, maxItems int64) (fileObjects []*FileObject, err error) {
	fileObjects = make([]*FileObject, 0)

	sess := manager.getSession()

	// Create S3 service client
	svc := s3.New(sess)

	ctx, cancel := context.WithTimeout(ctx, getSafeTimeout(manager.Timeout))
	defer cancel()

	// Get the list of items
	resp, err := svc.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(manager.Config.Bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: &maxItems,
		// Delimiter: aws.String("/"),
	})
	if err != nil {
		return
	}

	for _, item := range resp.Contents {
		fileObjects = append(fileObjects, &FileObject{*item.Key, *item.LastModified})
	}
	return
}

func (manager *DOSpacesManager) DeleteObjects(ctx context.Context, keys []string) (err error) {
	sess := manager.getSession()
	if err != nil {
		return fmt.Errorf(`get session: %v`, err)
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

		_ctx, cancel := context.WithTimeout(ctx, getSafeTimeout(manager.Timeout))
		_, err := svc.DeleteObjectsWithContext(_ctx, input)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				default:
					pkgLogger.Errorf(`Error while deleting digital ocean spaces objects: %v, error code: %v`, aerr.Error(), aerr.Code())
				}
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
	Timeout *time.Duration
}

func (manager *DOSpacesManager) SetTimeout(timeout *time.Duration) {
	manager.Timeout = timeout
}

func GetDOSpacesConfig(config map[string]interface{}) *DOSpacesConfig {
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
	return &DOSpacesConfig{
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

type DOSpacesConfig struct {
	Bucket         string
	Prefix         string
	EndPoint       string
	AccessKeyID    string
	AccessKey      string
	Region         *string
	ForcePathStyle *bool
	DisableSSL     *bool
}

func (manager *DOSpacesManager) GetConfiguredPrefix() string {
	return manager.Config.Prefix
}
