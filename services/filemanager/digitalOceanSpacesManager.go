package filemanager

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

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
func (manager *DOSpacesManager) Upload(file *os.File, prefixes ...string) (UploadOutput, error) {
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
	s3manager := SpacesManager.NewUploader(uploadSession)
	output, err := s3manager.Upload(uploadInput)
	if err != nil {
		if awsError, ok := err.(awserr.Error); ok && awsError.Code() == "MissingRegion" {
			err = fmt.Errorf(fmt.Sprintf(`Bucket '%s' not found.`, manager.Config.Bucket))
		}
		return UploadOutput{}, err
	}

	return UploadOutput{Location: output.Location, ObjectName: fileName}, err
}

func (manager *DOSpacesManager) Download(output *os.File, key string) error {

	downloadSession := manager.getSession()

	downloader := SpacesManager.NewDownloader(downloadSession)
	_, err := downloader.Download(output,
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
	parsedUrl, err := url.Parse(location)
	if err != nil {
		return "", err
	}
	trimedUrl := strings.TrimLeft(parsedUrl.Path, "/")
	if (manager.Config.ForcePathStyle != nil && *manager.Config.ForcePathStyle) || (!strings.Contains(parsedUrl.Host, manager.Config.Bucket)) {
		return strings.TrimPrefix(trimedUrl, fmt.Sprintf(`%s/`, manager.Config.Bucket)), nil
	}
	return trimedUrl, nil
}

func (manager *DOSpacesManager) ListFilesWithPrefix(prefix string, maxItems int64) (fileObjects []*FileObject, err error) {
	fileObjects = make([]*FileObject, 0)

	sess := manager.getSession()

	// Create S3 service client
	svc := s3.New(sess)

	// Get the list of items
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
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

func (manager *DOSpacesManager) DeleteObjects(keys []string) (err error) {
	sess := manager.getSession()
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
		_, err := svc.DeleteObjects(input)
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

type DOSpacesManager struct {
	Config *DOSpacesConfig
}

func GetDOSpacesConfig(config map[string]interface{}) *DOSpacesConfig {
	var bucketName, prefix, endPoint, accessKeyID, accessKey string
	var region *string
	var forcePathStyle, disableSSL *bool
	if config["bucketName"] != nil {
		bucketName = config["bucketName"].(string)
	}
	if config["prefix"] != nil {
		prefix = config["prefix"].(string)
	}
	if config["endPoint"] != nil {
		endPoint = config["endPoint"].(string)
	}
	if config["accessKeyID"] != nil {
		accessKeyID = config["accessKeyID"].(string)
	}
	if config["accessKey"] != nil {
		accessKey = config["accessKey"].(string)
	}
	if config["region"] != nil {
		tmp := config["region"].(string)
		region = &tmp
	}
	if config["forcePathStyle"] != nil {
		tmp := config["forcePathStyle"].(bool)
		forcePathStyle = &tmp
	}
	if config["disableSSL"] != nil {
		tmp := config["disableSSL"].(bool)
		disableSSL = &tmp
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
