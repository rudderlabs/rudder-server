package filemanager

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	SpacesManager "github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// Upload passed in file to spaces
func (manager *DOSpacesManager) Upload(file *os.File, prefixes ...string) (UploadOutput, error) {
	if manager.Config.Bucket == "" {
		return UploadOutput{}, errors.New("no storage bucket configured to uploader")
	}

	region := misc.GetSpacesLocation(manager.Config.EndPoint)
	uploadSession, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(manager.Config.AccessKeyID, manager.Config.AccessKey, ""),
		Endpoint:    aws.String(manager.Config.EndPoint),
	})
	if err != nil {
		return UploadOutput{}, fmt.Errorf("Encountered error while creating digitalOcean Session : %w", err)
	}

	s3Client := s3.New(uploadSession)
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
	uploadInput := s3.PutObjectInput{
		ACL:    aws.String("bucket-owner-full-control"),
		Bucket: aws.String(manager.Config.Bucket),
		Key:    aws.String(fileName),
		Body:   file,
	}
	_, err = s3Client.PutObject(&uploadInput)
	if err != nil {
		return UploadOutput{}, err
	}
	location := manager.Config.Bucket + "." + manager.Config.EndPoint + "." + fileName
	return UploadOutput{Location: location, ObjectName: fileName}, err
}

func (manager *DOSpacesManager) Download(output *os.File, key string) error {

	region := misc.GetSpacesLocation(manager.Config.EndPoint)
	downloadSession, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(manager.Config.AccessKeyID, manager.Config.AccessKey, ""),
		Endpoint:    aws.String(manager.Config.EndPoint),
	})
	if err != nil {
		return fmt.Errorf("Encountered error while creating digitalOcean Session : %w", err)
	}

	downloader := SpacesManager.NewDownloader(downloadSession)
	_, err = downloader.Download(output,
		&s3.GetObjectInput{
			Bucket: aws.String(manager.Config.Bucket),
			Key:    aws.String(key),
		})

	return err
}

//TODO complete this
func (manager *DOSpacesManager) GetDownloadKeyFromFileLocation(location string) string {
	return location
}

/*
GetObjectNameFromLocation gets the object name/key name from the object location url
	https://rudder.sgp1.digitaloceanspaces.com/key - >> key
*/
func (manager *DOSpacesManager) GetObjectNameFromLocation(location string) (string, error) {
	reg, err := regexp.Compile(`^https.+digitaloceanspaces\.com\/`)
	if err != nil {
		return "", err
	}
	return reg.ReplaceAllString(location, ""), nil
}

type SpacesObject struct {
	Key              string
	LastModifiedTime time.Time
}

func (manager *DOSpacesManager) ListFilesWithPrefix(prefix string) ([]*SpacesObject, error) {
	spacesObjects := make([]*SpacesObject, 0)

	getRegionSession := session.Must(session.NewSession())
	region, err := SpacesManager.GetBucketRegion(aws.BackgroundContext(), getRegionSession, manager.Config.Bucket, "us-east-1")
	if err != nil {
		pkgLogger.Errorf("Failed to fetch AWS region for bucket %s. Error %v", manager.Config.Bucket, err)
		return spacesObjects, err
	}
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(manager.Config.AccessKeyID, manager.Config.AccessKey, ""),
	}))

	// Create S3 service client
	svc := s3.New(sess)

	// Get the list of items
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(manager.Config.Bucket),
		Prefix: aws.String(prefix),
		// Delimiter: aws.String("/"),
	})
	if err != nil {
		return spacesObjects, err
	}

	for _, item := range resp.Contents {
		spacesObjects = append(spacesObjects, &SpacesObject{*item.Key, *item.LastModified})
	}

	return spacesObjects, nil
}

type DOSpacesManager struct {
	Config *DOSpacesConfig
}

func GetDOSpacesConfig(config map[string]interface{}) *DOSpacesConfig {
	var bucketName, prefix, endPoint, accessKeyID, accessKey string
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
	return &DOSpacesConfig{Bucket: bucketName, EndPoint: endPoint, Prefix: prefix, AccessKeyID: accessKeyID, AccessKey: accessKey}
}

type DOSpacesConfig struct {
	Bucket      string
	Prefix      string
	EndPoint    string
	AccessKeyID string
	AccessKey   string
}
