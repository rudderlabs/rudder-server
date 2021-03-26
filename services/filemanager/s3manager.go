package filemanager

import (
	"errors"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	awsS3Manager "github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Upload passed in file to s3
func (manager *S3Manager) Upload(file *os.File, prefixes ...string) (UploadOutput, error) {
	if manager.Config.Bucket == "" {
		return UploadOutput{}, errors.New("no storage bucket configured to uploader")
	}
	getRegionSession := session.Must(session.NewSession())
	region, err := awsS3Manager.GetBucketRegion(aws.BackgroundContext(), getRegionSession, manager.Config.Bucket, "us-east-1")
	if err != nil {
		pkgLogger.Errorf("Failed to fetch AWS region for bucket %s. Error %v", manager.Config.Bucket, err)
		/// Failed to Get Region probably due to VPC restrictions, Will proceed to try with AccessKeyID and AccessKey
	}
	var uploadSession *session.Session
	if manager.Config.AccessKeyID == "" || manager.Config.AccessKey == "" {
		uploadSession = session.Must(session.NewSession(&aws.Config{
			Region: aws.String(region),
		}))
	} else {
		uploadSession = session.Must(session.NewSession(&aws.Config{
			Region:      aws.String(region),
			Credentials: credentials.NewStaticCredentials(manager.Config.AccessKeyID, manager.Config.AccessKey, ""),
		}))
	}
	s3manager := awsS3Manager.NewUploader(uploadSession)
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
	output, err := s3manager.Upload(uploadInput)
	if err != nil {
		return UploadOutput{}, err
	}
	return UploadOutput{Location: output.Location, ObjectName: fileName}, err
}

func (manager *S3Manager) Download(output *os.File, key string) error {
	if manager.Config.Bucket == "" {
		return errors.New("no storage bucket configured to downloader")
	}

	getRegionSession := session.Must(session.NewSession())
	region, err := awsS3Manager.GetBucketRegion(aws.BackgroundContext(), getRegionSession, manager.Config.Bucket, "us-east-1")
	if err != nil {
		pkgLogger.Errorf("Failed to fetch AWS region for bucket %s. Error %v", manager.Config.Bucket, err)
		/// Failed to Get Region probably due to VPC restrictions, Will proceed to try with AccessKeyID and AccessKey
	}
	var sess *session.Session
	if manager.Config.AccessKeyID == "" || manager.Config.AccessKey == "" {
		sess = session.Must(session.NewSession(&aws.Config{
			Region: aws.String(region),
		}))
	} else {
		sess = session.Must(session.NewSession(&aws.Config{
			Region:      aws.String(region),
			Credentials: credentials.NewStaticCredentials(manager.Config.AccessKeyID, manager.Config.AccessKey, ""),
		}))
	}
	downloader := s3manager.NewDownloader(sess)
	_, err = downloader.Download(output,
		&s3.GetObjectInput{
			Bucket: aws.String(manager.Config.Bucket),
			Key:    aws.String(key),
		})

	return err
}

func (manager *S3Manager) GetDownloadKeyFromFileLocation(location string) string {
	locationSlice := strings.Split(location, "amazonaws.com/")
	return locationSlice[len(locationSlice)-1]
}

/*
GetObjectNameFromLocation gets the object name/key name from the object location url
	https://bucket-name.s3.amazonaws.com/key - >> key
*/
func (manager *S3Manager) GetObjectNameFromLocation(location string) (string, error) {
	reg, err := regexp.Compile(`^https.+\.s3\..*amazonaws\.com\/`)
	if err != nil {
		return "", err
	}
	return reg.ReplaceAllString(location, ""), nil
}

type S3Object struct {
	Key              string
	LastModifiedTime time.Time
}

func (manager *S3Manager) ListFilesWithPrefix(prefix string) ([]*S3Object, error) {
	s3Objects := make([]*S3Object, 0)

	getRegionSession := session.Must(session.NewSession())
	region, err := awsS3Manager.GetBucketRegion(aws.BackgroundContext(), getRegionSession, manager.Config.Bucket, "us-east-1")
	if err != nil {
		pkgLogger.Errorf("Failed to fetch AWS region for bucket %s. Error %v", manager.Config.Bucket, err)
		/// Failed to Get Region probably due to VPC restrictions, Will proceed to try with AccessKeyID and AccessKey
	}
	var sess *session.Session
	if manager.Config.AccessKeyID == "" || manager.Config.AccessKey == "" {
		sess = session.Must(session.NewSession(&aws.Config{
			Region: aws.String(region),
		}))
	} else {
		sess = session.Must(session.NewSession(&aws.Config{
			Region:      aws.String(region),
			Credentials: credentials.NewStaticCredentials(manager.Config.AccessKeyID, manager.Config.AccessKey, ""),
		}))
	}

	// Create S3 service client
	svc := s3.New(sess)

	// Get the list of items
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(manager.Config.Bucket),
		Prefix: aws.String(prefix),
		// Delimiter: aws.String("/"),
	})
	if err != nil {
		return s3Objects, err
	}

	for _, item := range resp.Contents {
		s3Objects = append(s3Objects, &S3Object{*item.Key, *item.LastModified})
	}

	return s3Objects, nil
}

type S3Manager struct {
	Config *S3Config
}

func GetS3Config(config map[string]interface{}) *S3Config {
	var bucketName, prefix, accessKeyID, accessKey string
	var enableSSE, ok bool
	if config["bucketName"] != nil {
		bucketName = config["bucketName"].(string)
	}
	if config["prefix"] != nil {
		prefix = config["prefix"].(string)
	}
	if config["accessKeyID"] != nil {
		accessKeyID = config["accessKeyID"].(string)
	}
	if config["accessKey"] != nil {
		accessKey = config["accessKey"].(string)
	}
	if config["enableSSE"] != nil {
		if enableSSE, ok = config["enableSSE"].(bool); !ok {
			enableSSE = false
		}
	}
	return &S3Config{Bucket: bucketName, Prefix: prefix, AccessKeyID: accessKeyID, AccessKey: accessKey, EnableSSE: enableSSE}
}

type S3Config struct {
	Bucket      string
	Prefix      string
	AccessKeyID string
	AccessKey   string
	EnableSSE   bool
}
