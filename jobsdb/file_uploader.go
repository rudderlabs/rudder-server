package jobsdb

import (
	"fmt"
	"os"
	"strings"

	"github.com/rudderlabs/rudder-server/config"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Upload file to s3 bucket conffigured in S3FileUploaderT
func (uploader *S3FileUploaderT) Upload(file *os.File) error {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(uploader.region)},
	)
	if err != nil {
		panic(err)
	}
	manager := s3manager.NewUploader(sess)
	splitFileName := strings.Split(file.Name(), "/")
	fileName := splitFileName[len(splitFileName)-1]
	fmt.Printf("Uploading %q to s3:%q\n", fileName, uploader.s3Bucket)
	_, err = manager.Upload(&s3manager.UploadInput{
		Bucket: aws.String(uploader.s3Bucket),
		Key:    aws.String(fileName),
		Body:   file,
	})
	if err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Printf("Successfully uploaded %q to s3:%q\n", fileName, uploader.s3Bucket)
	return err
}

// Uploader interface to have collection of common uploader method signatures
type Uploader interface {
	Upload(file *os.File) error
}

// FileUploaderT struct to store provider etc.
type FileUploaderT struct {
	provider string
	Upload   func(file *os.File) error
}

// S3FileUploaderT struct to store s3 config etc.
type S3FileUploaderT struct {
	region   string
	s3Bucket string
}

// Setup sets up fileUploader as per type
func (fileUploader *FileUploaderT) Setup() {
	switch fileUploader.provider {
	case "s3":
		s3Uploader := &S3FileUploaderT{
			region:   config.GetString("Aws.region", "us-east-2"),
			s3Bucket: config.GetString("Aws.backupDSBucket", "dump-gateway-ds-test"),
		}
		fileUploader.Upload = s3Uploader.Upload
	}
}
