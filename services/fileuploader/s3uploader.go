package fileuploader

import (
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/rudderlabs/rudder-server/misc"
)

// Upload passed in file to s3
func (uploader *S3Uploader) Upload(file *os.File) error {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(uploader.region)},
	)
	misc.AssertError(err)
	manager := s3manager.NewUploader(sess)
	splitFileName := strings.Split(file.Name(), "/")
	fileName := splitFileName[len(splitFileName)-1]
	_, err = manager.Upload(&s3manager.UploadInput{
		Bucket: aws.String(uploader.bucket),
		Key:    aws.String(fileName),
		Body:   file,
	})
	misc.AssertError(err)
	return err
}

// S3Uploader contains config for uploading object to s3
type S3Uploader struct {
	bucket string
	region string
}
