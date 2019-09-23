package fileuploader

import (
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/rudderlabs/rudder-server/config"
)

// Upload passed in file to s3
func (uploader *S3Uploader) Upload(file *os.File, prefixes ...string) error {
	getRegionSession := session.Must(session.NewSession())
	region, err := s3manager.GetBucketRegion(aws.BackgroundContext(), getRegionSession, uploader.bucket, "us-east-1")
	uploadSession := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(config.GetEnv("IAM_S3_COPY_ACCESS_KEY_ID", ""), config.GetEnv("IAM_S3_COPY_SECRET_ACCESS_KEY", ""), ""),
	}))
	manager := s3manager.NewUploader(uploadSession)
	splitFileName := strings.Split(file.Name(), "/")
	fileName := ""
	if len(prefixes) > 0 {
		fileName = strings.Join(prefixes[:], "/") + "/"
	}
	fileName += splitFileName[len(splitFileName)-1]
	_, err = manager.Upload(&s3manager.UploadInput{
		ACL:    aws.String("bucket-owner-full-control"),
		Bucket: aws.String(uploader.bucket),
		Key:    aws.String(fileName),
		Body:   file,
	})
	// do not panic if upload has failed for customer s3 bucket
	// misc.AssertError(err)
	return err
}

// S3Uploader contains config for uploading object to s3
type S3Uploader struct {
	bucket string
}
