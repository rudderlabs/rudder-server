package jobsdb

import (
	"errors"
	"fmt"
	"os"

	"github.com/rudderlabs/rudder-server/config"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func s3Upload(file *os.File) (err error) {
	s3Bucket := config.GetString("Aws.backupDSBucket", "dump-gateway-ds-test")
	fmt.Printf("Uploading %q to s3:%q\n", file.Name(), s3Bucket)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(file.Name()),
		Body:   file,
	})
	if err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Printf("Successfully uploaded %q to s3:%q\n", file.Name(), s3Bucket)
	return err
}

func (uploader *fileUploaderT) upload(file *os.File) error {
	switch uploader.provider {
	case "s3":
		return s3Upload(file)
	default:
		return errors.New("Invalid fileUploader provided")
	}
}

// Sets config for S3 uploader
func setupS3Uploader() {
	region := config.GetString("Aws.region", "us-east-2")
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)
	if err != nil {
		panic(err)
	}
	uploader = s3manager.NewUploader(sess)
}

type fileUploaderT struct {
	provider string
}

// SetupFileUploader sets up fileUploader as per type
func (uploader *fileUploaderT) Setup() {
	switch uploader.provider {
	case "s3":
		setupS3Uploader()
	}
}
