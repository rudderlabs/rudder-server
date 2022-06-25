package fileuploader

import (
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/rudderlabs/rudder-server/config"
)

// Upload passed in file to s3
func Download(file *os.File, key, bucket string) (int64, error) {
	getRegionSession := session.Must(session.NewSession())
	region, err := s3manager.GetBucketRegion(aws.BackgroundContext(), getRegionSession, bucket, "us-east-1")
	downloadSession := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(config.GetEnv("AWS_ACCESS_KEY_ID", ""), config.GetEnv("AWS_SECRET_ACCESS_KEY", ""), ""),
	}))

	downloader := s3manager.NewDownloader(downloadSession)
	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})

	return numBytes, err
}
