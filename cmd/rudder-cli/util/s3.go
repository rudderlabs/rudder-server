package util

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/config"
)

func testFileCreation() (string, error) {
	var err error
	tmpdirPath := strings.TrimSuffix(config.GetEnv("RUDDER_TMPDIR"), "/")
	if tmpdirPath == "" {
		tmpdirPath, err = os.UserHomeDir()
		if err != nil {
			return "Could not fetch tmp dir", err
		}
	}

	data := []byte("Rudder Test\n")
	testFilePath := fmt.Sprintf("%s/%s", tmpdirPath, "cli-test")
	err = os.MkdirAll(filepath.Dir(testFilePath), os.ModePerm)
	if err != nil {
		return "Could not create sub dir", err
	}

	err = os.WriteFile(testFilePath, data, 0o644)

	if err != nil {
		return "Could not write to temp file", err
	}

	return testFilePath, nil
}

// Upload passed in file to s3
func TestUpload() (string, error) {
	filePath, err := testFileCreation()
	if err != nil {
		return filePath, err
	}

	fileToUpload, err := os.Open(filePath)
	if err != nil {
		return "Failed to upload the file", err
	}

	bucketName := config.GetEnv(config.JobsS3BucketKey)
	getRegionSession := session.Must(session.NewSession())
	region, err := s3manager.GetBucketRegion(aws.BackgroundContext(), getRegionSession, bucketName, "us-east-1")
	if err != nil {
		return "Failed to get region for s3", err
	}
	uploadSession := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	}))
	manager := s3manager.NewUploader(uploadSession)
	splitFileName := strings.Split(fileToUpload.Name(), "/")

	_, err = manager.Upload(&s3manager.UploadInput{
		ACL:    aws.String("bucket-owner-full-control"),
		Bucket: aws.String(bucketName),
		Key:    aws.String(splitFileName[len(splitFileName)-1]),
		Body:   fileToUpload,
	})

	if err != nil {
		return "Failed to upload to S3. Check Credentials.", err
	}
	return "success", nil
}
