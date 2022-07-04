package fileuploader

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/rudderlabs/rudder-server/config"
)

func GetS3FilesList(svc *s3.S3, prefix string, continuationToken *string, bucket, startAfter string) (*S3Objects, error) {
	s3Objects := make([]*S3Object, 0)

	listObjectsV2Input := s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	if startAfter != "" {
		listObjectsV2Input.StartAfter = aws.String(startAfter)
	}

	if continuationToken != nil {
		listObjectsV2Input.ContinuationToken = continuationToken
	}

	resp, err := svc.ListObjectsV2(&listObjectsV2Input)
	if err != nil {
		return nil, err
	}

	for _, item := range resp.Contents {
		/* fmt.Println("Name:         ", *item.Key)
		fmt.Println("Last modified:", *item.LastModified)
		fmt.Println("Size:         ", *item.Size)
		fmt.Println("Storage class:", *item.StorageClass)
		fmt.Println("") */
		s3Objects = append(s3Objects, &S3Object{*item.Key, *item.LastModified})
	}

	return &S3Objects{Objects: s3Objects, Truncated: *resp.IsTruncated, ContinuationToken: resp.NextContinuationToken}, nil
}

func ListFilesWithPrefix(prefix, bucket, startAfter string, continuationToken *string) (*S3Objects, error) {
	getRegionSession := session.Must(session.NewSession())
	region, err := s3manager.GetBucketRegion(aws.BackgroundContext(), getRegionSession, bucket, "us-east-1")
	if err != nil {
		return nil, fmt.Errorf("GetBucketRegion failed with err : %w", err)
	}
	awsAccessKeyId := config.GetEnv("AWS_ACCESS_KEY_ID", "")
	awsSecretAccessKey := config.GetEnv("AWS_SECRET_ACCESS_KEY", "")

	config := &aws.Config{
		Region:                        aws.String(region),
		CredentialsChainVerboseErrors: aws.Bool(true),
	}
	if awsAccessKeyId != "" && awsSecretAccessKey != "" {
		config.Credentials = credentials.NewStaticCredentials(awsAccessKeyId, awsSecretAccessKey, "")
	}
	uploadSession := session.Must(session.NewSession(config))
	// Create S3 service client
	svc := s3.New(uploadSession)

	// Get the list of items
	s3Objects, err := GetS3FilesList(svc, prefix, continuationToken, bucket, startAfter)
	if err != nil {
		return nil, fmt.Errorf("GetS3FilesList failed with err : %w", err)
	}

	return s3Objects, nil
}

type S3Object struct {
	Key              string
	LastModifiedTime time.Time
}

type S3Objects struct {
	Objects           []*S3Object
	Truncated         bool
	ContinuationToken *string
}
