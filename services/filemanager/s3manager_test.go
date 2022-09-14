package filemanager

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/assert"
)

func TestGetSessionWithAccessKeys(t *testing.T) {
	s3Manager := S3Manager{
		Config: &S3Config{
			Bucket:      "someBucket",
			AccessKeyID: "someAccessKeyId",
			AccessKey:   "someSecretAccessKey",
			Region:      aws.String("someRegion"),
		},
	}
	awsSession, err := s3Manager.getSession(context.TODO())
	assert.Nil(t, err)
	assert.NotNil(t, awsSession)
	assert.NotNil(t, s3Manager.session)
}

func TestGetSessionWithIAMRole(t *testing.T) {
	s3Manager := S3Manager{
		Config: &S3Config{
			Bucket:     "someBucket",
			IAMRoleARN: "someIAMRole",
			ExternalID: "someExternalID",
			Region:     aws.String("someRegion"),
		},
	}
	awsSession, err := s3Manager.getSession(context.TODO())
	assert.Nil(t, err)
	assert.NotNil(t, awsSession)
	assert.NotNil(t, s3Manager.session)
}

func TestGetSessionConfigWithAccessKeys(t *testing.T) {
	s3Manager := S3Manager{
		Config: &S3Config{
			Bucket:      "someBucket",
			AccessKeyID: "someAccessKeyId",
			AccessKey:   "someSecretAccessKey",
			Region:      aws.String("someRegion"),
		},
	}
	awsSessionConfig := s3Manager.getSessionConfig()
	assert.NotNil(t, awsSessionConfig)
	assert.Equal(t, s3Manager.Config.AccessKey, awsSessionConfig.AccessKey)
	assert.Equal(t, s3Manager.Config.AccessKeyID, awsSessionConfig.AccessKeyID)
}

func TestGetSessionConfigWithIAMRole(t *testing.T) {
	s3Manager := S3Manager{
		Config: &S3Config{
			Bucket:     "someBucket",
			IAMRoleARN: "someIAMRole",
			ExternalID: "someExternalID",
			Region:     aws.String("someRegion"),
		},
	}
	awsSessionConfig := s3Manager.getSessionConfig()
	assert.NotNil(t, awsSessionConfig)
	assert.Equal(t, s3Manager.Config.IAMRoleARN, awsSessionConfig.IAMRoleARN)
	assert.Equal(t, s3Manager.Config.ExternalID, awsSessionConfig.ExternalID)
}
