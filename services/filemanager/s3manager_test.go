package filemanager

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/rudderlabs/rudder-server/utils/awsutils"
	"github.com/stretchr/testify/assert"
)

func TestNewS3ManagerWithNil(t *testing.T) {
	s3Manager, err := NewS3Manager(nil)
	assert.EqualError(t, err, "config should not be nil")
	assert.Nil(t, s3Manager)
}

func TestNewS3ManagerWithAccessKeys(t *testing.T) {
	s3Manager, err := NewS3Manager(map[string]interface{}{
		"bucketName":  "someBucket",
		"region":      "someRegion",
		"accessKeyID": "someAccessKeyId",
		"accessKey":   "someSecretAccessKey",
	})
	assert.Nil(t, err)
	assert.NotNil(t, s3Manager)
	assert.Equal(t, "someBucket", s3Manager.Config.Bucket)
	assert.Equal(t, aws.String("someRegion"), s3Manager.Config.Region)
	assert.Equal(t, "someAccessKeyId", s3Manager.SessionConfig.AccessKeyID)
	assert.Equal(t, "someSecretAccessKey", s3Manager.SessionConfig.AccessKey)
	assert.Equal(t, false, s3Manager.SessionConfig.RoleBasedAuth)
}

func TestNewS3ManagerWithRole(t *testing.T) {
	s3Manager, err := NewS3Manager(map[string]interface{}{
		"bucketName": "someBucket",
		"region":     "someRegion",
		"iamRoleARN": "someIAMRole",
		"externalID": "someExternalID",
	})
	assert.Nil(t, err)
	assert.NotNil(t, s3Manager)
	assert.Equal(t, "someBucket", s3Manager.Config.Bucket)
	assert.Equal(t, aws.String("someRegion"), s3Manager.Config.Region)
	assert.Equal(t, "someIAMRole", s3Manager.SessionConfig.IAMRoleARN)
	assert.Equal(t, "someExternalID", s3Manager.SessionConfig.ExternalID)
	assert.Equal(t, true, s3Manager.SessionConfig.RoleBasedAuth)
}

func TestNewS3ManagerWithBothAccessKeysAndRole(t *testing.T) {
	s3Manager, err := NewS3Manager(map[string]interface{}{
		"bucketName":  "someBucket",
		"region":      "someRegion",
		"iamRoleARN":  "someIAMRole",
		"externalID":  "someExternalID",
		"accessKeyID": "someAccessKeyId",
		"accessKey":   "someSecretAccessKey",
	})
	assert.Nil(t, err)
	assert.NotNil(t, s3Manager)
	assert.Equal(t, "someBucket", s3Manager.Config.Bucket)
	assert.Equal(t, aws.String("someRegion"), s3Manager.Config.Region)
	assert.Equal(t, "someAccessKeyId", s3Manager.SessionConfig.AccessKeyID)
	assert.Equal(t, "someSecretAccessKey", s3Manager.SessionConfig.AccessKey)
	assert.Equal(t, "someIAMRole", s3Manager.SessionConfig.IAMRoleARN)
	assert.Equal(t, "someExternalID", s3Manager.SessionConfig.ExternalID)
	assert.Equal(t, true, s3Manager.SessionConfig.RoleBasedAuth)
}

func TestNewS3ManagerWithBothAccessKeysAndRoleButRoleBasedAuthFalse(t *testing.T) {
	s3Manager, err := NewS3Manager(map[string]interface{}{
		"bucketName":    "someBucket",
		"region":        "someRegion",
		"iamRoleARN":    "someIAMRole",
		"externalID":    "someExternalID",
		"accessKeyID":   "someAccessKeyId",
		"accessKey":     "someSecretAccessKey",
		"roleBasedAuth": false,
	})
	assert.Nil(t, err)
	assert.NotNil(t, s3Manager)
	assert.Equal(t, "someBucket", s3Manager.Config.Bucket)
	assert.Equal(t, aws.String("someRegion"), s3Manager.Config.Region)
	assert.Equal(t, "someAccessKeyId", s3Manager.SessionConfig.AccessKeyID)
	assert.Equal(t, "someSecretAccessKey", s3Manager.SessionConfig.AccessKey)
	assert.Equal(t, "someIAMRole", s3Manager.SessionConfig.IAMRoleARN)
	assert.Equal(t, "someExternalID", s3Manager.SessionConfig.ExternalID)
	assert.Equal(t, false, s3Manager.SessionConfig.RoleBasedAuth)
}

func TestGetSessionWithAccessKeys(t *testing.T) {
	s3Manager := S3Manager{
		Config: &S3Config{
			Bucket: "someBucket",
			Region: aws.String("someRegion"),
		},
		SessionConfig: &awsutils.SessionConfig{
			AccessKeyID: "someAccessKeyId",
			AccessKey:   "someSecretAccessKey",
			Region:      "someRegion",
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
			Bucket: "someBucket",
			Region: aws.String("someRegion"),
		},
		SessionConfig: &awsutils.SessionConfig{
			IAMRoleARN: "someIAMRole",
			ExternalID: "someExternalID",
			Region:     "someRegion",
		},
	}
	awsSession, err := s3Manager.getSession(context.TODO())
	assert.Nil(t, err)
	assert.NotNil(t, awsSession)
	assert.NotNil(t, s3Manager.session)
}
