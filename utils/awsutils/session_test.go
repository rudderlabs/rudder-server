package awsutils

import (
	"testing"
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/stretchr/testify/assert"
)

var (
	someWorkspaceID     string = "workspaceID"
	someAccessKey       string = "accessKey"
	someSecretAccessKey string = "secretAccessKey"
	someAccessKeyID     string = "accessKeyID"
	someRegion          string = "region"
	someIAMRoleARN      string = "iamRoleArn"

	destinationWithAccessKey backendconfig.DestinationT = backendconfig.DestinationT{
		Config: map[string]interface{}{
			"region":      someRegion,
			"accessKeyID": someAccessKeyID,
			"accessKey":   someAccessKey,
		},
		WorkspaceID: someWorkspaceID,
	}

	timeOut time.Duration = 10 * time.Second
)

func TestNewSessionConfigWithAccessKey(t *testing.T) {
	serviceName := "kinesis"
	sessionConfig, err := NewSessionConfigForDestination(&destinationWithAccessKey, timeOut, serviceName)
	assert.Nil(t, err)
	assert.NotNil(t, sessionConfig)
	assert.Equal(t, *sessionConfig, SessionConfig{
		Region:      someRegion,
		AccessKeyID: someAccessKeyID,
		AccessKey:   someAccessKey,
		Timeout:     timeOut,
		Service:     serviceName,
	})
}

func TestNewSessionConfigWithSecretAccessKey(t *testing.T) {
	serviceName := "kinesis"
	destinationWithSecretAccessKey := backendconfig.DestinationT{
		Config: map[string]interface{}{
			"region":          someRegion,
			"accessKeyID":     someAccessKeyID,
			"secretAccessKey": someSecretAccessKey,
		},
		WorkspaceID: someWorkspaceID,
	}
	sessionConfig, err := NewSessionConfigForDestination(&destinationWithSecretAccessKey, timeOut, serviceName)
	assert.Nil(t, err)
	assert.NotNil(t, sessionConfig)
	assert.Equal(t, *sessionConfig, SessionConfig{
		Region:          someRegion,
		AccessKeyID:     someAccessKeyID,
		AccessKey:       someSecretAccessKey,
		SecretAccessKey: someSecretAccessKey,
		Timeout:         timeOut,
		Service:         serviceName,
	})
}

func TestNewSessionConfigWithRole(t *testing.T) {
	serviceName := "s3"
	destinationWithRole := backendconfig.DestinationT{
		Config: map[string]interface{}{
			"region":     someRegion,
			"iamRoleARN": someIAMRoleARN,
		},
		WorkspaceID: someWorkspaceID,
	}
	sessionConfig, err := NewSessionConfigForDestination(&destinationWithRole, timeOut, serviceName)
	assert.Nil(t, err)
	assert.NotNil(t, sessionConfig)
	assert.Equal(t, *sessionConfig, SessionConfig{
		Region:     someRegion,
		IAMRoleARN: someIAMRoleARN,
		ExternalID: someWorkspaceID,
		Timeout:    timeOut,
		Service:    serviceName,
	})
}

func TestNewSessionConfigWithBadDestination(t *testing.T) {
	serviceName := "s3"
	sessionConfig, err := NewSessionConfigForDestination(nil, timeOut, serviceName)
	assert.Equal(t, "destination should not be nil", err.Error())
	assert.Nil(t, sessionConfig)
}

func TestCreateSessionWithRole(t *testing.T) {
	sessionConfig := SessionConfig{
		Region:     someRegion,
		IAMRoleARN: someIAMRoleARN,
		ExternalID: someWorkspaceID,
		Timeout:    10 * time.Second,
	}
	awsSession, err := CreateSession(&sessionConfig)
	assert.Nil(t, err)
	assert.NotNil(t, awsSession)
	assert.NotNil(t, awsSession.Config.Credentials)
	assert.Equal(t, sessionConfig.Region, *awsSession.Config.Region)
	assert.Equal(t, sessionConfig.Timeout, awsSession.Config.HTTPClient.Timeout)
}

func TestCreateSessionWithRoleButWithoutExternalID(t *testing.T) {
	sessionConfig := SessionConfig{
		Region:     someRegion,
		IAMRoleARN: someIAMRoleARN,
		Timeout:    10 * time.Second,
	}
	awsSession, err := CreateSession(&sessionConfig)
	assert.NotNil(t, err)
	assert.Nil(t, awsSession)
	assert.EqualError(t, err, "externalID is required for IAM role")
}

func TestCreateSessionWithAccessKeys(t *testing.T) {
	sessionConfig := SessionConfig{
		Region:      destinationWithAccessKey.Config["region"].(string),
		AccessKeyID: destinationWithAccessKey.Config["accessKeyID"].(string),
		AccessKey:   destinationWithAccessKey.Config["accessKey"].(string),
		Timeout:     10 * time.Second,
	}
	awsSession, err := CreateSession(&sessionConfig)
	assert.Nil(t, err)
	assert.NotNil(t, awsSession)
	assert.NotNil(t, awsSession.Config.Credentials)
	assert.Equal(t, sessionConfig.Region, *awsSession.Config.Region)
	assert.Equal(t, sessionConfig.Timeout, awsSession.Config.HTTPClient.Timeout)
}

func TestCreateSessionWithoutAccessKeysOrRole(t *testing.T) {
	sessionConfig := SessionConfig{
		Region:  "someRegion",
		Timeout: 10 * time.Second,
	}
	awsSession, err := CreateSession(&sessionConfig)
	assert.Nil(t, err)
	assert.NotNil(t, awsSession)
	assert.NotNil(t, awsSession.Config.Credentials)
	assert.Equal(t, sessionConfig.Region, *awsSession.Config.Region)
	assert.Equal(t, sessionConfig.Timeout, awsSession.Config.HTTPClient.Timeout)
}

func TestCreateSessionWithoutRegion(t *testing.T) {
	sessionConfig := SessionConfig{
		Region:  "someRegion",
		Timeout: 10 * time.Second,
	}
	awsSession, err := CreateSession(&sessionConfig)
	assert.Nil(t, err)
	assert.NotNil(t, awsSession)
	assert.NotNil(t, awsSession.Config.Credentials)
	assert.Equal(t, sessionConfig.Region, *awsSession.Config.Region)
	assert.Equal(t, sessionConfig.Timeout, awsSession.Config.HTTPClient.Timeout)
}
