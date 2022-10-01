package awsutils

import (
	"testing"
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/stretchr/testify/assert"
)

var (
	someWorkspaceID          string                     = "workspaceID"
	someAccessKey            string                     = "accessKey"
	someSecretAccessKey      string                     = "secretAccessKey"
	someAccessKeyID          string                     = "accessKeyID"
	someRegion               string                     = "region"
	someIAMRoleARN           string                     = "iamRoleArn"
	destinationWithAccessKey backendconfig.DestinationT = backendconfig.DestinationT{
		Config: map[string]interface{}{
			"region":      someRegion,
			"accessKeyID": someAccessKeyID,
			"accessKey":   someAccessKey,
		},
		WorkspaceID: someWorkspaceID,
	}
	httpTimeout time.Duration = 10 * time.Second
)

func TestNewSessionConfigWithAccessKey(t *testing.T) {
	serviceName := "kinesis"
	sessionConfig, err := NewSessionConfigForDestination(&destinationWithAccessKey, httpTimeout, serviceName)
	assert.Nil(t, err)
	assert.NotNil(t, sessionConfig)
	assert.Equal(t, *sessionConfig, SessionConfig{
		Region:      someRegion,
		AccessKeyID: someAccessKeyID,
		AccessKey:   someAccessKey,
		Timeout:     &httpTimeout,
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
	sessionConfig, err := NewSessionConfigForDestination(&destinationWithSecretAccessKey, httpTimeout, serviceName)
	assert.Nil(t, err)
	assert.NotNil(t, sessionConfig)
	assert.Equal(t, *sessionConfig, SessionConfig{
		Region:          someRegion,
		AccessKeyID:     someAccessKeyID,
		AccessKey:       someSecretAccessKey,
		SecretAccessKey: someSecretAccessKey,
		Timeout:         &httpTimeout,
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
	sessionConfig, err := NewSessionConfigForDestination(&destinationWithRole, httpTimeout, serviceName)
	assert.Nil(t, err)
	assert.NotNil(t, sessionConfig)
	assert.Equal(t, *sessionConfig, SessionConfig{
		Region:     someRegion,
		IAMRoleARN: someIAMRoleARN,
		ExternalID: someWorkspaceID,
		Timeout:    &httpTimeout,
		Service:    serviceName,
	})
}

func TestNewSimpleSessionConfigWithoutRegion(t *testing.T) {
	serviceName := "s3"
	destinationWithRole := backendconfig.DestinationT{
		Config:      map[string]interface{}{},
		WorkspaceID: someWorkspaceID,
	}
	sessionConfig, err := NewSimpleSessionConfigForDestination(&destinationWithRole, serviceName)
	assert.Nil(t, err)
	assert.NotNil(t, sessionConfig)
}

func TestNewSessionConfigWithoutRegion(t *testing.T) {
	serviceName := "s3"
	destinationWithRole := backendconfig.DestinationT{
		Config:      map[string]interface{}{},
		WorkspaceID: someWorkspaceID,
	}
	sessionConfig, err := NewSessionConfigForDestination(&destinationWithRole, httpTimeout, serviceName)
	assert.NotNil(t, err)
	assert.Nil(t, sessionConfig)
	assert.EqualError(t, err, "could not find region configuration")
}

func TestNewSessionConfigWithBadDestination(t *testing.T) {
	serviceName := "s3"
	sessionConfig, err := NewSessionConfigForDestination(nil, httpTimeout, serviceName)
	assert.Equal(t, "destination should not be nil", err.Error())
	assert.Nil(t, sessionConfig)
}

func TestCreateSessionWithRole(t *testing.T) {
	sessionConfig := SessionConfig{
		Region:     someRegion,
		IAMRoleARN: someIAMRoleARN,
		ExternalID: someWorkspaceID,
		Timeout:    &httpTimeout,
	}
	awsSession, err := CreateSession(&sessionConfig)
	assert.Nil(t, err)
	assert.NotNil(t, awsSession)
	assert.NotNil(t, awsSession.Config.Credentials)
	assert.Equal(t, sessionConfig.Region, *awsSession.Config.Region)
	assert.Equal(t, *sessionConfig.Timeout, awsSession.Config.HTTPClient.Timeout)
}

func TestCreateSessionWithRoleButWithoutExternalID(t *testing.T) {
	sessionConfig := SessionConfig{
		Region:     someRegion,
		IAMRoleARN: someIAMRoleARN,
		Timeout:    &httpTimeout,
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
		Timeout:     &httpTimeout,
	}
	awsSession, err := CreateSession(&sessionConfig)
	assert.Nil(t, err)
	assert.NotNil(t, awsSession)
	assert.NotNil(t, awsSession.Config.Credentials)
	assert.Equal(t, sessionConfig.Region, *awsSession.Config.Region)
	assert.Equal(t, *sessionConfig.Timeout, awsSession.Config.HTTPClient.Timeout)
}

func TestCreateSessionWithoutAccessKeysOrRole(t *testing.T) {
	sessionConfig := SessionConfig{
		Region:  "someRegion",
		Timeout: &httpTimeout,
	}
	awsSession, err := CreateSession(&sessionConfig)
	assert.Nil(t, err)
	assert.NotNil(t, awsSession)
	assert.NotNil(t, awsSession.Config.Credentials)
	assert.Equal(t, sessionConfig.Region, *awsSession.Config.Region)
	assert.Equal(t, *sessionConfig.Timeout, awsSession.Config.HTTPClient.Timeout)
}

func TestCreateSessionWithoutTimeout(t *testing.T) {
	sessionConfig := SessionConfig{
		Region: "someRegion",
	}
	awsSession, err := CreateSession(&sessionConfig)
	assert.Nil(t, err)
	assert.NotNil(t, awsSession)
	assert.NotNil(t, awsSession.Config.Credentials)
	// Http client created with defaults
	assert.NotNil(t, awsSession.Config.HTTPClient)
	assert.Equal(t, sessionConfig.Region, *awsSession.Config.Region)
}
