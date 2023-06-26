package awsutils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-go-kit/awsutil"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
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

func TestNewSessionConfigWithNilDestConfig(t *testing.T) {
	serviceName := "kinesis"
	sessionConfig, err := NewSessionConfigForDestination(&backendconfig.DestinationT{}, httpTimeout, serviceName)
	assert.EqualError(t, err, "config should not be nil")
	assert.Nil(t, sessionConfig)
}

func TestNewSessionConfigWithAccessKey(t *testing.T) {
	serviceName := "kinesis"
	sessionConfig, err := NewSessionConfigForDestination(&destinationWithAccessKey, httpTimeout, serviceName)
	assert.Nil(t, err)
	assert.NotNil(t, sessionConfig)
	assert.Equal(t, *sessionConfig, awsutil.SessionConfig{
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
	assert.Equal(t, *sessionConfig, awsutil.SessionConfig{
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
	t.Run("Without RoleBasedAuth", func(t *testing.T) {
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
		assert.Equal(t, *sessionConfig, awsutil.SessionConfig{
			Region:        someRegion,
			RoleBasedAuth: true,
			IAMRoleARN:    someIAMRoleARN,
			ExternalID:    someWorkspaceID,
			Timeout:       &httpTimeout,
			Service:       serviceName,
		})
	})

	t.Run("With RoleBasedAuth false", func(t *testing.T) {
		destinationWithRole := backendconfig.DestinationT{
			Config: map[string]interface{}{
				"region":        someRegion,
				"roleBasedAuth": false,
				"iamRoleARN":    someIAMRoleARN,
			},
			WorkspaceID: someWorkspaceID,
		}
		sessionConfig, err := NewSessionConfigForDestination(&destinationWithRole, httpTimeout, serviceName)
		assert.Nil(t, err)
		assert.NotNil(t, sessionConfig)
		assert.Equal(t, *sessionConfig, awsutil.SessionConfig{
			Region:        someRegion,
			RoleBasedAuth: false,
			IAMRoleARN:    someIAMRoleARN,
			ExternalID:    someWorkspaceID,
			Timeout:       &httpTimeout,
			Service:       serviceName,
		})
	})
}

func TestNewSessionConfigWithRoleBasedAuth(t *testing.T) {
	serviceName := "s3"
	t.Run("invalid RoleBasedAuth flag", func(t *testing.T) {
		destinationWithRole := backendconfig.DestinationT{
			Config: map[string]interface{}{
				"region":        someRegion,
				"iamRoleARN":    someIAMRoleARN,
				"roleBasedAuth": "no", // should be bool
			},
			WorkspaceID: someWorkspaceID,
		}
		_, err := NewSessionConfigForDestination(&destinationWithRole, httpTimeout, serviceName)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "'roleBasedAuth' expected type 'bool'")
	})
	t.Run("With iamRoleARN", func(t *testing.T) {
		destinationWithRole := backendconfig.DestinationT{
			Config: map[string]interface{}{
				"region":        someRegion,
				"iamRoleARN":    someIAMRoleARN,
				"roleBasedAuth": true,
			},
			WorkspaceID: someWorkspaceID,
		}
		sessionConfig, err := NewSessionConfigForDestination(&destinationWithRole, httpTimeout, serviceName)
		assert.Nil(t, err)
		assert.NotNil(t, sessionConfig)
		assert.Equal(t, *sessionConfig, awsutil.SessionConfig{
			Region:        someRegion,
			RoleBasedAuth: true,
			IAMRoleARN:    someIAMRoleARN,
			ExternalID:    someWorkspaceID,
			Timeout:       &httpTimeout,
			Service:       serviceName,
		})
	})

	t.Run("Without iamRoleARN", func(t *testing.T) {
		destinationWithRole := backendconfig.DestinationT{
			Config: map[string]interface{}{
				"region":        someRegion,
				"roleBasedAuth": true,
			},
			WorkspaceID: someWorkspaceID,
		}
		_, err := NewSessionConfigForDestination(&destinationWithRole, httpTimeout, serviceName)
		assert.Nil(t, err)
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
	t.Run("With RoleBasedAuth but without ExternalID", func(t *testing.T) {
		sessionConfig := awsutil.SessionConfig{
			Region:        someRegion,
			RoleBasedAuth: true,
			IAMRoleARN:    someIAMRoleARN,
			Timeout:       &httpTimeout,
		}
		awsSession, err := awsutil.CreateSession(&sessionConfig)
		assert.NotNil(t, err)
		assert.Nil(t, awsSession)
		assert.EqualError(t, err, "externalID is required for IAM role")
	})

	t.Run("With RoleBasedAuth false and without ExternalID", func(t *testing.T) {
		sessionConfig := awsutil.SessionConfig{
			Region:        someRegion,
			RoleBasedAuth: false,
			IAMRoleARN:    someIAMRoleARN,
			Timeout:       &httpTimeout,
		}
		awsSession, err := awsutil.CreateSession(&sessionConfig)
		assert.Nil(t, err)
		assert.NotNil(t, awsSession)
	})

	t.Run("With RoleBasedAuth true auth and ExternalID", func(t *testing.T) {
		sessionConfig := awsutil.SessionConfig{
			Region:        someRegion,
			RoleBasedAuth: true,
			ExternalID:    someWorkspaceID,
			IAMRoleARN:    someIAMRoleARN,
			Timeout:       &httpTimeout,
		}
		awsSession, err := awsutil.CreateSession(&sessionConfig)
		assert.Nil(t, err)
		assert.NotNil(t, awsSession)
	})
}

func TestCreateSessionWithAccessKeys(t *testing.T) {
	sessionConfig := awsutil.SessionConfig{
		Region:      destinationWithAccessKey.Config["region"].(string),
		AccessKeyID: destinationWithAccessKey.Config["accessKeyID"].(string),
		AccessKey:   destinationWithAccessKey.Config["accessKey"].(string),
		Timeout:     &httpTimeout,
	}
	awsSession, err := awsutil.CreateSession(&sessionConfig)
	assert.Nil(t, err)
	assert.NotNil(t, awsSession)
	assert.NotNil(t, awsSession.Config.Credentials)
	assert.Equal(t, sessionConfig.Region, *awsSession.Config.Region)
	assert.Equal(t, *sessionConfig.Timeout, awsSession.Config.HTTPClient.Timeout)
}

func TestCreateSessionWithoutAccessKeysOrRole(t *testing.T) {
	sessionConfig := awsutil.SessionConfig{
		Region:  "someRegion",
		Timeout: &httpTimeout,
	}
	awsSession, err := awsutil.CreateSession(&sessionConfig)
	assert.Nil(t, err)
	assert.NotNil(t, awsSession)
	assert.NotNil(t, awsSession.Config.Credentials)
	assert.Equal(t, sessionConfig.Region, *awsSession.Config.Region)
	assert.Equal(t, *sessionConfig.Timeout, awsSession.Config.HTTPClient.Timeout)
}

func TestCreateSessionWithoutTimeout(t *testing.T) {
	sessionConfig := awsutil.SessionConfig{
		Region: "someRegion",
	}
	awsSession, err := awsutil.CreateSession(&sessionConfig)
	assert.Nil(t, err)
	assert.NotNil(t, awsSession)
	assert.NotNil(t, awsSession.Config.Credentials)
	// Http client created with defaults
	assert.NotNil(t, awsSession.Config.HTTPClient)
	assert.Equal(t, sessionConfig.Region, *awsSession.Config.Region)
}
