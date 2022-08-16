package awsutils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	destinationConfigWithRole map[string]interface{} = map[string]interface{}{
		"region":     "us-east-1",
		"iamRoleARN": "role-arn",
		"externalID": "ExternalID",
	}
	destinationConfigWithAccessKey map[string]interface{} = map[string]interface{}{
		"region":      "us-east-1",
		"accessKeyID": "AccessKeyID",
		"accessKey":   "AccessKey",
	}
	timeOut time.Duration = 10 * time.Second
)

func TestNewSessionConfigWithAccessKey(t *testing.T) {
	serviceName := "kinesis"
	sessionConfig, err := NewSessionConfig(destinationConfigWithAccessKey, timeOut, serviceName)
	assert.Nil(t, err)
	assert.NotNil(t, sessionConfig)
	assert.Equal(t, *sessionConfig, SessionConfig{
		Region:      destinationConfigWithAccessKey["region"].(string),
		AccessKeyID: destinationConfigWithAccessKey["accessKeyID"].(string),
		AccessKey:   destinationConfigWithAccessKey["accessKey"].(string),
		Timeout:     timeOut,
		Service:     serviceName,
	})
}

func TestNewSessionConfigWithRole(t *testing.T) {
	serviceName := "s3"
	sessionConfig, err := NewSessionConfig(destinationConfigWithRole, timeOut, serviceName)
	assert.Nil(t, err)
	assert.NotNil(t, sessionConfig)
	assert.Equal(t, *sessionConfig, SessionConfig{
		Region:     destinationConfigWithRole["region"].(string),
		IAMRoleARN: destinationConfigWithRole["iamRoleARN"].(string),
		ExternalID: destinationConfigWithRole["externalID"].(string),
		Timeout:    timeOut,
		Service:    serviceName,
	})
}

func TestNewSessionConfigBadConfig(t *testing.T) {
	serviceName := "s3"
	sessionConfig, err := NewSessionConfig(nil, timeOut, serviceName)
	assert.Equal(t, "destinationConfig should not be nil", err.Error())
	assert.Nil(t, sessionConfig)
}

func TestCreateSessionWithRole(t *testing.T) {
	sessionConfig := SessionConfig{
		Region:     destinationConfigWithRole["region"].(string),
		IAMRoleARN: destinationConfigWithRole["iamRoleARN"].(string),
		ExternalID: destinationConfigWithRole["externalID"].(string),
		Timeout:    10 * time.Second,
	}
	awsSession, err := CreateSession(&sessionConfig)
	assert.Nil(t, err)
	assert.NotNil(t, awsSession)
	assert.NotNil(t, awsSession.Config.Credentials)
	assert.Equal(t, sessionConfig.Region, *awsSession.Config.Region)
	assert.Equal(t, sessionConfig.Timeout, awsSession.Config.HTTPClient.Timeout)
}

func TestCreateSessionWithAccessKeys(t *testing.T) {
	sessionConfig := SessionConfig{
		Region:      destinationConfigWithAccessKey["region"].(string),
		AccessKeyID: destinationConfigWithAccessKey["accessKeyID"].(string),
		AccessKey:   destinationConfigWithAccessKey["accessKey"].(string),
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
		Region:  destinationConfigWithAccessKey["region"].(string),
		Timeout: 10 * time.Second,
	}
	awsSession, err := CreateSession(&sessionConfig)
	assert.Nil(t, err)
	assert.NotNil(t, awsSession)
	assert.NotNil(t, awsSession.Config.Credentials)
	assert.Equal(t, sessionConfig.Region, *awsSession.Config.Region)
	assert.Equal(t, sessionConfig.Timeout, awsSession.Config.HTTPClient.Timeout)
}
