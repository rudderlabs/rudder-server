package awsutils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	destinationConfig map[string]string = map[string]string{
		"Region":      "us-east-1",
		"AccessKeyID": "sampleAccessId",
		"AccessKey":   "sampleAccess",
	}
	timeOut time.Duration = 10 * time.Second
)

func TestNewSessionConfigWithAccessKey(t *testing.T) {
	serviceName := "kinesis"
	sessionConfig, err := NewSessionConfig(destinationConfig, timeOut, serviceName)
	assert.Nil(t, err)
	assert.NotNil(t, sessionConfig)
	assert.Equal(t, *sessionConfig, SessionConfig{
		Region:      destinationConfig["Region"],
		AccessKeyID: destinationConfig["AccessKeyID"],
		AccessKey:   destinationConfig["AccessKey"],
		Timeout:     timeOut,
		Service:     serviceName,
	})
}

func TestNewSessionConfigWithRole(t *testing.T) {
	serviceName := "s3"
	sessionConfig, err := NewSessionConfig(destinationConfig, timeOut, serviceName)
	assert.Nil(t, err)
	assert.NotNil(t, sessionConfig)
	assert.Equal(t, *sessionConfig, SessionConfig{
		Region:     destinationConfig["Region"],
		IAMRoleARN: destinationConfig["IAMRoleARN"],
		ExternalID: destinationConfig["ExternalID"],
		Timeout:    timeOut,
		Service:    serviceName,
	})
}

func TestNewSessionConfigBadConfig(t *testing.T) {
	serviceName := "s3"
	sessionConfig, err := NewSessionConfig("Bad config", timeOut, serviceName)
	assert.Contains(t, err.Error(), "marshalling")
	assert.Nil(t, sessionConfig)
}

func TestCreateSession(t *testing.T) {
	sessionConfig := SessionConfig{
		Region:     destinationConfig["Region"],
		IAMRoleARN: destinationConfig["IAMRoleARN"],
		ExternalID: destinationConfig["ExternalID"],
		Timeout:    10 * time.Second,
	}
	awsSession := CreateSession(&sessionConfig)
	assert.NotNil(t, awsSession)
	assert.NotNil(t, awsSession.Config.Credentials)
	assert.Equal(t, sessionConfig.Region, *awsSession.Config.Region)
	assert.Equal(t, sessionConfig.Timeout, awsSession.Config.HTTPClient.Timeout)
}
