package awsutils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewSessionConfigWithAccessKey(t *testing.T) {
	destinationConfig := map[string]string{
		"Region":      "us-east-1",
		"AccessKeyID": "sampleAccessId",
		"AccessKey":   "sampleAccess",
	}
	timeOut := 10 * time.Second
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
	destinationConfig := map[string]string{
		"Region":     "us-east-1",
		"IAMRoleARN": "sampleRoleArn",
		"ExternalID": "sampleExternalID",
	}
	timeOut := 10 * time.Second
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
	destinationConfig := "Bad config"
	timeOut := 10 * time.Second
	serviceName := "s3"
	sessionConfig, err := NewSessionConfig(destinationConfig, timeOut, serviceName)
	assert.NotNil(t, err)
	assert.Nil(t, sessionConfig)
}

func TestCreateSession(t *testing.T) {
	sessionConfig := SessionConfig{Region: "us-east-1", IAMRoleARN: "sampleRoleArn",
		ExternalID: "sampleExternalID", Timeout: 10 * time.Second}
	awsSession := CreateSession(&sessionConfig)
	assert.NotNil(t, awsSession)
	assert.NotNil(t, awsSession.Config.Credentials)
	assert.Equal(t, sessionConfig.Region, *awsSession.Config.Region)
	assert.Equal(t, sessionConfig.Timeout, awsSession.Config.HTTPClient.Timeout)
}
