package awsutils

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
)

type SessionConfig struct {
	Region      string
	AccessKeyID string
	AccessKey   string
	IAMRoleARN  string
	ExternalID  string
	Service     string
	Timeout     time.Duration
}

func createRoleSessionName(serviceName string) string {
	return fmt.Sprintf("rudderstack-aws-%s-access", strings.ToLower(serviceName))
}

func createDefaultSession(config *SessionConfig) *session.Session {
	return session.Must(session.NewSession(&aws.Config{
		HTTPClient: &http.Client{
			Timeout: config.Timeout,
		},
		Region: aws.String(config.Region),
	}))
}

func createCredentailsForRole(config *SessionConfig) *credentials.Credentials {
	hostSession := createDefaultSession(config)
	return stscreds.NewCredentials(hostSession, config.IAMRoleARN,
		func(p *stscreds.AssumeRoleProvider) {
			p.ExternalID = aws.String(config.ExternalID)
			p.RoleSessionName = createRoleSessionName(config.Service)
		})
}

func createCredentails(config *SessionConfig) *credentials.Credentials {
	if config.IAMRoleARN != "" {
		return createCredentailsForRole(config)
	} else if config.AccessKey != "" && config.AccessKeyID != "" {
		return credentials.NewStaticCredentials(config.AccessKeyID, config.AccessKey, "")
	}
	return nil
}

func CreateSession(config *SessionConfig) *session.Session {
	return session.Must(session.NewSession(&aws.Config{
		HTTPClient: &http.Client{
			Timeout: config.Timeout,
		},
		Region:      aws.String(config.Region),
		Credentials: createCredentails(config),
	}))
}

func NewSessionConfig(destinationConfig map[string]interface{}, timeout time.Duration, serviceName string) (*SessionConfig, error) {
	if destinationConfig == nil {
		return nil, errors.New("destinationConfig should not be nil")
	}
	sessionConfig := SessionConfig{}

	if region, ok := destinationConfig["Region"].(string); ok {
		sessionConfig.Region = region
	} else if region, ok := destinationConfig["region"].(string); ok {
		sessionConfig.Region = region
	}
	if roleArn, ok := destinationConfig["IAMRoleARN"].(string); ok {
		sessionConfig.IAMRoleARN = roleArn
	} else if roleArn, ok := destinationConfig["iamRoleARN"].(string); ok {
		sessionConfig.IAMRoleARN = roleArn
	}
	if externalID, ok := destinationConfig["ExternalID"].(string); ok {
		sessionConfig.ExternalID = externalID
	} else if externalID, ok := destinationConfig["externalID"].(string); ok {
		sessionConfig.ExternalID = externalID
	}
	if accessKeyID, ok := destinationConfig["AccessKeyID"].(string); ok {
		sessionConfig.AccessKeyID = accessKeyID
	} else if accessKeyID, ok := destinationConfig["accessKeyID"].(string); ok {
		sessionConfig.AccessKeyID = accessKeyID
	}
	if accessKey, ok := destinationConfig["AccessKey"].(string); ok {
		sessionConfig.AccessKey = accessKey
	} else if accessKey, ok := destinationConfig["accessKey"].(string); ok {
		sessionConfig.AccessKey = accessKey
	}
	sessionConfig.Timeout = timeout
	sessionConfig.Service = serviceName
	return &sessionConfig, nil
}
