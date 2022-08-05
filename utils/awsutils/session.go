package awsutils

import (
	"encoding/json"
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

func NewSessionConfig(destinationConfig interface{}, timeout time.Duration, serviceName string) (*SessionConfig, error) {
	sessionConfig := SessionConfig{}

	jsonConfig, err := json.Marshal(destinationConfig)
	if err != nil {
		return nil, fmt.Errorf("[%s] Error while marshalling destination config :: %w", serviceName, err)
	}
	err = json.Unmarshal(jsonConfig, &sessionConfig)
	if err != nil {
		return nil, fmt.Errorf("[%s] Error while unmarshalling destination config :: %w", serviceName, err)
	}
	sessionConfig.Timeout = timeout
	sessionConfig.Service = serviceName
	return &sessionConfig, nil
}
