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
	"github.com/mitchellh/mapstructure"
)

type SessionConfig struct {
	Region      string `mapstructure:"region"`
	AccessKeyID string `mapstructure:"accessKeyID"`
	AccessKey   string `mapstructure:"accessKey"`
	IAMRoleARN  string `mapstructure:"iamRoleARN"`
	ExternalID  string `mapstructure:"externalID"`
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
	err := mapstructure.Decode(destinationConfig, &sessionConfig)
	if err != nil {
		return nil, errors.New("unable to populate session config using destinationConfig")
	}
	sessionConfig.Timeout = timeout
	sessionConfig.Service = serviceName
	return &sessionConfig, nil
}
