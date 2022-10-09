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
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
)

// Some AWS destinations are using SecretAccessKey instead of accessKey
type SessionConfig struct {
	Region           string         `mapstructure:"region"`
	AccessKeyID      string         `mapstructure:"accessKeyID"`
	AccessKey        string         `mapstructure:"accessKey"`
	SecretAccessKey  string         `mapstructure:"secretAccessKey"`
	IAMRoleARN       string         `mapstructure:"iamRoleARN"`
	ExternalID       string         `mapstructure:"externalID"`
	Endpoint         *string        `mapstructure:"endpoint"`
	S3ForcePathStyle *bool          `mapstructure:"s3ForcePathStyle"`
	DisableSSL       *bool          `mapstructure:"disableSSL"`
	Service          string         `mapstructure:"service"`
	Timeout          *time.Duration `mapstructure:"timeout"`
}

func createRoleSessionName(serviceName string) string {
	return fmt.Sprintf("rudderstack-aws-%s-access", strings.ToLower(serviceName))
}

func getHttpClient(config *SessionConfig) *http.Client {
	var httpClient *http.Client
	if config.Timeout != nil {
		httpClient = &http.Client{
			Timeout: *config.Timeout,
		}
	}
	return httpClient
}

func createDefaultSession(config *SessionConfig) (*session.Session, error) {
	return session.NewSession(&aws.Config{
		HTTPClient: getHttpClient(config),
		Region:     aws.String(config.Region),
	})
}

func createCredentailsForRole(config *SessionConfig) (*credentials.Credentials, error) {
	if config.ExternalID == "" {
		return nil, errors.New("externalID is required for IAM role")
	}
	hostSession, err := createDefaultSession(config)
	if err != nil {
		return nil, err
	}
	return stscreds.NewCredentials(hostSession, config.IAMRoleARN,
		func(p *stscreds.AssumeRoleProvider) {
			p.ExternalID = aws.String(config.ExternalID)
			p.RoleSessionName = createRoleSessionName(config.Service)
		}), err
}

func CreateSession(config *SessionConfig) (*session.Session, error) {
	var (
		awsCredentials *credentials.Credentials
		err            error
	)
	if config.IAMRoleARN != "" {
		awsCredentials, err = createCredentailsForRole(config)
	} else if config.AccessKey != "" && config.AccessKeyID != "" {
		awsCredentials, err = credentials.NewStaticCredentials(config.AccessKeyID, config.AccessKey, ""), nil
	}
	if err != nil {
		return nil, err
	}
	return session.NewSession(&aws.Config{
		HTTPClient:                    getHttpClient(config),
		Region:                        aws.String(config.Region),
		CredentialsChainVerboseErrors: aws.Bool(true),
		Credentials:                   awsCredentials,
		Endpoint:                      config.Endpoint,
		S3ForcePathStyle:              config.S3ForcePathStyle,
		DisableSSL:                    config.DisableSSL,
	})
}

func NewSimpleSessionConfigForDestination(destination *backendconfig.DestinationT, serviceName string) (*SessionConfig, error) {
	if destination == nil {
		return nil, errors.New("destination should not be nil")
	}
	sessionConfig := SessionConfig{}
	if err := mapstructure.Decode(destination.Config, &sessionConfig); err != nil {
		return nil, fmt.Errorf("unable to populate session config using destinationConfig: %w", err)
	}
	// Some AWS destinations are using SecretAccessKey instead of accessKey
	if sessionConfig.SecretAccessKey != "" {
		sessionConfig.AccessKey = sessionConfig.SecretAccessKey
	}
	sessionConfig.Service = serviceName
	if sessionConfig.IAMRoleARN != "" {
		/**
		In order prevent confused deputy problem, we are using
		workspace token as external ID.
		Ref: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
		*/
		sessionConfig.ExternalID = destination.WorkspaceID
	}
	return &sessionConfig, nil
}

func NewSessionConfigForDestination(destination *backendconfig.DestinationT, timeout time.Duration, serviceName string) (*SessionConfig, error) {
	sessionConfig, err := NewSimpleSessionConfigForDestination(destination, serviceName)
	if err != nil {
		return nil, err
	}
	if sessionConfig.Region == "" {
		return nil, errors.New("could not find region configuration")
	}
	sessionConfig.Timeout = &timeout
	return sessionConfig, nil
}
