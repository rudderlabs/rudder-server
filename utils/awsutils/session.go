package awsutils

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/mitchellh/mapstructure"
)

type SessionConfig struct {
	Region           string        `mapstructure:"region"`
	AccessKeyID      string        `mapstructure:"accessKeyID"`
	AccessKey        string        `mapstructure:"accessKey"`
	IAMRoleARN       string        `mapstructure:"iamRoleARN"`
	ExternalID       string        `mapstructure:"externalID"`
	Endpoint         *string       `mapstructure:"endpoint"`
	S3ForcePathStyle *bool         `mapstructure:"s3ForcePathStyle"`
	DisableSSL       *bool         `mapstructure:"disableSSL"`
	Service          string        `mapstructure:"service"`
	Timeout          time.Duration `mapstructure:"timeout"`
}

func createRoleSessionName(serviceName string) string {
	return fmt.Sprintf("rudderstack-aws-%s-access", strings.ToLower(serviceName))
}

func createDefaultSession(config *SessionConfig) (*session.Session, error) {
	return session.NewSession(&aws.Config{
		HTTPClient: &http.Client{
			Timeout: config.Timeout,
		},
		Region: aws.String(config.Region),
	})
}

func createCredentailsForRole(config *SessionConfig) (*credentials.Credentials, error) {
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

func getHostCredentials(config *SessionConfig) (*credentials.Credentials, error) {
	hostSession, err := createDefaultSession(config)
	if err != nil {
		return nil, err
	}
	return credentials.NewChainCredentials(
		[]credentials.Provider{
			&credentials.EnvProvider{},
			&ec2rolecreds.EC2RoleProvider{
				Client: ec2metadata.New(hostSession),
			},
		}), nil
}

func createCredentails(config *SessionConfig) (*credentials.Credentials, error) {
	if config.IAMRoleARN != "" {
		return createCredentailsForRole(config)
	} else if config.AccessKey != "" && config.AccessKeyID != "" {
		return credentials.NewStaticCredentials(config.AccessKeyID, config.AccessKey, ""), nil
	}
	return getHostCredentials(config)
}

func CreateSession(config *SessionConfig) (*session.Session, error) {
	awsCredentials, err := createCredentails(config)
	if err != nil {
		return nil, err
	}
	return session.NewSession(&aws.Config{
		HTTPClient: &http.Client{
			Timeout: config.Timeout,
		},
		Region:           aws.String(config.Region),
		Credentials:      awsCredentials,
		Endpoint:         config.Endpoint,
		S3ForcePathStyle: config.S3ForcePathStyle,
		DisableSSL:       config.DisableSSL,
	})
}

func NewSessionConfig(destinationConfig map[string]interface{}, timeout time.Duration, serviceName string) (*SessionConfig, error) {
	if destinationConfig == nil {
		return nil, errors.New("destinationConfig should not be nil")
	}
	sessionConfig := SessionConfig{}
	if err := mapstructure.Decode(destinationConfig, &sessionConfig); err != nil {
		return nil, fmt.Errorf("unable to populate session config using destinationConfig: %w", err)
	}
	sessionConfig.Timeout = timeout
	sessionConfig.Service = serviceName
	return &sessionConfig, nil
}
