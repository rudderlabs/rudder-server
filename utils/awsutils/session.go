package awsutils

import (
	"errors"
	"time"

	"github.com/rudderlabs/rudder-go-kit/awsutil"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

func NewSimpleSessionConfigForDestination(destination *backendconfig.DestinationT, serviceName string) (*awsutil.SessionConfig, error) {
	if destination == nil {
		return nil, errors.New("destination should not be nil")
	}
	sessionConfig, err := awsutil.NewSimpleSessionConfig(destination.Config, serviceName)
	if err != nil {
		return nil, err
	}
	if sessionConfig.IAMRoleARN != "" && sessionConfig.ExternalID == "" {
		/**
		In order prevent confused deputy problem, we are using
		workspace token as external ID.
		Ref: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
		*/
		sessionConfig.ExternalID = destination.WorkspaceID
	}
	return sessionConfig, nil
}

func NewSessionConfigForDestination(destination *backendconfig.DestinationT, timeout time.Duration, serviceName string) (*awsutil.SessionConfig, error) {
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
