package deployment

import (
	"errors"
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-go-kit/config"
)

type Type string // skipcq: RVV-B0009

const (
	DedicatedType   Type   = "DEDICATED"
	MultiTenantType Type   = "MULTITENANT"
	hostedNamespace string = "free-us-1" // Having it here to support legacy cp-router hosted
)

// Types of tokens that can be used to authenticate with CP router
const (
	workspaceToken = "WORKSPACE_TOKEN"
	namespace      = "NAMESPACE"
)

const defaultClusterType = DedicatedType

var pkgLogger = logger.NewLogger().Child("deployment")

func GetType(conf *config.Config) (Type, error) {
	t := Type(conf.GetString("DEPLOYMENT_TYPE", ""))
	if t == "" {
		t = defaultClusterType
	}
	if !t.Valid() {
		return "", fmt.Errorf("invalid deployment type: %q", t)
	}

	return t, nil
}

func (t Type) Valid() bool {
	if t == DedicatedType || t == MultiTenantType {
		return true
	}
	return false
}

func GetConnectionToken(conf *config.Config) (string, string, bool, error) {
	deploymentType, err := GetType(conf)
	if err != nil {
		pkgLogger.Errorf("error getting deployment type: %v", err)
		return "", "", false, err
	}
	var connectionToken, tokenType string
	var isMultiWorkspace bool
	switch deploymentType {
	case DedicatedType:
		connectionToken = GetWorkspaceToken(conf)
		tokenType = workspaceToken
	case MultiTenantType:
		isMultiWorkspace = true
		tokenType = namespace
		isNamespaced := conf.IsSet("WORKSPACE_NAMESPACE")
		if isNamespaced {
			connectionToken = conf.GetString("WORKSPACE_NAMESPACE", "")
			if connectionToken == hostedNamespace {
				// CP Router still has some things hardcoded for hosted
				// which needs to be supported
				connectionToken = conf.GetString("HOSTED_SERVICE_SECRET", "")
			}
		} else {
			if !conf.IsSet("HOSTED_SERVICE_SECRET") {
				pkgLogger.Error("hosted service secret not set")
				return "", "", false, errors.New("hosted service secret not set")
			}
			connectionToken = config.GetString("HOSTED_SERVICE_SECRET", "")
		}
	}
	return connectionToken, tokenType, isMultiWorkspace, nil
}

func GetWorkspaceToken(conf *config.Config) string {
	token := conf.GetString("WORKSPACE_TOKEN", "")
	if token != "" && token != "<your_token_here>" {
		return token
	}
	return conf.GetString("CONFIG_BACKEND_TOKEN", "")
}
