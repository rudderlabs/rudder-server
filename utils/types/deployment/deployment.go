package deployment

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/logger"

	"github.com/rudderlabs/rudder-server/config"
)

type Type string // skipcq: RVV-B0009

const (
	DedicatedType   Type   = "DEDICATED"
	MultiTenantType Type   = "MULTITENANT"
	hostedNamespace string = "free-us-1" // Having it here to support legacy cp-router hosted
)

// Types of tokens that can be used to authenticate with CP router
const (
	WorkspaceToken = "WORKSPACE_TOKEN"
	Namespace      = "NAMESPACE"
)

const defaultClusterType = DedicatedType

var pkgLogger = logger.NewLogger().Child("deployment")

func GetFromEnv() (Type, error) {
	t := Type(config.GetEnv("DEPLOYMENT_TYPE", ""))
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

func GetConnectionToken() (string, string, bool, error) {
	deploymentType, err := GetFromEnv()
	if err != nil {
		pkgLogger.Errorf("error getting deployment type: %v", err)
		return "", "", false, err
	}
	var connectionToken, tokenType string
	var isMultiWorkspace bool
	switch deploymentType {
	case DedicatedType:
		connectionToken = config.GetWorkspaceToken()
		tokenType = WorkspaceToken
	case MultiTenantType:
		isMultiWorkspace = true
		tokenType = Namespace
		isNamespaced := config.IsEnvSet("WORKSPACE_NAMESPACE")
		if isNamespaced {
			connectionToken, err = config.GetEnvErr("WORKSPACE_NAMESPACE")
			if err != nil {
				pkgLogger.Errorf("error getting workspace namespace: %v", err)
				return "", "", false, err
			}
			if connectionToken == hostedNamespace {
				// CP Router still has some things hardcoded for hosted
				// which needs to be supported
				connectionToken = config.GetEnv("HOSTED_SERVICE_SECRET", "")
			}
		} else {
			connectionToken, err = config.GetEnvErr("HOSTED_SERVICE_SECRET")
			if err != nil {
				pkgLogger.Errorf("error getting hosted service secret: %v", err)
				return "", "", false, err
			}
		}
	}
	return connectionToken, tokenType, isMultiWorkspace, nil
}
