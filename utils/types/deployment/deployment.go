package deployment

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/logger"

	"github.com/rudderlabs/rudder-server/config"
)

type Type string // skipcq: RVV-B0009

const (
	DedicatedType   Type = "DEDICATED"
	MultiTenantType Type = "MULTITENANT"
)

const defaultClusterType = DedicatedType

var pkgLogger = logger.NewLogger().Child("deployment")

func GetFromEnv() (Type, error) {
	t := Type(config.GetEnv("DEPLOYMENT_TYPE", ""))
	if t == "" {
		t = defaultClusterType
	}
	if !t.Valid() {
		return "", fmt.Errorf("Invalid deployment type: %q", t)
	}

	return t, nil
}

func (t Type) Valid() bool {
	if t == DedicatedType || t == MultiTenantType {
		return true
	}
	return false
}

func GetConnectionIdentifier() (string, bool, error) {
	deploymentType, err := GetFromEnv()
	if err != nil {
		pkgLogger.Errorf("error getting deployment type: %s", err.Error())
		return "", false, err
	}
	var connectionIdentifier string
	var isMultiWorkspace bool
	switch deploymentType {
	case DedicatedType:
		connectionIdentifier = config.GetWorkspaceToken()
	case MultiTenantType:
		isMultiWorkspace = true
		isNamespaced := config.IsEnvSet("WORKSPACE_NAMESPACE")
		if isNamespaced {
			connectionIdentifier, err = config.GetEnvErr("WORKSPACE_NAMESPACE")
			if err != nil {
				pkgLogger.Errorf("error getting workspace namespace: %s", err.Error())
				return "", false, err
			}
		} else {
			connectionIdentifier, err = config.GetEnvErr("HOSTED_SERVICE_SECRET")
			if err != nil {
				pkgLogger.Errorf("error getting hosted service secret: %s", err.Error())
				return "", false, err
			}
		}
	}
	return connectionIdentifier, isMultiWorkspace, nil
}
