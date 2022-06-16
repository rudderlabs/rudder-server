package deployment

import (
	"fmt"
	"github.com/rudderlabs/rudder-server/config"
)

type Type string

const (
	DedicatedType   Type = "DEDICATED"
	HostedType      Type = "HOSTED"
	MultiTenantType Type = "MULTITENANT"
)

const defaultClusterType = DedicatedType

func GetFromEnv() (Type, error) {
	t := Type(config.GetEnv("DEPLOYMENT_TYPE", ""))
	if t == "" {
		if config.GetEnvAsBool("HOSTED_SERVICE", false) {
			t = HostedType
		} else {
			t = defaultClusterType
		}
	}
	if !t.Valid() {
		return "", fmt.Errorf("Invalid deployment type: %q", t)
	}

	return t, nil

}

func (t Type) Valid() bool {
	if t == DedicatedType || t == HostedType || t == MultiTenantType {
		return true
	}
	return false
}
