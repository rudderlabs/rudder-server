package deployment

import (
	"fmt"
	"os"

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
	t := Type(os.Getenv("DEPLOYMENT_TYPE"))
	if t == "" {
		t = defaultClusterType
		if config.GetEnvAsBool("HOSTED_SERVICE", false) {
			t = HostedType
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
