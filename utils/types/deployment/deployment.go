package deployment

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/config"
)

type Type string // skipcq: RVV-B0009

const (
	DedicatedType   Type = "DEDICATED"
	MultiTenantType Type = "MULTITENANT"
)

const defaultClusterType = DedicatedType

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
