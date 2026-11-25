package deployment

import (
	"errors"
	"fmt"

	"github.com/rudderlabs/rudder-cp-sdk/tokenfile"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

type Type string // skipcq: RVV-B0009

const (
	DedicatedType   Type = "DEDICATED"
	MultiTenantType Type = "MULTITENANT"
)

// Types of tokens that can be used to authenticate with CP router
const (
	workspaceToken = "WORKSPACE_TOKEN"
	namespace      = "NAMESPACE"
)

const defaultClusterType = DedicatedType

var pkgLogger = logger.NewLogger().Child("deployment")

func GetFromEnv() (Type, error) {
	t := Type(config.GetStringVar("", "DEPLOYMENT_TYPE"))
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
		pkgLogger.Errorn("error getting deployment type", obskit.Error(err))
		return "", "", false, err
	}
	var connectionToken, tokenType string
	var isMultiWorkspace bool
	switch deploymentType {
	case DedicatedType:
		connectionToken = config.GetWorkspaceToken()
		tokenType = workspaceToken
	case MultiTenantType:
		isMultiWorkspace = true
		tokenType = namespace
		isNamespaced := config.IsSet("WORKSPACE_NAMESPACE")
		if isNamespaced {
			connectionToken = config.GetStringVar("", "WORKSPACE_NAMESPACE")
		} else {
			if !config.IsSet("HOSTED_SERVICE_SECRET") {
				pkgLogger.Errorn("hosted service secret not set")
				return "", "", false, errors.New("hosted service secret not set")
			}
			connectionToken = config.GetStringVar("", "HOSTED_SERVICE_SECRET")
		}
	}
	return connectionToken, tokenType, isMultiWorkspace, nil
}

// IdentityToken returns the dataplane identity token to send on cp-router handshakes, or nil when none is sent:
// only NAMESPACE handshakes carry one, and only when DATAPLANE_IDENTITY_TOKEN_FILE is set.
// It fails when the file can't be loaded, as backend config does for the same file.
func IdentityToken(tokenType string) (*tokenfile.Token, error) {
	path := config.GetStringVar("", "DATAPLANE_IDENTITY_TOKEN_FILE")
	if tokenType != namespace || path == "" {
		return nil, nil // nolint:nilnil // no token to send is not an error
	}
	token := tokenfile.New(path)
	if err := token.Load(); err != nil {
		return nil, fmt.Errorf("loading the identity token: %w", err)
	}
	return token, nil
}
