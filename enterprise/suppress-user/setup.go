package suppression

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

type Factory struct {
	EnterpriseToken string
}

var (
	regulationsPollInterval time.Duration
	configBackendURL        string
	suppressionApiPageSize  int
)

func loadConfig() {
	config.RegisterDurationConfigVariable(300, &regulationsPollInterval, true, time.Second, "BackendConfig.Regulations.pollInterval")
	config.RegisterIntConfigVariable(50, &suppressionApiPageSize, false, 1, "BackendConfig.Regulations.pageSize")
	configBackendURL = config.GetString("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
}

// Setup initializes Suppress User feature
func (m *Factory) Setup(backendConfig backendconfig.BackendConfig) (types.UserSuppression, error) {
	pkgLogger = logger.NewLogger().Child("enterprise").Child("suppress-user")

	if m.EnterpriseToken == "" {
		pkgLogger.Info("Suppress User feature is enterprise only")
		return &NOOP{}, nil
	}

	pkgLogger.Info("[[ SuppressUser ]] Setting up Suppress User Feature")
	loadConfig()
	ctx := context.TODO()
	backendConfig.WaitForConfig(ctx)
	ID := backendConfig.Identity().ID()
	deploymentType, err := deployment.GetFromEnv()
	if err != nil {
		return &NOOP{}, fmt.Errorf("deployment type from env: %w", err)
	}
	var dType string
	switch deploymentType {
	case deployment.DedicatedType:
		dType = `workspaces`
	case deployment.MultiTenantType:
		dType = `namespaces`
	default:
		return &NOOP{}, fmt.Errorf("deployment type %q not supported", deploymentType)
	}
	regulationURL := fmt.Sprintf(
		"%s/dataplane/%s/%s/regulations/suppressions",
		configBackendURL, dType, ID,
	)
	suppressUser := &SuppressRegulationHandler{
		RegulationBackendURL:    regulationURL,
		RegulationsPollInterval: regulationsPollInterval,
		ID:                      ID,
		pageSize:                strconv.Itoa(suppressionApiPageSize),
	}
	suppressUser.setup(ctx)

	return suppressUser, nil
}
