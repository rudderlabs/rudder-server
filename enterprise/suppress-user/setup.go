package suppression

import (
	"context"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type suppressUserFeatureImpl struct{}

var (
	regulationsPollInterval time.Duration
	configBackendURL        string
	suppressionApiPageSize  int
)

// Setup initializes Suppress User feature
func (m *suppressUserFeatureImpl) Setup(backendConfig backendconfig.BackendConfig) types.SuppressUserI {
	pkgLogger = logger.NewLogger().Child("enterprise").Child("suppress-user")
	pkgLogger.Info("[[ SuppressUser ]] Setting up Suppress User Feature")
	loadConfig()
	ctx := context.Background()
	if err := backendConfig.WaitForConfig(ctx); err != nil {
		pkgLogger.Errorf("error initializing backend config: %s", err.Error())
		return nil
	}
	workspaceId := backendConfig.GetWorkspaceIDForWriteKey("")
	suppressUser := &SuppressRegulationHandler{
		RegulationBackendURL:    configBackendURL,
		RegulationsPollInterval: regulationsPollInterval,
		WorkspaceID:             workspaceId,
		pageSize:                strconv.Itoa(suppressionApiPageSize),
	}
	suppressUser.setup(ctx)

	return suppressUser
}

func loadConfig() {
	config.RegisterDurationConfigVariable(300, &regulationsPollInterval, true, time.Second, "BackendConfig.Regulations.pollInterval")
	config.RegisterIntConfigVariable(50, &suppressionApiPageSize, false, 1, "BackendConfig.Regulations.pageSize")
	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
}
