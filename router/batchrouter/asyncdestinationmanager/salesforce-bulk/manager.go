package salesforcebulk

import (
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
)

func NewManager(
	logger logger.Logger,
	statsFactory stats.Stats,
	destination *backendconfig.DestinationT,
	backendConfig backendconfig.BackendConfig,
) (common.AsyncDestinationManager, error) {
	config, err := parseDestinationConfig(destination)
	if err != nil {
		return nil, fmt.Errorf("parsing destination config: %w", err)
	}

	if config.APIVersion == "" {
		config.APIVersion = "v62.0"
	}

	if config.Operation == "" {
		config.Operation = "insert"
	}

	oauthClient := oauthv2.NewOAuthHandler(backendConfig)

	authService := &SalesforceAuthService{
		logger:      logger,
		oauthClient: oauthClient,
		workspaceID: destination.WorkspaceID,
		accountID:   config.RudderAccountID,
		destID:      destination.ID,
		apiVersion:  config.APIVersion,
	}

	apiService := NewSalesforceAPIService(authService, logger, config.APIVersion)

	return &SalesforceBulkUploader{
		destName:        destName,
		config:          config,
		logger:          logger,
		statsFactory:    statsFactory,
		apiService:      apiService,
		authService:     authService,
		dataHashToJobID: make(map[string]int64),
	}, nil
}

func parseDestinationConfig(destination *backendconfig.DestinationT) (DestinationConfig, error) {
	var config DestinationConfig
	configMap := destination.Config

	rudderAccountID, _ := configMap["rudderAccountId"].(string)
	if rudderAccountID == "" {
		return config, fmt.Errorf("rudderAccountId is required")
	}

	operation, _ := configMap["operation"].(string)
	if operation != "" {
		validOps := map[string]bool{
			"insert": true,
			"update": true,
			"upsert": true,
			"delete": true,
		}
		if !validOps[operation] {
			return config, fmt.Errorf("invalid operation: %s (must be insert, update, upsert, or delete)", operation)
		}
	}

	apiVersion, _ := configMap["apiVersion"].(string)
	objectType, _ := configMap["objectType"].(string)

	config = DestinationConfig{
		RudderAccountID: rudderAccountID,
		Operation:       operation,
		ObjectType:      objectType,
		APIVersion:      apiVersion,
	}

	return config, nil
}

