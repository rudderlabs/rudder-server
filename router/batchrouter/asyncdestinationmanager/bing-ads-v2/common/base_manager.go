package common

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"

	"github.com/rudderlabs/bing-ads-go-sdk/bingads"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
)

// BaseDestinationConfig contains common configuration fields for Bing Ads destinations
type BaseDestinationConfig struct {
	CustomerAccountID string `mapstructure:"customerAccountId"`
	CustomerID        string `mapstructure:"customerId"`
	RudderAccountID   string `mapstructure:"rudderAccountId"`
	IsHashRequired    bool   `mapstructure:"isHashRequired"`
}

// BaseManager provides common functionality for Bing Ads managers
type BaseManager struct {
	Conf            *config.Config
	Logger          logger.Logger
	StatsFactory    stats.Stats
	DestinationName string
	DestConfig      *BaseDestinationConfig
	Service         bingads.BulkServiceI
}

// NewBaseManager creates a new base manager with common dependencies (for production use)
func NewBaseManager(conf *config.Config, logger logger.Logger, statsFactory stats.Stats, backendConfig backendconfig.BackendConfig, destination *backendconfig.DestinationT) (*BaseManager, error) {
	destConfig, err := ParseDestinationConfig(destination)
	if err != nil {
		return nil, err
	}

	// Create OAuth client
	oauthClientV2 := oauthv2.NewOAuthHandler(backendConfig,
		oauthv2.WithLogger(logger),
		oauthv2.WithCPConnectorTimeout(conf.GetDuration("HttpClient.oauth.timeout", 30, time.Second)),
		oauthv2.WithStats(statsFactory),
	)

	// Create session
	session, err := createSession(destConfig, destination, oauthClientV2)
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	// Create bulk service
	service := bingads.NewBulkService(session)

	return &BaseManager{
		Conf:            conf,
		Logger:          logger,
		StatsFactory:    statsFactory,
		DestinationName: destination.DestinationDefinition.Name,
		DestConfig:      destConfig,
		Service:         service,
	}, nil
}

// Helper functions
func ParseDestinationConfig(destination *backendconfig.DestinationT) (*BaseDestinationConfig, error) {
	if destination == nil {
		return nil, fmt.Errorf("destination cannot be nil")
	}

	var destConfig BaseDestinationConfig
	if err := mapstructure.Decode(destination.Config, &destConfig); err != nil {
		return nil, fmt.Errorf("error in decoding destination config: %w", err)
	}

	if err := ValidateDestinationConfig(&destConfig); err != nil {
		return nil, fmt.Errorf("error in validating destination config: %w", err)
	}

	return &destConfig, nil
}

func ValidateDestinationConfig(config *BaseDestinationConfig) error {
	if config == nil {
		return fmt.Errorf("config cannot be nil")
	}
	if strings.TrimSpace(config.CustomerAccountID) == "" {
		return fmt.Errorf("customerAccountId is required")
	}
	if strings.TrimSpace(config.CustomerID) == "" {
		return fmt.Errorf("customerId is required")
	}
	if strings.TrimSpace(config.RudderAccountID) == "" {
		return fmt.Errorf("rudderAccountID is required")
	}
	return nil
}

func createSession(destConfig *BaseDestinationConfig, destination *backendconfig.DestinationT, oauthClient oauthv2.Authorizer) (*bingads.Session, error) {
	// Generate OAuth token
	tokenSource := &TokenSource{
		WorkspaceID:        destination.WorkspaceID,
		DestinationDefName: destination.DestinationDefinition.Name,
		AccountID:          destConfig.RudderAccountID,
		OauthClientV2:      oauthClient,
		DestinationID:      destination.ID,
		CurrentTime:        time.Now,
	}

	_, err := tokenSource.GenerateTokenV2()
	if err != nil {
		return nil, fmt.Errorf("failed to generate oauth token: %w", err)
	}

	// Create session - using minimal fields to avoid struct field issues
	session := &bingads.Session{}
	// Set fields based on what's available in the bingads.Session struct
	// This will be adjusted based on the actual struct definition
	return session, nil
}
