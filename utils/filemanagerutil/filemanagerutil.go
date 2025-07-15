package filemanagerutil

import (
	"context"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

func GetProviderConfigForBackupsFromEnv(ctx context.Context, config *config.Config) map[string]interface{} {
	providerConfig := filemanager.GetProviderConfigFromEnv(ProviderConfigOpts(ctx,
		config.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3"),
		config,
	))

	// GetProviderConfigFromEnv sets region to AWS_REGION by default,
	// but we remove it here to allow customers to use their own bucket
	// in a different region than the default AWS_REGION
	delete(providerConfig, "region")

	return providerConfig
}

func ProviderConfigOpts(ctx context.Context, provider string, config *config.Config) filemanager.ProviderConfigOpts {
	return filemanager.ProviderConfigOpts{
		Provider: provider,
		Bucket:   config.GetString("JOBS_BACKUP_BUCKET", "rudder-saas"),
		Prefix:   config.GetString("JOBS_BACKUP_PREFIX", ""),
		Config:   config,
		ExternalIDSupplier: func() string {
			backendconfig.DefaultBackendConfig.WaitForConfig(ctx)
			return backendconfig.DefaultBackendConfig.Identity().ID()
		},
	}
}
