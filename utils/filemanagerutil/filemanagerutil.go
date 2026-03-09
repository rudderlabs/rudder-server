package filemanagerutil

import (
	"context"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

func GetProviderConfigForBackupsFromEnv(ctx context.Context, config *config.Config) map[string]any {
	return filemanager.GetProviderConfigFromEnv(ProviderConfigOpts(ctx,
		config.GetStringVar("S3", "JOBS_BACKUP_STORAGE_PROVIDER"),
		config,
	))
}

func ProviderConfigOpts(ctx context.Context, provider string, config *config.Config) filemanager.ProviderConfigOpts {
	return filemanager.ProviderConfigOpts{
		Provider: provider,
		Bucket:   config.GetStringVar("rudder-saas", "JOBS_BACKUP_BUCKET"),
		Prefix:   config.GetStringVar("", "JOBS_BACKUP_PREFIX"),
		Config:   config,
		ExternalIDSupplier: func() string {
			backendconfig.DefaultBackendConfig.WaitForConfig(ctx)
			return backendconfig.DefaultBackendConfig.Identity().ID()
		},
	}
}
