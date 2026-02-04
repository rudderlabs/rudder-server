package etcdkeys

import (
	"github.com/rudderlabs/rudder-go-kit/config"
)

// MigrationRequestKeyPrefix returns the etcd key prefix for migration requests
func MigrationRequestKeyPrefix(config *config.Config) string {
	return "/" + getNamespace(config) + "/migration/request/"
}

// MigrationJobKeyPrefix returns the etcd key prefix for migration jobs
func MigrationJobKeyPrefix(config *config.Config) string {
	return "/" + getNamespace(config) + "/migration/job/"
}

// ReloadGatewayRequestKeyPrefix returns the etcd key prefix for gateway reload requests
func ReloadGatewayRequestKeyPrefix(config *config.Config) string {
	return "/" + getNamespace(config) + "/reload/gateway/request/"
}

// getNamespace retrieves the workspace namespace from the configuration
func getNamespace(config *config.Config) string {
	return config.GetString("WORKSPACE_NAMESPACE", "default")
}
