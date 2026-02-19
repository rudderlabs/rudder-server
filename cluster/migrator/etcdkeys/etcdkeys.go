package etcdkeys

import (
	"github.com/rudderlabs/rudder-go-kit/config"
)

// MigrationRequestKeyPrefix returns the etcd key prefix for migration requests
func MigrationRequestKeyPrefix(config *config.Config) string {
	return "/" + getEtcdNamespace(config) + "/migration/request/"
}

// MigrationJobKeyPrefix returns the etcd key prefix for migration jobs
func MigrationJobKeyPrefix(config *config.Config) string {
	return "/" + getEtcdNamespace(config) + "/migration/job/"
}

// ReloadGatewayRequestKeyPrefix returns the etcd key prefix for gateway reload requests
func ReloadGatewayRequestKeyPrefix(config *config.Config) string {
	return "/" + getEtcdNamespace(config) + "/reload/gateway/request/"
}

// getEtcdNamespace retrieves the key namespace from the configuration
func getEtcdNamespace(config *config.Config) string {
	return config.GetStringVar("", "RELEASE_NAME")
}
