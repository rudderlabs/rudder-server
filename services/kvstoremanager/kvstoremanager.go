package kvstoremanager

import "github.com/rudderlabs/rudder-server/utils/types"

var (
	KVStoreDestinations []string
)

func init() {
	loadConfig()
}

func loadConfig() {
	KVStoreDestinations = []string{"REDIS"}
}

type KVStoreManager interface {
	Connect()
	Close() error
	HMSet(key string, fields map[string]interface{}) (string, error)
}

type SettingsT struct {
	Provider string
	Config   types.ConfigT
}

func New(settings SettingsT) (m KVStoreManager) {
	switch settings.Provider {
	case "REDIS":
		m = &redisManagerT{
			config: settings.Config,
		}
		m.Connect()
	}
	return m
}
