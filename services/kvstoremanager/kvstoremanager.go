package kvstoremanager

type KVStoreManager interface {
	Connect()
	Close() error
	HMSet(key string, fields map[string]interface{}) error
	StatusCode(err error) int
}

type SettingsT struct {
	Provider string
	Config   map[string]interface{}
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
