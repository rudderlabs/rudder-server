package kvstoremanager

import (
	"encoding/json"

	"github.com/tidwall/gjson"
)

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

func New(provider string, config map[string]interface{}) (m KVStoreManager) {
	return newManager(SettingsT{
		Provider: provider,
		Config:   config,
	})
}

func newManager(settings SettingsT) (m KVStoreManager) {
	switch settings.Provider {
	case "REDIS":
		m = &redisManagerT{
			config: settings.Config,
		}
		m.Connect()
	}
	return m
}

func EventToKeyValue(jsonData json.RawMessage) (string, map[string]interface{}) {
	key := gjson.GetBytes(jsonData, "message.key").String()
	result := gjson.GetBytes(jsonData, "message.fields").Map()
	fields := make(map[string]interface{})
	for k, v := range result {
		fields[k] = v.Str
	}

	return key, fields
}
