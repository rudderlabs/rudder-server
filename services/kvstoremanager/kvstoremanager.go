package kvstoremanager

import (
	"encoding/json"

	"github.com/tidwall/gjson"
)

type KVStoreManager interface {
	Connect()
	Close() error
	HMSet(key string, fields map[string]interface{}) error
	HSet(key string, field string, value interface{}) error
	StatusCode(err error) int
	DeleteKey(key string) (err error)
	HMGet(key string, fields ...string) (result []interface{}, err error)
	HGetAll(key string) (result map[string]string, err error)
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

func SupportsKeyUpdate(jsonData json.RawMessage) bool {
	return gjson.GetBytes(jsonData, "message.key").Exists() && gjson.GetBytes(jsonData, "message.value").Exists() && gjson.GetBytes(jsonData, "message.hash").Exists()
}

func EventToHashKeyValue(jsonData json.RawMessage) (string, string, string) {
	hash := gjson.GetBytes(jsonData, "message.hash").String()
	value := gjson.GetBytes(jsonData, "message.value").String()
	key := gjson.GetBytes(jsonData, "message.key").String()

	return hash, key, value
}
