package kvstoremanager

//go:generate mockgen -destination=../../mocks/services/kvstoremanager/mock_kvstoremanager.go -package=mock_kvstoremanager github.com/rudderlabs/rudder-server/services/kvstoremanager KVStoreManager

import (
	"encoding/json"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/services/kvstoremanager/redis"
)

type KVStoreManager interface {
	CreateClient()
	Close() error
	HMSet(key string, fields map[string]interface{}) error
	HSet(key, field string, value interface{}) error
	StatusCode(err error) int
	DeleteKey(key string) (err error)
	HMGet(key string, fields ...string) (result []interface{}, err error)
	HGetAll(key string) (result map[string]string, err error)

	SendDataAsJSON(jsonData json.RawMessage, config map[string]interface{}) (interface{}, error)
	ShouldSendDataAsJSON(config map[string]interface{}) bool
}

type SettingsT struct {
	Provider string
	Config   map[string]interface{}
}

const (
	hashPath  = "message.hash"
	keyPath   = "message.key"
	valuePath = "message.value"
)

func New(provider string, config map[string]interface{}) (m KVStoreManager) {
	return newManager(SettingsT{
		Provider: provider,
		Config:   config,
	})
}

func newManager(settings SettingsT) (m KVStoreManager) {
	switch settings.Provider {
	case "REDIS":
		m = redis.NewRedisManager(settings.Config)
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

// IsHSETCompatibleEvent identifies if the event supports HSET operation
// To support HSET, the event must have the following fields:
// - message.key
// - message.value
// - message.hash
// It doesn't account for the value of the fields.
func IsHSETCompatibleEvent(jsonData json.RawMessage) bool {
	return gjson.GetBytes(jsonData, hashPath).Exists() && gjson.GetBytes(jsonData, keyPath).Exists() && gjson.GetBytes(jsonData, valuePath).Exists()
}

func ExtractHashKeyValueFromEvent(jsonData json.RawMessage) (hash, key, value string) {
	hash = gjson.GetBytes(jsonData, hashPath).String()
	key = gjson.GetBytes(jsonData, keyPath).String()
	value = gjson.GetBytes(jsonData, valuePath).String()

	return hash, key, value
}
