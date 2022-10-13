package kvstoremanager

import (
	"encoding/json"
	"fmt"

	"github.com/tidwall/gjson"
)

var ProviderNotSupported = fmt.Errorf("Provider not supported")

type KVStoreManager interface {
	Connect() error
	Close() error
	HMSet(key string, fields map[string]interface{}) error
	StatusCode(err error) int
	DeleteKey(key string) (err error)
	HMGet(key string, fields ...string) (result []interface{}, err error)
	HGetAll(key string) (result map[string]string, err error)
}

func New(provider string, config map[string]interface{}) (KVStoreManager, error) {
	switch provider {
	case "REDIS":
		m := &redisManagerT{
			config: config,
		}
		if err := m.Connect(); err != nil {
			return nil, err
		}
		return m, nil
	default:
		return nil, ProviderNotSupported
	}
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
