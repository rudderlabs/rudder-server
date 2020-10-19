package kvstoremanager

import (
	"github.com/go-redis/redis"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type redisManagerT struct {
	config types.ConfigT
	client *redis.Client
}

func (m *redisManagerT) Connect() {
	addr, _ := m.config["address"].(string)
	password, _ := m.config["password"].(string)
	db, _ := m.config["database"].(int)

	redisClient := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	m.client = redisClient
}

func (m *redisManagerT) Close() error {
	return m.client.Close()
}

func (m *redisManagerT) HMSet(key string, fields map[string]interface{}) (string, error) {
	return m.client.HMSet(key, fields).Result()
}
