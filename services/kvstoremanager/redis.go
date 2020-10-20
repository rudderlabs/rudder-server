package kvstoremanager

import (
	"net/http"
	"strings"

	"github.com/go-redis/redis"
	"github.com/rudderlabs/rudder-server/utils/types"
)

var abortableErrors = []string{}

type redisManagerT struct {
	config types.ConfigT
	client *redis.Client
}

func init() {
	abortableErrors = []string{"connection refused", "invalid password"}
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

func (m *redisManagerT) HMSet(key string, fields map[string]interface{}) error {
	_, err := m.client.HMSet(key, fields).Result()
	return err
}

func (m *redisManagerT) StatusCode(err error) int {
	if err == nil {
		return http.StatusOK
	}
	statusCode := http.StatusInternalServerError
	errorString := err.Error()
	for _, s := range abortableErrors {
		if strings.Contains(errorString, s) {
			statusCode = 400
			break
		}
	}
	return statusCode
}
