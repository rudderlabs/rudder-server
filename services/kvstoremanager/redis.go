package kvstoremanager

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/utils/types"
)

var abortableErrors = []string{}

type RedisManager struct {
	clusterMode   bool
	config        types.ConfigT
	client        *redis.Client
	clusterClient *redis.ClusterClient
}

func init() {
	abortableErrors = []string{"connection refused", "invalid password"}
}

func NewRedisManager(config types.ConfigT) *RedisManager {
	redisMgr := &RedisManager{
		config: config,
	}
	redisMgr.CreateClient()
	return redisMgr
}

func (m *RedisManager) GetClient() redis.Cmdable {
	if m.clusterMode {
		return m.clusterClient
	}
	return m.client
}

func (m *RedisManager) CreateClient() {
	var ok bool
	if m.clusterMode, ok = m.config["clusterMode"].(bool); !ok {
		// setting redis to cluster mode by default if setting missing in config
		m.clusterMode = true
	}
	shouldSecureConn, _ := m.config["secure"].(bool)
	addr, _ := m.config["address"].(string)
	password, _ := m.config["password"].(string)

	tlsConfig := tls.Config{}
	if shouldSecureConn {
		if skipServerCertCheck, ok := m.config["skipVerify"].(bool); ok && skipServerCertCheck {
			tlsConfig.InsecureSkipVerify = true
		}
		if serverCACert, ok := m.config["caCertificate"].(string); ok && len(strings.TrimSpace(serverCACert)) > 0 {
			caCert := []byte(serverCACert)
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tlsConfig.RootCAs = caCertPool
		}
	}

	if m.clusterMode {
		addrs := strings.Split(addr, ",")
		for i := range addrs {
			addrs[i] = strings.TrimSpace(addrs[i])
		}
		opts := redis.ClusterOptions{
			Addrs:    addrs,
			Password: password,
		}
		if shouldSecureConn {
			opts.TLSConfig = &tlsConfig
		}
		m.clusterClient = redis.NewClusterClient(&opts)
	} else {
		var db int
		if dbStr, ok := m.config["database"].(string); ok {
			db, _ = strconv.Atoi(dbStr)
		}
		opts := redis.Options{
			Addr:     strings.TrimSpace(addr),
			Password: password,
			DB:       db,
		}
		if shouldSecureConn {
			opts.TLSConfig = &tlsConfig
		}
		m.client = redis.NewClient(&opts)
	}
}

func (m *RedisManager) Close() error {
	if m.clusterMode {
		return m.clusterClient.Close()
	}
	return m.client.Close()
}

func (m *RedisManager) HMSet(key string, fields map[string]interface{}) (err error) {
	ctx := context.Background()
	if m.clusterMode {
		_, err = m.clusterClient.HMSet(ctx, key, fields).Result()
	} else {
		_, err = m.client.HMSet(ctx, key, fields).Result()
	}
	return err
}

func (*RedisManager) StatusCode(err error) int {
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

func (m *RedisManager) DeleteKey(key string) (err error) {
	ctx := context.Background()
	if m.clusterMode {
		_, err = m.clusterClient.Del(ctx, key).Result()
	} else {
		_, err = m.client.Del(ctx, key).Result()
	}
	return err
}

func (m *RedisManager) HMGet(key string, fields ...string) (result []interface{}, err error) {
	ctx := context.Background()
	if m.clusterMode {
		result, err = m.clusterClient.HMGet(ctx, key, fields...).Result()
	} else {
		result, err = m.client.HMGet(ctx, key, fields...).Result()
	}
	return result, err
}

func (m *RedisManager) HGetAll(key string) (result map[string]string, err error) {
	ctx := context.Background()
	if m.clusterMode {
		result, err = m.clusterClient.HGetAll(ctx, key).Result()
	} else {
		result, err = m.client.HGetAll(ctx, key).Result()
	}
	return result, err
}

func (m *RedisManager) HSet(hash, key string, value interface{}) (err error) {
	ctx := context.Background()
	if m.clusterMode {
		_, err = m.clusterClient.HSet(ctx, hash, key, value).Result()
	} else {
		_, err = m.client.HSet(ctx, hash, key, value).Result()
	}
	return err
}

func (m *RedisManager) ExtractJSONSetArgs(jsonData json.RawMessage) ([]string, error) {
	key := gjson.GetBytes(jsonData, "message.key").String()
	path := gjson.GetBytes(jsonData, "message.path").String()
	jsonVal := gjson.GetBytes(jsonData, "message.value")

	actualPath := "$" // root insert
	if path != "" {
		actualPath = fmt.Sprintf("$.%s", path)
	}
	args := []string{key, actualPath, jsonVal.String()}

	if actualPath != "$" {
		v, err := m.GetClient().JSONGet(context.Background(), key).Result()
		if err != nil {
			return nil, err
		}
		if v == "" {
			// key is new one but we need to insert a value other than root
			// formulate {[path]: value}
			mapStr, err := json.Marshal(map[string]interface{}{
				path: jsonVal.Value(),
			})
			if err != nil {
				return nil, err
			}
			// data is not present in key
			args = []string{key, "$", string(mapStr)} // arguments to insert data
		}
	}
	return args, nil
}

func (m *RedisManager) SendDataAsJSON(jsonData json.RawMessage) (interface{}, error) {
	nmSetArgs, err := m.ExtractJSONSetArgs(jsonData)
	if err != nil {
		return nil, err
	}
	redisClient := m.GetClient()
	ctx := context.Background()
	val, err := redisClient.JSONSet(ctx, nmSetArgs[0], nmSetArgs[1], nmSetArgs[2]).Result()
	if err != nil {
		return nil, fmt.Errorf("setting key:(%s %s %s): %w", nmSetArgs[0], nmSetArgs[1], nmSetArgs[2], err)
	}

	return val, err
}

func (*RedisManager) ShouldSendDataAsJSON(config map[string]interface{}) bool {
	var dataAsJSON bool
	if dataAsJSONI, ok := config["useJSONModule"]; ok {
		if dataAsJSON, ok = dataAsJSONI.(bool); ok {
			return dataAsJSON
		}
	}
	return dataAsJSON
}
