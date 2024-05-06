package kvstoremanager

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/samber/lo"

	"github.com/redis/go-redis/v9"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/utils/types"
)

var abortableErrors = []string{}

type RedisManagerT struct {
	clusterMode   bool
	Config        types.ConfigT
	client        *redis.Client
	clusterClient *redis.ClusterClient
}

func init() {
	abortableErrors = []string{"connection refused", "invalid password"}
}

func (m *RedisManagerT) UpdateClient(client *redis.Client) {
	m.client = client
}

func (m *RedisManagerT) GetClient() *redis.Client {
	return m.client
}

func (m *RedisManagerT) Connect() {
	var ok bool
	if m.clusterMode, ok = m.Config["clusterMode"].(bool); !ok {
		// setting redis to cluster mode by default if setting missing in config
		m.clusterMode = true
	}
	shouldSecureConn, _ := m.Config["secure"].(bool)
	addr, _ := m.Config["address"].(string)
	password, _ := m.Config["password"].(string)

	tlsConfig := tls.Config{}
	if shouldSecureConn {
		if skipServerCertCheck, ok := m.Config["skipVerify"].(bool); ok && skipServerCertCheck {
			tlsConfig.InsecureSkipVerify = true
		}
		if serverCACert, ok := m.Config["caCertificate"].(string); ok && len(strings.TrimSpace(serverCACert)) > 0 {
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
		if dbStr, ok := m.Config["database"].(string); ok {
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

func (m *RedisManagerT) Close() error {
	if m.clusterMode {
		return m.clusterClient.Close()
	}
	return m.client.Close()
}

func (m *RedisManagerT) HMSet(key string, fields map[string]interface{}) (err error) {
	ctx := context.Background()
	if m.clusterMode {
		_, err = m.clusterClient.HMSet(ctx, key, fields).Result()
	} else {
		_, err = m.client.HMSet(ctx, key, fields).Result()
	}
	return err
}

func (*RedisManagerT) StatusCode(err error) int {
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

func (m *RedisManagerT) DeleteKey(key string) (err error) {
	ctx := context.Background()
	if m.clusterMode {
		_, err = m.clusterClient.Del(ctx, key).Result()
	} else {
		_, err = m.client.Del(ctx, key).Result()
	}
	return err
}

func (m *RedisManagerT) HMGet(key string, fields ...string) (result []interface{}, err error) {
	ctx := context.Background()
	if m.clusterMode {
		result, err = m.clusterClient.HMGet(ctx, key, fields...).Result()
	} else {
		result, err = m.client.HMGet(ctx, key, fields...).Result()
	}
	return result, err
}

func (m *RedisManagerT) HGetAll(key string) (result map[string]string, err error) {
	ctx := context.Background()
	if m.clusterMode {
		result, err = m.clusterClient.HGetAll(ctx, key).Result()
	} else {
		result, err = m.client.HGetAll(ctx, key).Result()
	}
	return result, err
}

func (m *RedisManagerT) HSet(hash, key string, value interface{}) (err error) {
	ctx := context.Background()
	if m.clusterMode {
		_, err = m.clusterClient.HSet(ctx, hash, key, value).Result()
	} else {
		_, err = m.client.HSet(ctx, hash, key, value).Result()
	}
	return err
}

func (m *RedisManagerT) ExtractJSONSetArgs(jsonData json.RawMessage) []interface{} {
	msg := gjson.GetBytes(jsonData, "message").Map()
	nmSetArgs := lo.Flatten(
		lo.MapToSlice(msg, func(k string, v gjson.Result) []interface{} {
			return []interface{}{k, "$", v.String()}
		}),
	)
	return nmSetArgs
}

func (m *RedisManagerT) SendDataAsJSON(jsonData json.RawMessage) (interface{}, error) {
	nmSetArgs := m.ExtractJSONSetArgs(jsonData)
	var jsonMSetStatusCmd *redis.StatusCmd
	if m.clusterMode {
		jsonMSetStatusCmd = m.clusterClient.JSONMSet(context.Background(), nmSetArgs...)
	} else {
		jsonMSetStatusCmd = m.client.JSONMSet(context.Background(), nmSetArgs...)
	}
	return jsonMSetStatusCmd.Result()
}

func (_ *RedisManagerT) ShouldSendDataAsJSON(config map[string]interface{}) bool {
	var dataAsJSON bool
	if dataAsJSONI, ok := config["shouldSendDataAsJSON"]; ok {
		if dataAsJSON, ok = dataAsJSONI.(bool); ok {
			return dataAsJSON
		}
	}
	return dataAsJSON
}
