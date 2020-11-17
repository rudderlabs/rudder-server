package kvstoremanager

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-redis/redis"
	"github.com/rudderlabs/rudder-server/utils/types"
)

var abortableErrors = []string{}

type redisManagerT struct {
	clusterMode   bool
	config        types.ConfigT
	client        *redis.Client
	clusterClient *redis.ClusterClient
}

func init() {
	abortableErrors = []string{"connection refused", "invalid password"}
}

func (m *redisManagerT) Connect() {
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

func (m *redisManagerT) Close() error {
	if m.clusterMode {
		return m.clusterClient.Close()
	}
	return m.client.Close()
}

func (m *redisManagerT) HMSet(key string, fields map[string]interface{}) (err error) {
	if m.clusterMode {
		_, err = m.clusterClient.HMSet(key, fields).Result()
	} else {
		_, err = m.client.HMSet(key, fields).Result()
	}
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
