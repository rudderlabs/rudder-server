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
	config types.ConfigT
	client *redis.Client
}

func init() {
	abortableErrors = []string{"connection refused", "invalid password"}
}

func (m *redisManagerT) Connect() {
	addr, _ := m.config["address"].(string)
	password, _ := m.config["password"].(string)
	var db int
	if dbStr, ok := m.config["database"].(string); ok {
		db, _ = strconv.Atoi(dbStr)
	}

	opts := redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	}

	if shouldSecureConn, ok := m.config["secure"].(bool); ok && shouldSecureConn {
		tlsConfig := tls.Config{}
		opts.TLSConfig = &tlsConfig
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

	redisClient := redis.NewClient(&opts)
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
