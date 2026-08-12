package clickhouse

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	clickhousev2 "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func newTestClickhouse(t *testing.T, conf *config.Config, wsID, destID string, destConfig map[string]any) *Clickhouse {
	t.Helper()
	ch := New(conf, logger.NOP, stats.NOP)
	ch.Namespace = "test_ns"
	ch.Warehouse = model.Warehouse{
		WorkspaceID: wsID,
		Destination: backendconfig.DestinationT{ID: destID, Config: destConfig},
	}
	return ch
}

func TestClickhouseColumnTypeAndDDL(t *testing.T) {
	const wsID, destID = "ws1", "dest1"

	t.Run("json maps to String when native JSON disabled", func(t *testing.T) {
		ch := newTestClickhouse(t, config.New(), wsID, destID, nil)
		ddl := ch.ColumnsWithDataTypes("some_table", model.TableSchema{"payload": "json"}, nil)
		require.Contains(t, ddl, `"payload" Nullable(String)`)
		require.NotContains(t, ddl, "JSON")
	})

	t.Run("native JSON gated by the nativeJSONColumns flag", func(t *testing.T) {
		testCases := []struct {
			name       string
			nativeJSON bool
			wantJSON   bool
		}{
			{"native JSON disabled", false, false},
			{"native JSON enabled", true, true},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				conf := config.New()
				conf.Set("Warehouse.clickhouse.nativeJSONColumns", tc.nativeJSON)
				ch := newTestClickhouse(t, conf, wsID, destID, nil)

				ddl := ch.ColumnsWithDataTypes("some_table", model.TableSchema{"payload": "json"}, nil)
				if tc.wantJSON {
					// Native JSON must not be wrapped in Nullable(...).
					require.Contains(t, ddl, `"payload" JSON`)
					require.NotContains(t, ddl, "Nullable(JSON)")
				} else {
					require.Contains(t, ddl, `"payload" Nullable(String)`)
				}
			})
		}
	})

	t.Run("native JSON on the users table is not wrapped in SimpleAggregateFunction", func(t *testing.T) {
		conf := config.New()
		conf.Set("Warehouse.clickhouse.nativeJSONColumns", true)
		ch := newTestClickhouse(t, conf, wsID, destID, nil)

		ddl := ch.ColumnsWithDataTypes(warehouseutils.UsersTable, model.TableSchema{"payload": "json"}, nil)
		require.Contains(t, ddl, `"payload" JSON`)
		require.NotContains(t, ddl, "SimpleAggregateFunction(anyLast, JSON)")
	})
}

func TestTLSConfigV2(t *testing.T) {
	t.Run("nil when not secure and no certificate", func(t *testing.T) {
		ch := newTestClickhouse(t, config.New(), "ws", "dest", map[string]any{})
		tlsConfig, err := ch.tlsConfigV2()
		require.NoError(t, err)
		require.Nil(t, tlsConfig)
	})

	t.Run("secure without certificate uses system roots and honors skipVerify", func(t *testing.T) {
		ch := newTestClickhouse(t, config.New(), "ws", "dest", map[string]any{
			"secure":     true,
			"skipVerify": true,
		})
		tlsConfig, err := ch.tlsConfigV2()
		require.NoError(t, err)
		require.NotNil(t, tlsConfig)
		require.True(t, tlsConfig.InsecureSkipVerify)
		require.Nil(t, tlsConfig.RootCAs)
	})

	t.Run("certificate is added to the root pool", func(t *testing.T) {
		ch := newTestClickhouse(t, config.New(), "ws", "dest", map[string]any{
			"secure":        true,
			"caCertificate": selfSignedCertPEM(t),
		})
		tlsConfig, err := ch.tlsConfigV2()
		require.NoError(t, err)
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.RootCAs)
	})

	t.Run("invalid certificate errors", func(t *testing.T) {
		ch := newTestClickhouse(t, config.New(), "ws", "dest", map[string]any{
			"secure":        true,
			"caCertificate": "not-a-valid-pem",
		})
		_, err := ch.tlsConfigV2()
		require.Error(t, err)
	})
}

func TestClickhouseV2Options(t *testing.T) {
	conf := config.New()
	conf.Set("Warehouse.clickhouse.compress", true)
	conf.Set("Warehouse.clickhouse.poolSize", "42")
	conf.Set("Warehouse.clickhouse.readTimeout", "300")
	conf.Set("Warehouse.clickhouse.blockSize", "1000")

	ch := newTestClickhouse(t, conf, "ws", "dest", map[string]any{
		"host":     "clickhouse.example.com",
		"port":     "9440",
		"database": "analytics",
		"user":     "rudder",
		"password": "secret",
	})

	cred, err := ch.connectionCredentials()
	require.NoError(t, err)

	opts, err := ch.clickhouseV2Options(cred, true)
	require.NoError(t, err)

	require.Equal(t, []string{"clickhouse.example.com:9440"}, opts.Addr)
	require.Equal(t, "rudder", opts.Auth.Username)
	require.Equal(t, "secret", opts.Auth.Password)
	require.Equal(t, "analytics", opts.Auth.Database)
	require.Equal(t, 42, opts.MaxOpenConns)
	require.Equal(t, 42, opts.MaxIdleConns)
	require.Equal(t, 300*time.Second, opts.ReadTimeout)
	require.Equal(t, 1000, opts.Settings["max_block_size"])
	require.NotNil(t, opts.Compression)
	require.Equal(t, clickhousev2.CompressionLZ4, opts.Compression.Method)

	t.Run("database omitted when includeDBInConn is false", func(t *testing.T) {
		opts, err := ch.clickhouseV2Options(cred, false)
		require.NoError(t, err)
		require.Empty(t, opts.Auth.Database)
	})
}

func TestIsUnknownDatabaseErr(t *testing.T) {
	require.False(t, isUnknownDatabaseErr(nil))
	require.False(t, isUnknownDatabaseErr(errors.New("some other error")))
	require.True(t, isUnknownDatabaseErr(&clickhousev2.Exception{Code: clickhouseUnknownDatabaseCode}))
	require.False(t, isUnknownDatabaseErr(&clickhousev2.Exception{Code: 60}))
}

// selfSignedCertPEM returns a PEM-encoded self-signed CA certificate for tests.
func selfSignedCertPEM(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Unix(0, 0),
		NotAfter:              time.Unix(1<<31-1, 0),
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	require.NoError(t, err)
	return strings.TrimSpace(string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})))
}
