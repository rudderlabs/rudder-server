package clickhouse_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	chresource "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/clickhouse"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

// TestIntegrationSecureConnection covers the TLS path. Its name starts with
// TestIntegration so the CI warehouse job (-run TestIntegration) runs it. It needs
// a server certificate that exists only for this run, so it starts its own go-kit
// ClickHouse resource with WithTLS.
func TestIntegrationSecureConnection(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}

	misc.Init()
	validations.Init()
	whutils.Init()

	const (
		host      = "localhost"
		database  = "rudderdb"
		user      = "rudder"
		password  = "rudder-password"
		namespace = "test_namespace"
		timeout   = 10 * time.Second
	)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	// The go-kit resource generates a throwaway CA and serves secure native on
	// 9440 with a certificate for localhost. It returns after readiness.
	resource, err := chresource.Setup(pool, t,
		chresource.WithTLS(),
		chresource.WithUser(user),
		chresource.WithPassword(password),
		chresource.WithDatabase(database),
		chresource.WithPrintLogsOnError(true),
	)
	require.NoError(t, err)
	certificate := string(resource.CAPEM)
	// A certificate the server never presents, so verifying against it has to fail.
	untrustedCertificate := generateCertificate(t)

	testCases := []struct {
		name        string
		certificate string
		skipVerify  bool
		wantError   string
	}{
		{
			name:        "verified",
			certificate: certificate,
		},
		{
			name:       "skip verify",
			skipVerify: true,
		},
		{
			name:        "untrusted certificate",
			certificate: untrustedCertificate,
			wantError:   "x509: certificate signed by unknown authority",
		},
	}

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			warehouse := model.Warehouse{
				Namespace:   namespace,
				WorkspaceID: whutils.RandHex(),
				Destination: backendconfig.DestinationT{
					// v1 registers the TLS config globally under the destination
					// ID, so the cases cannot share one.
					ID: fmt.Sprintf("test-destination-%d", i),
					Config: map[string]any{
						"bucketProvider": whutils.MINIO,
						"host":           host,
						"port":           resource.NativePort,
						"database":       database,
						"user":           user,
						"password":       password,
						"secure":         true,
						"skipVerify":     tc.skipVerify,
						"caCertificate":  tc.certificate,
					},
				},
			}

			var ch manager.WarehouseOperations = clickhouse.New(config.New(), logger.NOP, stats.NOP)

			require.NoError(t, ch.Setup(context.Background(), warehouse, newMockUploader(t, "", nil, nil)))
			ch.SetConnectionTimeout(timeout)

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			err := ch.TestConnection(ctx, warehouse)
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
		})
	}
}

// generateCertificate returns a self-signed certificate in PEM. The server
// never presents it, so a client that trusts only it must fail verification.
func generateCertificate(t *testing.T) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}
