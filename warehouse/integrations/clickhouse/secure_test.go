package clickhouse_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

// secureServerConfig turns on the native TLS port and points the server at the
// generated certificate. It is written next to that certificate and mounted into
// config.d, so the whole server side of the test is generated per run.
//
// The root element is the image's: a config.d file has to match the root of the
// main config, and the 21.x server v1 runs against predates clickhouse being
// accepted as an alias for yandex.
const secureServerConfig = `<%[1]s>
    <tcp_port_secure>9440</tcp_port_secure>
    <openSSL>
        <server>
            <certificateFile>/etc/clickhouse-server/tls/server.crt</certificateFile>
            <privateKeyFile>/etc/clickhouse-server/tls/server.key</privateKeyFile>
            <verificationMode>none</verificationMode>
            <cacheSessions>true</cacheSessions>
            <disableProtocols>sslv2,sslv3</disableProtocols>
            <preferServerCiphers>true</preferServerCiphers>
        </server>
    </openSSL>
</%[1]s>
`

// TestSecureConnection covers the TLS path against the v1 implementation, and
// TestSecureConnectionV2 covers it against v2.
//
// It stands apart from TestIntegration because the server needs a certificate
// that only exists for the duration of the run. dockertest can mount a path
// computed at runtime, which the shared compose files cannot, so this is the one
// place in the package that starts its container directly.
func TestSecureConnection(t *testing.T) {
	testSecureConnection(t, false)
}

func TestSecureConnectionV2(t *testing.T) {
	testSecureConnection(t, true)
}

func testSecureConnection(t *testing.T, useV2 bool) {
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

	certificate, dir := generateCertificate(t)
	// A certificate the server never presents, so verifying against it has to fail.
	untrustedCertificate, _ := generateCertificate(t)

	// v1 stays on the 21.x server it has always been tested against; v2 needs a
	// server its driver supports (MinSupportedVersion 25.8.0).
	image, tag, rootElement := "yandex/clickhouse-server", "21-alpine", "yandex"
	if useV2 {
		image, tag, rootElement = "clickhouse/clickhouse-server", "25.8-alpine", "clickhouse"
	}
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "secure.xml"),
		fmt.Appendf(nil, secureServerConfig, rootElement),
		0o644,
	))

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	container, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: image,
		Tag:        tag,
		Env: []string{
			"CLICKHOUSE_DB=" + database,
			"CLICKHOUSE_USER=" + user,
			"CLICKHOUSE_PASSWORD=" + password,
		},
		ExposedPorts: []string{"8123/tcp", "9440/tcp"},
		PortBindings: map[dc.Port][]dc.PortBinding{
			"8123/tcp": {{HostIP: "127.0.0.1", HostPort: "0"}},
			"9440/tcp": {{HostIP: "127.0.0.1", HostPort: "0"}},
		},
		Mounts: []string{
			filepath.Join(dir, "server.crt") + ":/etc/clickhouse-server/tls/server.crt:ro",
			filepath.Join(dir, "server.key") + ":/etc/clickhouse-server/tls/server.key:ro",
			filepath.Join(dir, "secure.xml") + ":/etc/clickhouse-server/config.d/secure.xml:ro",
		},
	}, func(hc *dc.HostConfig) { hc.PublishAllPorts = false })
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := pool.Purge(container); err != nil {
			t.Logf("Could not purge clickhouse container: %v", err)
		}
	})

	// The server refuses to start if it cannot read the mounted certificate, so
	// answering on the HTTP port means the secure port is listening as well.
	pingURL := fmt.Sprintf("http://%s:%s/ping", host, container.GetPort("8123/tcp"))
	require.NoError(t, pool.Retry(func() error {
		resp, err := http.Get(pingURL)
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("clickhouse not ready: %s", resp.Status)
		}
		return nil
	}))

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
						"port":           container.GetPort("9440/tcp"),
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
			if useV2 {
				ch = clickhouse.NewV2(config.New(), logger.NOP, stats.NOP)
			}

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

// generateCertificate generates a self-signed certificate and key into a new
// temporary directory and returns the certificate in PEM along with the
// directory to mount.
//
// Generating per run keeps key material out of the repository, the same way the
// tunnelling tests generate their SSH key pair. The certificate signs itself, so
// it doubles as the CA the client trusts, and it carries SAN DNS:localhost so the
// verified case can leave skipVerify off and have verification mean something.
func generateCertificate(t *testing.T) (string, string) {
	t.Helper()

	// The server reads these as the clickhouse user rather than as whoever runs
	// the test, so the directory has to be traversable and the files readable.
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o755))

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost", Organization: []string{"RudderStack ClickHouse Test"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})

	require.NoError(t, os.WriteFile(filepath.Join(dir, "server.crt"), certPEM, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "server.key"), keyPEM, 0o644))

	return string(certPEM), dir
}
