package testhelper

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/stretchr/testify/require"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	TestKeyPairUnencrypted = "SNOWPIPE_STREAMING_KEYPAIR_UNENCRYPTED_INTEGRATION_TEST_CREDENTIALS"
)

type TestCredentials struct {
	Account              string `json:"account"`
	Warehouse            string `json:"warehouse"`
	User                 string `json:"user"`
	Role                 string `json:"role"`
	Database             string `json:"database"`
	PrivateKey           string `json:"privateKey"`
	PrivateKeyPassphrase string `json:"privateKeyPassphrase"`
}

func GetSnowPipeTestCredentials(key string) (*TestCredentials, error) {
	cred, exists := os.LookupEnv(key)
	if !exists {
		return nil, errors.New("snowpipe test credentials not found")
	}

	var credentials TestCredentials
	err := json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		return nil, fmt.Errorf("unable to marshall %s to snowpipe test credentials: %v", key, err)
	}
	return &credentials, nil
}

func RandSchema(provider string) string {
	hex := strings.ToLower(rand.String(12))
	namespace := fmt.Sprintf("test_%s_%d", hex, time.Now().Unix())
	return whutils.ToProviderCase(provider, whutils.ToSafeNamespace(provider,
		namespace,
	))
}

func DropSchema(t *testing.T, db *sql.DB, namespace string) {
	t.Helper()
	t.Log("dropping schema", namespace)

	require.Eventually(t,
		func() bool {
			_, err := db.ExecContext(context.Background(), fmt.Sprintf(`DROP SCHEMA %q CASCADE;`, namespace))
			if err != nil {
				t.Logf("error deleting schema %q: %v", namespace, err)
				return false
			}
			return true
		},
		time.Minute,
		time.Second,
	)
}
