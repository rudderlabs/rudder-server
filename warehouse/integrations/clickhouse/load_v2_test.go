package clickhouse

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// TestCredentialsV2 covers where the copy engine's literals come from. The s3
// table function only takes literals, and an aws destination need not hold any
// — rudder storage, a role and the shared copy user all authenticate through
// the SDK credential chain — so aws always goes through GetTemporaryS3Cred and
// minio never does.
func TestCredentialsV2(t *testing.T) {
	errMinting := errors.New("sts unreachable")

	newCH := func(objectStorage string, destConfig map[string]any) *ClickhouseV2 {
		ch := NewV2(config.New(), logger.NOP, stats.NOP)
		ch.ObjectStorage = objectStorage
		ch.Warehouse = model.Warehouse{
			Destination: backendconfig.DestinationT{Config: destConfig},
		}
		ch.TemporaryS3Cred = func(*backendconfig.DestinationT) (string, string, string, error) {
			return "tempKey", "tempSecret", "tempToken", nil
		}
		return ch
	}

	// The destination shapes below all reach the same call. They are listed
	// separately because each one used to take a different path, and the point
	// of the change is that none of them does any more.
	for _, tc := range []struct {
		name       string
		destConfig map[string]any
	}{
		{
			name: "rudder storage",
			destConfig: map[string]any{
				"useRudderStorage": true,
			},
		},
		{
			name: "a role",
			destConfig: map[string]any{
				"iamRoleARN": "arn:aws:iam::000000000000:role/rudder",
			},
		},
		{
			// Neither keys nor a role: the shared copy user, whose credentials
			// live in the server's env rather than the destination.
			name:       "no credentials at all",
			destConfig: map[string]any{},
		},
		{
			// Static keys are minted from too, so the statement carries a token
			// that expires rather than the customer's long-term secret.
			name: "static keys",
			destConfig: map[string]any{
				"accessKeyID": "staticKey",
				"accessKey":   "staticSecret",
			},
		},
	} {
		t.Run("s3 with "+tc.name, func(t *testing.T) {
			ch := newCH(warehouseutils.S3, tc.destConfig)

			accessKeyID, secretAccessKey, sessionToken, err := ch.credentials()
			require.NoError(t, err)
			require.Equal(t, "tempKey", accessKeyID)
			require.Equal(t, "tempSecret", secretAccessKey)
			require.Equal(t, "tempToken", sessionToken)
		})
	}

	t.Run("minting failure is reported", func(t *testing.T) {
		ch := newCH(warehouseutils.S3, map[string]any{})
		ch.TemporaryS3Cred = func(*backendconfig.DestinationT) (string, string, string, error) {
			return "", "", "", errMinting
		}

		_, _, _, err := ch.credentials()
		require.ErrorIs(t, err, errMinting)
	})

	t.Run("minted credentials that came back empty are named", func(t *testing.T) {
		ch := newCH(warehouseutils.S3, map[string]any{})
		ch.TemporaryS3Cred = func(*backendconfig.DestinationT) (string, string, string, error) {
			return "", "", "", nil
		}

		_, _, _, err := ch.credentials()
		require.ErrorIs(t, err, errMissingS3Credentials)
	})

	t.Run("minio reads its own keys and gets no token", func(t *testing.T) {
		ch := newCH(warehouseutils.MINIO, map[string]any{
			"accessKeyID":     "minioKey",
			"secretAccessKey": "minioSecret",
		})

		accessKeyID, secretAccessKey, sessionToken, err := ch.credentials()
		require.NoError(t, err)
		require.Equal(t, "minioKey", accessKeyID)
		require.Equal(t, "minioSecret", secretAccessKey)
		require.Empty(t, sessionToken, "minio has no STS to mint from")
	})

	t.Run("missing minio keys are named rather than interpolated", func(t *testing.T) {
		ch := newCH(warehouseutils.MINIO, map[string]any{"accessKeyID": "minioKey"})

		_, _, _, err := ch.credentials()
		require.ErrorIs(t, err, errMissingS3Credentials)
	})

	t.Run("unsupported object storage", func(t *testing.T) {
		ch := newCH(warehouseutils.GCS, map[string]any{})

		_, _, _, err := ch.credentials()
		require.ErrorIs(t, err, errObjectStorageNotSupported)
	})
}
