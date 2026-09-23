package clickhouse

import (
	"errors"
	"strings"
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

	newCH := func(objectStorage string, destConfig map[string]any) *Clickhouse {
		ch := New(config.New(), logger.NOP, stats.NOP)
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

// TestS3CopySettingsV2 covers the SETTINGS the copy statement carries. The copy
// reads a whole folder of gzipped CSV in one INSERT ... SELECT, so its peak
// memory follows parse and insert parallelism; these settings are the only way
// to bound it from here. Nothing is set by default, so a deployment that
// configures none of them keeps sending the statement it sent before.
func TestS3CopySettingsV2(t *testing.T) {
	const workspaceID = "workspace-1"

	settingsFor := func(set func(*config.Config)) []string {
		conf := config.New()
		if set != nil {
			set(conf)
		}
		return New(conf, logger.NOP, stats.NOP).config.s3CopySettings(workspaceID)
	}

	t.Run("unset by default", func(t *testing.T) {
		require.Empty(t, settingsFor(nil))
	})

	t.Run("global keys apply", func(t *testing.T) {
		require.Equal(t,
			[]string{"max_threads = 4", "max_memory_usage = 21474836480"},
			settingsFor(func(c *config.Config) {
				c.Set("Warehouse.clickhouse.v2.s3Copy.maxThreads", 4)
				c.Set("Warehouse.clickhouse.v2.s3Copy.maxMemoryUsage", 21474836480)
			}),
		)
	})

	t.Run("workspace key wins over global", func(t *testing.T) {
		require.Equal(t,
			[]string{"max_threads = 2"},
			settingsFor(func(c *config.Config) {
				c.Set("Warehouse.clickhouse.v2.s3Copy.maxThreads", 8)
				c.Set("Warehouse.clickhouse.v2."+workspaceID+".s3Copy.maxThreads", 2)
			}),
		)
	})

	// The docs pair min_insert_block_size_bytes with rows at zero, so zero has
	// to reach the statement rather than read as unset.
	t.Run("zero is a value, not unset", func(t *testing.T) {
		require.Equal(t,
			[]string{"min_insert_block_size_bytes = 268435456", "min_insert_block_size_rows = 0"},
			settingsFor(func(c *config.Config) {
				c.Set("Warehouse.clickhouse.v2.s3Copy.minInsertBlockSizeBytes", 268435456)
				c.Set("Warehouse.clickhouse.v2.s3Copy.minInsertBlockSizeRows", 0)
			}),
		)
	})

	t.Run("parallel parsing is disabled by name, not by number", func(t *testing.T) {
		require.Equal(t,
			[]string{"input_format_parallel_parsing = 0"},
			settingsFor(func(c *config.Config) {
				c.Set("Warehouse.clickhouse.v2.s3Copy.disableParallelParsing", true)
			}),
		)
	})

	// Spelled out rather than derived, so a typo in a ClickHouse setting name
	// fails here rather than on the server mid-load.
	t.Run("every setting maps to its ClickHouse name", func(t *testing.T) {
		require.Equal(t,
			[]string{
				"max_threads = 1",
				"max_parsing_threads = 2",
				"max_insert_threads = 3",
				"max_memory_usage = 4",
				"min_insert_block_size_bytes = 5",
				"min_insert_block_size_rows = 6",
				"input_format_parallel_parsing = 0",
			},
			settingsFor(func(c *config.Config) {
				c.Set("Warehouse.clickhouse.v2.s3Copy.maxThreads", 1)
				c.Set("Warehouse.clickhouse.v2.s3Copy.maxParsingThreads", 2)
				c.Set("Warehouse.clickhouse.v2.s3Copy.maxInsertThreads", 3)
				c.Set("Warehouse.clickhouse.v2.s3Copy.maxMemoryUsage", 4)
				c.Set("Warehouse.clickhouse.v2.s3Copy.minInsertBlockSizeBytes", 5)
				c.Set("Warehouse.clickhouse.v2.s3Copy.minInsertBlockSizeRows", 6)
				c.Set("Warehouse.clickhouse.v2.s3Copy.disableParallelParsing", true)
			}),
		)
	})

	t.Run("statement carries them after the two it always needs", func(t *testing.T) {
		base := copySQLStatement("ns", "tracks", "id", []string{"'folder'"}, nil)
		require.Contains(t, base, "date_time_input_format = 'best_effort'")
		require.Contains(t, base, "input_format_csv_arrays_as_nested_csv = 1")
		require.NotContains(t, base, "max_threads")

		tuned := copySQLStatement("ns", "tracks", "id", []string{"'folder'"},
			[]string{"max_threads = 4", "input_format_parallel_parsing = 0"})
		require.Contains(t, tuned, "date_time_input_format = 'best_effort'")
		require.Contains(t, tuned, "max_threads = 4")
		require.Contains(t, tuned, "input_format_parallel_parsing = 0")
		// One settings keyword, one terminator: the clause stays a single list.
		require.Equal(t, 1, strings.Count(tuned, "settings"))
		require.Equal(t, 1, strings.Count(tuned, ";"))
	})
}
