package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
)

// TestCacheStoreSet exercises the upsert: it asserts that writing the
// same config twice does not rewrite the row (updated_at stays put), while
// writing a different config does.
func TestCacheStoreSet(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	resourcePostgres, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	db := resourcePostgres.DB
	require.NoError(t, migrate(db))

	store := &cacheStore{
		DB:     db,
		secret: sha256.Sum256([]byte("secret")),
		key:    "test-workspace",
	}
	ctx := context.Background()

	// expectedHash mirrors the production hashing so the test verifies the
	// stored value independently.
	expectedHash := func(config any) string {
		b, err := jsonrs.Marshal(config)
		require.NoError(t, err)
		sum := sha256.Sum256(b)
		return hex.EncodeToString(sum[:])
	}

	readRow := func() (updatedAt time.Time, hash string, config []byte) {
		t.Helper()
		require.NoError(t, db.QueryRowContext(ctx,
			`SELECT updated_at, config_hash, config FROM config_cache WHERE key = $1`,
			store.key,
		).Scan(&updatedAt, &hash, &config))
		return updatedAt, hash, config
	}

	configA := map[string]any{"workspace": "A", "sources": []string{"s1", "s2"}}
	configB := map[string]any{"workspace": "B", "sources": []string{"s3"}}

	// first write inserts the row
	require.NoError(t, store.set(ctx, configA))
	firstUpdatedAt, firstHash, firstConfig := readRow()
	require.Equal(t, expectedHash(configA), firstHash)

	// writing an identical config must not rewrite the row
	require.NoError(t, store.set(ctx, configA))
	sameUpdatedAt, sameHash, sameConfig := readRow()
	require.Equal(t, firstUpdatedAt, sameUpdatedAt, "updated_at must not change for identical config")
	require.Equal(t, firstHash, sameHash)
	require.Equal(t, firstConfig, sameConfig, "config blob must not be rewritten for identical config")

	// writing a different config must rewrite the row and bump updated_at
	time.Sleep(time.Millisecond) // ensure NOW() is observably later
	require.NoError(t, store.set(ctx, configB))
	changedUpdatedAt, changedHash, changedConfig := readRow()
	require.True(t, changedUpdatedAt.After(firstUpdatedAt), "updated_at must advance when config changes")
	require.Equal(t, expectedHash(configB), changedHash)
	require.NotEqual(t, firstConfig, changedConfig, "config blob must be rewritten when config changes")
}
