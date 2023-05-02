package sqlquerywrapper

import (
	"context"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	rsLogger "github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/stretchr/testify/require"
)

func TestTxWithTimeout_Rollback(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pgResource, err := resource.SetupPostgres(pool, t)
	require.NoError(t, err)

	t.Run("rollback success", func(t *testing.T) {
		ctx := context.Background()

		db := New(pgResource.DB)
		db.since = time.Since
		db.logger = rsLogger.NOP

		tx, err := db.Begin()
		require.NoError(t, err)

		txn := txWithTimeout{
			Tx:      tx,
			timeout: 1 * time.Second,
		}

		_, err = txn.ExecContext(ctx, "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)")
		require.NoError(t, err)

		_, err = txn.ExecContext(ctx, "INSERT INTO users (name) VALUES ('Alice'), ('Bob')")
		require.NoError(t, err)

		err = txn.Rollback()
		require.NoError(t, err)

		var count int
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'users'").Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 0, count)
	})

	t.Run("context timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		db := New(pgResource.DB)
		db.since = time.Since
		db.logger = rsLogger.NOP

		tx, err := db.Begin()
		require.NoError(t, err)

		txt := txWithTimeout{
			Tx: tx,
		}

		_, err = txt.ExecContext(ctx, "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)")
		require.NoError(t, err)

		_, err = txt.ExecContext(ctx, "INSERT INTO users (name) VALUES ('Alice'), ('Bob')")
		require.NoError(t, err)

		err = txt.Rollback()
		require.EqualError(t, err, "rollback timed out after 0s")
	})
}
