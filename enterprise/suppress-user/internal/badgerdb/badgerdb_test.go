package badgerdb_test

import (
	"bytes"
	"context"
	"io"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/internal/badgerdb"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
)

type readerFunc func(p []byte) (n int, err error)

func (f readerFunc) Read(p []byte) (n int, err error) {
	return f(p)
}

// TestBadgerRepository contains badgerdb-specific tests.
func TestBadgerRepository(t *testing.T) {
	basePath := path.Join(t.TempDir(), strings.ReplaceAll(uuid.New().String(), "-", ""))
	token := []byte("token")
	repo, err := badgerdb.NewRepository(basePath, logger.NOP, stats.Default)
	require.NoError(t, err)

	t.Run("adding suppressions with createdAt", func(t *testing.T) {
		err := repo.Add([]model.Suppression{
			{
				WorkspaceID: "workspace1",
				UserID:      "user1",
				SourceIDs:   []string{},
				CreatedAt:   time.Date(2020, time.March, 27, 2, 2, 1, 2, time.UTC),
			},
		}, token)
		require.NoError(t, err)
		metadata, err := repo.Suppressed("workspace1", "user1", "")
		require.NoError(t, err)
		require.Equal(t, time.Date(2020, time.March, 27, 2, 2, 1, 2, time.UTC), metadata.CreatedAt) // should be the same as the one we added
		metadata, err = repo.Suppressed("workspace2", "user1", "")
		require.Error(t, err)
		require.Nil(t, metadata) // should be nil

		// Add a suppression with a source ID
		err = repo.Add([]model.Suppression{
			{
				WorkspaceID: "workspace2",
				UserID:      "user2",
				SourceIDs:   []string{"source2"},
				CreatedAt:   time.Date(2019, time.March, 27, 2, 2, 1, 2, time.UTC),
			},
		}, token)
		require.NoError(t, err)
		metadata, err = repo.Suppressed("workspace2", "user2", "source1") // wrong source ID
		require.Error(t, err)
		require.Nil(t, metadata)
		metadata, err = repo.Suppressed("workspace2", "user2", "source2") // wrong workspace and user ID and correct source ID
		require.NoError(t, err)
		require.NotNil(t, metadata)
		require.Equal(t, time.Date(2019, time.March, 27, 2, 2, 1, 2, time.UTC), metadata.CreatedAt) // should be the same as the one we added
	})

	t.Run("trying to use a repository during restore", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var reader readerFunc = func(_ []byte) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		}
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			require.Error(t, repo.Restore(reader))
			wg.Done()
		}()

		time.Sleep(1 * time.Millisecond)
		err := repo.Add([]model.Suppression{
			{
				WorkspaceID: "workspace1",
				UserID:      "user1",
				SourceIDs:   []string{},
				CreatedAt:   time.Now(),
			},
			{
				WorkspaceID: "workspace2",
				UserID:      "user2",
				SourceIDs:   []string{"source1"},
				CreatedAt:   time.Now(),
			},
		}, token)
		require.Error(t, err, "it should return an error when trying to add a suppression to a repository that is restoring")
		require.ErrorIs(t, model.ErrRestoring, err)

		err = repo.Restore(nil)
		require.Error(t, err, "it should return an error when trying to restore a repository that is already restoring")
		require.ErrorIs(t, model.ErrRestoring, err)

		err = repo.Backup(nil)
		require.Error(t, err, "it should return an error when trying to backup a repository that is already restoring")
		require.ErrorIs(t, model.ErrRestoring, err)

		_, err = repo.GetToken()
		require.Error(t, err, "it should return an error when trying to get the token from a repository that is restoring")
		require.ErrorIs(t, model.ErrRestoring, err)

		_, err = repo.Suppressed("workspace2", "user2", "source2")
		require.Error(t, err, "it should return an error when trying to suppress a user from a repository that is restoring")
		require.ErrorIs(t, model.ErrRestoring, err)

		cancel()
		wg.Wait() // wait for the restore to finish
	})

	defer func() { _ = repo.Stop() }()

	t.Run("trying to start a second repository using the same path", func(t *testing.T) {
		_, err := badgerdb.NewRepository(basePath, logger.NOP, stats.Default)
		require.Error(t, err, "it should return an error when trying to start a second repository using the same path")
	})

	backup := []byte{}
	buffer := bytes.NewBuffer(backup)
	t.Run("backup", func(t *testing.T) {
		require.NoError(t, repo.Backup(buffer), "it should be able to backup the repository without an error")
	})

	t.Run("restore", func(t *testing.T) {
		require.NoError(t, repo.Restore(buffer), "it should be able to restore the repository without an error")
	})

	t.Run("new with seeder", func(t *testing.T) {
		basePath := path.Join(t.TempDir(), "badger-test-2")
		_, err := badgerdb.NewRepository(basePath, logger.NOP, stats.Default, badgerdb.WithSeederSource(func() (io.ReadCloser, error) {
			return io.NopCloser(buffer), nil
		}), badgerdb.WithMaxSeedWait(1*time.Millisecond))
		require.NoError(t, err)
	})

	t.Run("try to restore invalid data", func(t *testing.T) {
		r := bytes.NewBuffer([]byte("invalid data"))
		require.Error(t, repo.Restore(r), "it should return an error when trying to restore invalid data")
	})

	t.Run("badgerdb errors", func(t *testing.T) {
		require.NoError(t, repo.Stop(), "it should be able to stop the badgerdb instance without an error")

		_, err := repo.Suppressed("workspace1", "user1", "")
		require.Error(t, err)

		_, err = repo.GetToken()
		require.Error(t, err)

		require.Error(t, repo.Add([]model.Suppression{}, []byte("")))

		require.Error(t, repo.Add([]model.Suppression{{
			WorkspaceID: "workspace1",
			UserID:      "user1",
			SourceIDs:   []string{},
			CreatedAt:   time.Now(),
		}}, []byte("token")))
	})

	t.Run("trying to use a closed repository", func(t *testing.T) {
		repo, err := badgerdb.NewRepository(basePath, logger.NOP, stats.Default)
		require.NoError(t, err)
		require.NoError(t, repo.Stop())

		require.Equal(t, repo.Add(nil, nil), badger.ErrDBClosed)

		s, err := repo.Suppressed("", "", "")
		require.Nil(t, s)
		require.Equal(t, err, badger.ErrDBClosed)

		_, err = repo.GetToken()
		require.Equal(t, err, badger.ErrDBClosed)

		require.Equal(t, repo.Backup(nil), badger.ErrDBClosed)

		require.Equal(t, repo.Restore(nil), badger.ErrDBClosed)
	})
}
