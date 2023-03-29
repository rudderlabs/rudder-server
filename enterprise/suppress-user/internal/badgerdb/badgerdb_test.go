package badgerdb_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/gorilla/mux"

	"github.com/google/uuid"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/internal/badgerdb"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
	"github.com/stretchr/testify/require"
)

type readerFunc func(p []byte) (n int, err error)

func (f readerFunc) Read(p []byte) (n int, err error) {
	return f(p)
}

// TestBadgerRepository contains badgerdb-specific tests.
func TestBadgerRepository(t *testing.T) {
	basePath := path.Join(t.TempDir(), strings.ReplaceAll(uuid.New().String(), "-", ""))
	token := []byte("token")
	repo, err := badgerdb.NewRepository(basePath, false, logger.NOP, stats.Default)
	require.NoError(t, err)

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
			},
			{
				WorkspaceID: "workspace2",
				UserID:      "user2",
				SourceIDs:   []string{"source1"},
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
		_, err := badgerdb.NewRepository(basePath, false, logger.NOP, stats.Default)
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
		_, err := badgerdb.NewRepository(basePath, false, logger.NOP, stats.Default, badgerdb.WithSeederSource(func() (io.Reader, error) {
			return buffer, nil
		}), badgerdb.WithMaxSeedWait(1*time.Millisecond))
		require.NoError(t, err)
	})
	t.Run("new with use of suppression backup service", func(t *testing.T) {
		srv := httptest.NewServer(httpHandler(t))
		defer t.Cleanup(srv.Close)
		t.Setenv("WORKSPACE_TOKEN", "216Co97d9So9TkqphM0cxBzRxc3")
		t.Setenv("CONFIG_BACKEND_URL", srv.URL)
		t.Setenv("SUPPRESS_USER_BACKEND_URL", srv.URL)
		t.Setenv("SUPPRESS_BACKUP_URL", srv.URL)
		dir, err := os.MkdirTemp("/tmp", "rudder-server")
		require.NoError(t, err)
		t.Setenv("RUDDER_TMPDIR", dir)
		basePath := path.Join(t.TempDir(), "badger-test-3")
		_, err = badgerdb.NewRepository(basePath, true, logger.NOP, stats.Default)
		require.NoError(t, err)
		defer os.RemoveAll(dir)
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
		}}, []byte("token")))
	})

	t.Run("trying to use a closed repository", func(t *testing.T) {
		repo, err := badgerdb.NewRepository(basePath, false, logger.NOP, stats.Default)
		require.NoError(t, err)
		require.NoError(t, repo.Stop())

		require.Equal(t, repo.Add(nil, nil), badger.ErrDBClosed)

		s, err := repo.Suppressed("", "", "")
		require.False(t, s)
		require.Equal(t, err, badger.ErrDBClosed)

		_, err = repo.GetToken()
		require.Equal(t, err, badger.ErrDBClosed)

		require.Equal(t, repo.Backup(nil), badger.ErrDBClosed)

		require.Equal(t, repo.Restore(nil), badger.ErrDBClosed)
	})
}

func httpHandler(t *testing.T) http.Handler {
	t.Helper()
	srvMux := mux.NewRouter()
	srvMux.HandleFunc("/workspaceConfig", getSingleTenantWorkspaceConfig).Methods(http.MethodGet)
	srvMux.HandleFunc("/full-export", func(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, "testdata/full-export") }).Methods(http.MethodGet)
	srvMux.HandleFunc("/latest-export", func(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, "testdata/latest-export") }).Methods(http.MethodGet)
	srvMux.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req)
		})
	})

	return srvMux
}

func getSingleTenantWorkspaceConfig(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	config := backendconfig.ConfigT{
		WorkspaceID: "reg-test-workspaceId",
	}
	body, err := json.Marshal(config)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(body)
}
