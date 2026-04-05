package keydb

import (
	"context"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	keydbclient "github.com/rudderlabs/keydb/client"
	keydbnode "github.com/rudderlabs/keydb/node"
	keydbproto "github.com/rudderlabs/keydb/proto"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper"
)

func Test_KeyDB(t *testing.T) {
	conf := config.New()
	startKeydb(t, conf)

	db, err := NewKeyDB(conf, stats.NOP, logger.NOP)
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()

	t.Run("key not present in db", func(t *testing.T) {
		keys := []string{"test_key_1"}
		result, err := db.Get(keys)
		require.NoError(t, err)
		require.Empty(t, result)
	})

	t.Run("set and get keys", func(t *testing.T) {
		keys := []string{"test_key_2", "test_key_3"}

		// Set keys
		err := db.Set(keys)
		require.NoError(t, err)

		// Get keys
		result, err := db.Get(keys)
		require.NoError(t, err)
		require.Len(t, result, 2)
		require.True(t, result["test_key_2"])
		require.True(t, result["test_key_3"])
	})

	t.Run("mixed existing and non-existing keys", func(t *testing.T) {
		existingKeys := []string{"test_key_4"}
		newKeys := []string{"test_key_5"}
		allKeys := append(existingKeys, newKeys...)

		// Set only some keys
		err := db.Set(existingKeys)
		require.NoError(t, err)

		// Get all keys
		result, err := db.Get(allKeys)
		require.NoError(t, err)
		require.Len(t, result, 1)
		require.True(t, result["test_key_4"])
		_, exists := result["test_key_5"]
		require.False(t, exists)
	})
}

func startKeydb(t testing.TB, conf *config.Config) {
	t.Helper()

	freePort, err := testhelper.GetFreePort()
	require.NoError(t, err)

	var service *keydbnode.Service
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	address := "localhost:" + strconv.Itoa(freePort)
	conf.Set("KeyDB.Dedup.Addresses", address)
	conf.Set("KeyDB.Dedup.RetryCount", 3)

	nodeConfig := keydbnode.Config{
		NodeID:           0,
		TotalHashRanges:  128,
		SnapshotInterval: time.Minute,
		Addresses:        func() []string { return []string{address} },
	}

	require.NoError(t, err)
	service, err = keydbnode.NewService(ctx, nodeConfig, &mockedCloudStorage{}, conf, stats.NOP, logger.NOP)
	require.NoError(t, err)

	// Create a gRPC server
	server := grpc.NewServer()
	keydbproto.RegisterNodeServiceServer(server, service)

	lis, err := net.Listen("tcp", address)
	require.NoError(t, err)

	// Start the server
	go func() {
		require.NoError(t, server.Serve(lis))
	}()
	t.Cleanup(func() {
		cancel()
		server.GracefulStop()
		_ = lis.Close()
		service.Close()
	})

	c, err := keydbclient.NewClient(keydbclient.Config{
		Addresses:       []string{address},
		TotalHashRanges: 128,
		RetryPolicy: keydbclient.RetryPolicy{
			Disabled: true,
		},
	}, logger.NOP)
	require.NoError(t, err)
	size := c.ClusterSize()
	require.NoError(t, err)
	require.EqualValues(t, 1, size)
	require.NoError(t, c.Close())

	t.Logf("keydb address: %s", address)
}

type mockedFilemanagerSession struct{}

func (m *mockedFilemanagerSession) Next() (fileObjects []*filemanager.FileInfo, err error) {
	return nil, nil
}

type mockedCloudStorage struct{}

func (m *mockedCloudStorage) Download(_ context.Context, _ io.WriterAt, _ string, _ ...filemanager.DownloadOption) error {
	return nil
}

func (m *mockedCloudStorage) Delete(_ context.Context, _ []string) error {
	return nil
}

func (m *mockedCloudStorage) UploadReader(_ context.Context, _ string, _ io.Reader) (filemanager.UploadedFile, error) {
	return filemanager.UploadedFile{}, nil
}

func (m *mockedCloudStorage) ListFilesWithPrefix(_ context.Context, _, _ string, _ int64) filemanager.ListSession {
	return &mockedFilemanagerSession{}
}
