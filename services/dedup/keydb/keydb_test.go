package keydb

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync/atomic"
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

	db, err := NewKeyDB(t.Context(), conf, stats.NOP, logger.NOP)
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

// Test_KeyDBStartsBeforeKeydb checks that setting up dedup doesn't wait for keydb: the client is created while no
// node is up, with the default retry policy, and a request sent meanwhile is served once a node is.
func Test_KeyDBStartsBeforeKeydb(t *testing.T) {
	port, err := testhelper.GetFreePort()
	require.NoError(t, err)
	conf := config.New()
	conf.Set("KeyDB.Dedup.Addresses", "127.0.0.1:"+strconv.Itoa(port))

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	start := time.Now()
	db, err := NewKeyDB(ctx, conf, stats.NOP, logger.NOP)
	require.NoError(t, err)
	defer db.Close()
	require.Less(t, time.Since(start), time.Second, "creating the client must not wait for keydb")

	done := make(chan error, 1)
	go func() { done <- db.Set([]string{"early_key"}) }()
	time.Sleep(300 * time.Millisecond) // the request waits for keydb meanwhile
	startKeydbOn(t, conf, port)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("the request sent before keydb was up never finished")
	}
	result, err := db.Get([]string{"early_key"})
	require.NoError(t, err)
	require.True(t, result["early_key"])
}

func startKeydb(t testing.TB, conf *config.Config) {
	t.Helper()
	freePort, err := testhelper.GetFreePort()
	require.NoError(t, err)
	startKeydbOn(t, conf, freePort)
}

// startKeydbOn starts a keydb node on freePort and points conf at it
func startKeydbOn(t testing.TB, conf *config.Config, freePort int) {
	t.Helper()

	var service *keydbnode.Service
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// the processors are given the keydb headless Service, an address no node advertises
	seed := "localhost:" + strconv.Itoa(freePort)
	address := "127.0.0.1:" + strconv.Itoa(freePort)
	conf.Set("KeyDB.Dedup.Addresses", seed)
	conf.Set("KeyDB.Dedup.RetryCount", 3)

	nodeConfig := keydbnode.Config{
		NodeID:           0,
		TotalHashRanges:  128,
		SnapshotInterval: time.Minute,
		Addresses:        func() []string { return []string{address} },
	}

	service, err := keydbnode.NewService(ctx, nodeConfig, &mockedCloudStorage{}, conf, stats.NOP, logger.NOP)
	require.NoError(t, err)

	// Create a gRPC server
	var getNodeInfoCalls atomic.Int64
	server := grpc.NewServer(grpc.UnaryInterceptor(
		func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if info.FullMethod == keydbproto.NodeService_GetNodeInfo_FullMethodName {
				getNodeInfoCalls.Add(1)
			}
			return handler(ctx, req)
		},
	))
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

	// The Service can also list a pod the node list doesn't have, like a shadow during a node move, which nothing
	// listens for here: the client must ask the node behind it for the node list, whatever localhost resolves to.
	unlistedPort, err := testhelper.GetFreePort()
	require.NoError(t, err)
	behindSeed := []string{address, "127.0.0.1:" + strconv.Itoa(unlistedPort)}
	c, err := keydbclient.NewClient(t.Context(), keydbclient.Config{
		Addresses:       []string{seed},
		TotalHashRanges: 128,
		RetryPolicy: keydbclient.RetryPolicy{
			Disabled: true,
		},
	}, logger.NOP, keydbclient.WithAddressResolver(func(context.Context, string) ([]string, error) {
		return behindSeed, nil
	}))
	require.NoError(t, err)
	_, err = c.Get(t.Context(), []string{"startup-check"}) // the first answer settles the client's node list
	require.NoError(t, err)
	require.EqualValues(t, 1, c.ClusterSize())
	require.Positive(t, getNodeInfoCalls.Load(), "the client never asked the node for the node list")
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
