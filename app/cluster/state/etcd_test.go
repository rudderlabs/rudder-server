package state_test

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/ory/dockertest"
	"github.com/rudderlabs/rudder-server/app/cluster/state"
	"github.com/rudderlabs/rudder-server/utils/types/servermode"
	"github.com/stretchr/testify/require"
	etcd "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

var (
	hold       bool
	etcdHosts  = []string{"http://localhost:2379"}
	etcdClient *etcd.Client
)

func TestMain(m *testing.M) {
	flag.BoolVar(&hold, "hold", false, "hold environment clean-up after test execution until Ctrl+C is provided")
	flag.Parse()

	// hack to make defer work, without being affected by the os.Exit in TestMain
	os.Exit(run(m))
}

func run(m *testing.M) int {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	// pulls an image, creates a container based on it and runs it
	container, err := pool.Run("bitnami/etcd", "3.4", []string{
		"ALLOW_NONE_AUTHENTICATION=yes",
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	defer func() {
		if err := pool.Purge(container); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()

	etcdHosts = []string{"http://localhost:" + container.GetPort("2379/tcp")}
	if err := pool.Retry(func() error {
		var err error

		etcdClient, err = etcd.New(etcd.Config{
			Endpoints: etcdHosts,
			DialOptions: []grpc.DialOption{
				grpc.WithBlock(), // block until the underlying connection is up
			},
		})
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		log.Fatalf("Could not connect to dockerized etcd: %s", err)
	}

	code := m.Run()
	blockOnHold()

	return code
}

func blockOnHold() {
	if !hold {
		return
	}

	fmt.Println("Test on hold, before cleanup")
	fmt.Println("Press Ctrl+C to exit")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
}

func Test_Ping(t *testing.T) {
	etcd := state.ETCDManager{
		Config: &state.ETCDConfig{
			Endpoints: etcdHosts,
		},
	}

	err := etcd.Ping()
	require.NoError(t, err)

	etcd.Close()
}

func Test_ServerMode(t *testing.T) {
	provider := state.ETCDManager{
		Config: &state.ETCDConfig{
			Endpoints:   etcdHosts,
			ReleaseName: "test",
			ServerIndex: "0",
		},
	}
	modeRequestKey := fmt.Sprintf("/%s/server/%s/mode", provider.Config.ReleaseName, provider.Config.ServerIndex)
	defer provider.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	etcdClient.Put(ctx, modeRequestKey, `{"mode": "DEGRADED", "ack_key": "test-ack/1"}`)

	ch := provider.ServerMode(ctx)

	t.Log("Should get initial state")
	{
		m, ok := <-ch
		require.True(t, ok)
		require.NoError(t, m.Err())
		require.Equal(t, servermode.DegradedMode, m.Mode())
		m.Ack()

		resp, err := etcdClient.Get(ctx, "test-ack/1")
		require.NoError(t, err)
		require.JSONEq(t, `{"status":"DEGRADED"}`, string(resp.Kvs[0].Value))
	}

	t.Log("update should be received")
	{
		etcdClient.Put(ctx, modeRequestKey, `{"mode": "NORMAL", "ack_key": "test-ack/2"}`)

		m, ok := <-ch
		require.True(t, ok)
		require.NoError(t, m.Err())
		require.Equal(t, servermode.NormalMode, m.Mode())
		m.Ack()

		resp, err := etcdClient.Get(ctx, "test-ack/2")
		require.NoError(t, err)
		require.JSONEq(t, `{"status":"NORMAL"}`, string(resp.Kvs[0].Value))
	}

	t.Log("update with invalid JSON should return error")
	{
		etcdClient.Put(ctx, modeRequestKey, `{"mode''`)

		m, ok := <-ch
		require.True(t, ok)
		require.Error(t, m.Err())
	}

	t.Log("update with invalid mode should return error")
	{
		etcdClient.Put(ctx, modeRequestKey, `{"mode": "NOT_A_MODE", "ack_key": "test-ack/2"}`)

		m, ok := <-ch
		require.True(t, ok)
		require.Error(t, m.Err())
	}

	t.Log("delete key should return error")
	{
		etcdClient.Delete(ctx, modeRequestKey)

		m, ok := <-ch
		require.True(t, ok)
		require.Error(t, m.Err())
	}

	t.Log("channel should close after context cancelation")
	cancel()
	{
		_, ok := <-ch
		require.False(t, ok)
	}

	t.Log("error if key is missing")
	{
		ctx, cancel := context.WithCancel(context.Background())
		ch := provider.ServerMode(ctx)
		m, ok := <-ch
		require.True(t, ok)
		require.Error(t, m.Err())
		cancel()
	}

}

func Test_Workspaces(t *testing.T) {
	provider := state.ETCDManager{
		Config: &state.ETCDConfig{
			Endpoints:   etcdHosts,
			ReleaseName: "test",
			ServerIndex: "0",
		},
	}
	requestKey := fmt.Sprintf("/%s/server/%s/workspaces", provider.Config.ReleaseName, provider.Config.ServerIndex)
	defer provider.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	etcdClient.Put(ctx, requestKey, `{"workspaces": "1,2", "ack_key": "test-ack/1"}`)

	ch := provider.WorkspaceIDs(ctx)

	t.Log("Should get initial state")
	{
		m, ok := <-ch
		require.True(t, ok)
		require.NoError(t, m.Err())
		require.Equal(t, []string{"1", "2"}, m.WorkspaceIDs())
		m.Ack()

		resp, err := etcdClient.Get(ctx, "test-ack/1")
		require.NoError(t, err)
		require.JSONEq(t, `{"status":"RELOADED"}`, string(resp.Kvs[0].Value))
	}

	t.Log("update should be received")
	{
		etcdClient.Put(ctx, requestKey, `{"workspaces": "1,2,5", "ack_key": "test-ack/2"}`)

		m, ok := <-ch
		require.True(t, ok)
		require.NoError(t, m.Err())
		require.Equal(t, []string{"1", "2", "5"}, m.WorkspaceIDs())
		m.Ack()

		resp, err := etcdClient.Get(ctx, "test-ack/2")
		require.NoError(t, err)
		require.JSONEq(t, `{"status":"RELOADED"}`, string(resp.Kvs[0].Value))
	}

	t.Log("error if update with invalid JSON ")
	{
		etcdClient.Put(ctx, requestKey, `{"mode''`)

		m, ok := <-ch
		require.True(t, ok)
		require.Error(t, m.Err())
	}

	t.Log("error if key is deleted")
	{
		etcdClient.Delete(ctx, requestKey)

		m, ok := <-ch
		require.True(t, ok)
		require.Error(t, m.Err())
	}

	t.Log("channel should close after context cancelation")
	cancel()
	{
		_, ok := <-ch
		require.False(t, ok)
	}

	t.Log("error if key is missing")
	{
		ctx, cancel := context.WithCancel(context.Background())
		ch := provider.WorkspaceIDs(ctx)
		m, ok := <-ch
		require.True(t, ok)
		require.Error(t, m.Err())
		cancel()
	}

}
