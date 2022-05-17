package state_test

import (
	"context"
	"flag"
	"fmt"
	"github.com/rudderlabs/rudder-server/app"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	etcd "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/rudderlabs/rudder-server/app/cluster/state"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/types/servermode"
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
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Printf("Could not connect to docker: %s \n", err)
		return 1
	}

	container, err := pool.Run("bitnami/etcd", "3.4", []string{
		"ALLOW_NONE_AUTHENTICATION=yes",
	})
	if err != nil {
		log.Printf("Could not start resource: %s", err)
		return 1
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
		log.Printf("Could not connect to dockerized etcd: %s \n", err)
		return 1
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

func Init() {
	config.Load()
	stats.Setup()
	logger.Init()
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
	Init()

	provider := state.ETCDManager{
		Config: &state.ETCDConfig{
			Endpoints:   etcdHosts,
			Namespace:   "test",
			ServerIndex: "0",
		},
	}
	modeRequestKey := fmt.Sprintf("/%s/server/%s/mode", provider.Config.Namespace, provider.Config.ServerIndex)
	defer provider.Close()

	t.Run("key is missing initially", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		ch := provider.ServerMode(ctx)

		etcdClient.Put(ctx, modeRequestKey, `{"mode": "DEGRADED", "ack_key": "test-ack/1"}`)
		m, ok := <-ch

		require.True(t, ok)
		require.NoError(t, m.Err())
		require.Equal(t, servermode.DegradedMode, m.Mode())
		m.Ack(ctx)

		resp, err := etcdClient.Get(ctx, "test-ack/1")
		require.NoError(t, err)
		require.JSONEq(t, `{"status":"DEGRADED"}`, string(resp.Kvs[0].Value))

	})

	t.Run("ack timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		provider := state.ETCDManager{
			Config: &state.ETCDConfig{
				Endpoints:   etcdHosts,
				Namespace:   "test_ack_timeout",
				ServerIndex: "0",
				ACKTimeout:  time.Duration(1),
			},
		}
		modeRequestKey := fmt.Sprintf("/%s/server/%s/mode", provider.Config.Namespace, provider.Config.ServerIndex)

		ch := provider.ServerMode(ctx)

		etcdClient.Put(ctx, modeRequestKey, `{"mode": "DEGRADED", "ack_key": "test-ack/1"}`)
		m, ok := <-ch

		require.True(t, ok)
		require.NoError(t, m.Err())

		require.ErrorAs(t, m.Ack(ctx), &context.DeadlineExceeded)
	})

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
		m.Ack(ctx)

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
		m.Ack(ctx)

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

	t.Log("channel should close after context cancelation")
	cancel()
	{
		_, ok := <-ch
		require.False(t, ok)
	}
	cancel()

}

func Test_Workspaces(t *testing.T) {
	Init()
	t.Run("ack timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		provider := state.ETCDManager{
			Config: &state.ETCDConfig{
				Endpoints:   etcdHosts,
				Namespace:   "test_ack_timeout",
				ServerIndex: "0",
				ACKTimeout:  time.Duration(1),
			},
		}
		defer provider.Close()
		appType := strings.ToUpper(config.GetEnv("APP_TYPE", app.PROCESSOR))
		requestKey := fmt.Sprintf("/%s/server/%s/%s/workspaces", provider.Config.Namespace,
			provider.Config.ServerIndex, appType)

		ch := provider.WorkspaceIDs(ctx)

		etcdClient.Put(ctx, requestKey, `{"mode": "DEGRADED", "ack_key": "test-ack/1"}`)
		m, ok := <-ch

		require.True(t, ok)
		require.NoError(t, m.Err())

		require.ErrorAs(t, m.Ack(ctx), &context.DeadlineExceeded)
	})

	provider := state.ETCDManager{
		Config: &state.ETCDConfig{
			Endpoints:   etcdHosts,
			Namespace:   "test",
			ServerIndex: "0",
		},
	}
	defer provider.Close()

	appType := strings.ToUpper(config.GetEnv("APP_TYPE", app.PROCESSOR))
	requestKey := fmt.Sprintf("/%s/server/%s/%s/workspaces", provider.Config.Namespace, provider.Config.ServerIndex,
		appType)

	t.Run("key is missing initially", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		ch := provider.WorkspaceIDs(ctx)

		etcdClient.Put(ctx, requestKey, `{"workspaces": "1,2", "ack_key": "test-ack/1"}`)
		m, ok := <-ch

		require.True(t, ok)
		require.NoError(t, m.Err())
		require.Equal(t, []string{"1", "2"}, m.WorkspaceIDs())
		m.Ack(ctx)

		resp, err := etcdClient.Get(ctx, "test-ack/1")
		require.NoError(t, err)
		require.JSONEq(t, `{"status":"RELOADED"}`, string(resp.Kvs[0].Value))
	})

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
		m.Ack(ctx)

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
		m.Ack(ctx)

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
