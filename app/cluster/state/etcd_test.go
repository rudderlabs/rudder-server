package state_test

import (
	"context"
	"flag"
	"fmt"
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

	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/app/cluster/state"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/testhelper"
	thEtcd "github.com/rudderlabs/rudder-server/testhelper/etcd"
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

	cleaner := &testhelper.Cleanup{}
	defer cleaner.Run()

	var etcdRes *thEtcd.Resource
	if etcdRes, err = thEtcd.Setup(pool, cleaner); err != nil {
		log.Printf("Could not setup ETCD: %v", err)
		return 1
	}

	etcdHosts = etcdRes.Hosts
	etcdClient = etcdRes.Client

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
	em := state.ETCDManager{
		Config: &state.ETCDConfig{
			Endpoints: etcdHosts,
		},
	}

	err := em.Ping()
	require.NoError(t, err)
	em.Close()
}

func Test_ServerMode(t *testing.T) {
	Init()

	provider := state.ETCDManager{
		Config: &state.ETCDConfig{
			Endpoints:   etcdHosts,
			ReleaseName: "test",
			ServerIndex: "0",
		},
	}
	modeRequestKey := fmt.Sprintf("/%s/SERVER/%s/MODE", provider.Config.ReleaseName, provider.Config.ServerIndex)
	defer provider.Close()

	t.Run("key is missing initially", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		ch := provider.ServerMode(ctx)

		_, err := etcdClient.Put(ctx, modeRequestKey, `{"mode": "DEGRADED", "ack_key": "test-ack/1"}`)
		require.NoError(t, err)
		m, ok := <-ch

		require.True(t, ok)
		require.NoError(t, m.Err())
		require.Equal(t, servermode.DegradedMode, m.Mode())
		require.NoError(t, m.Ack(ctx))

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
				ReleaseName: "test_ack_timeout",
				ServerIndex: "0",
				ACKTimeout:  time.Duration(1),
			},
		}
		modeRequestKey := fmt.Sprintf("/%s/SERVER/%s/MODE", provider.Config.ReleaseName, provider.Config.ServerIndex)

		ch := provider.ServerMode(ctx)

		_, err := etcdClient.Put(ctx, modeRequestKey, `{"mode": "DEGRADED", "ack_key": "test-ack/1"}`)
		require.NoError(t, err)
		m, ok := <-ch

		require.True(t, ok)
		require.NoError(t, m.Err())

		require.ErrorAs(t, m.Ack(ctx), &context.DeadlineExceeded)
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	_, err := etcdClient.Put(ctx, modeRequestKey, `{"mode": "DEGRADED", "ack_key": "test-ack/1"}`)
	require.NoError(t, err)

	ch := provider.ServerMode(ctx)

	t.Log("Should get initial state")
	{
		m, ok := <-ch
		require.True(t, ok)
		require.NoError(t, m.Err())
		require.Equal(t, servermode.DegradedMode, m.Mode())
		require.NoError(t, m.Ack(ctx))

		resp, err := etcdClient.Get(ctx, "test-ack/1")
		require.NoError(t, err)
		require.JSONEq(t, `{"status":"DEGRADED"}`, string(resp.Kvs[0].Value))
	}

	t.Log("update should be received")
	{
		_, err := etcdClient.Put(ctx, modeRequestKey, `{"mode": "NORMAL", "ack_key": "test-ack/2"}`)
		require.NoError(t, err)

		m, ok := <-ch
		require.True(t, ok)
		require.NoError(t, m.Err())
		require.Equal(t, servermode.NormalMode, m.Mode())
		require.NoError(t, m.Ack(ctx))

		resp, err := etcdClient.Get(ctx, "test-ack/2")
		require.NoError(t, err)
		require.JSONEq(t, `{"status":"NORMAL"}`, string(resp.Kvs[0].Value))
	}

	t.Log("update with invalid JSON should return error")
	{
		_, err := etcdClient.Put(ctx, modeRequestKey, `{"mode''`)
		require.NoError(t, err)

		m, ok := <-ch
		require.True(t, ok)
		require.Error(t, m.Err())
	}

	t.Log("update with invalid mode should return error")
	{
		_, err := etcdClient.Put(ctx, modeRequestKey, `{"mode": "NOT_A_MODE", "ack_key": "test-ack/2"}`)
		require.NoError(t, err)

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
				ReleaseName: "test_ack_timeout",
				ServerIndex: "0",
				ACKTimeout:  time.Duration(1),
			},
		}
		defer provider.Close()
		appType := strings.ToUpper(config.GetEnv("APP_TYPE", app.PROCESSOR))
		requestKey := fmt.Sprintf("/%s/SERVER/%s/%s/WORKSPACES", provider.Config.ReleaseName,
			provider.Config.ServerIndex, appType)

		ch := provider.WorkspaceIDs(ctx)

		_, err := etcdClient.Put(ctx, requestKey, `{"mode": "DEGRADED", "ack_key": "test-ack/1"}`)
		require.NoError(t, err)
		m, ok := <-ch

		require.True(t, ok)
		require.NoError(t, m.Err())

		require.ErrorAs(t, m.Ack(ctx), &context.DeadlineExceeded)
	})

	provider := state.ETCDManager{
		Config: &state.ETCDConfig{
			Endpoints:   etcdHosts,
			ReleaseName: "test",
			ServerIndex: "0",
		},
	}
	defer provider.Close()

	appType := strings.ToUpper(config.GetEnv("APP_TYPE", app.PROCESSOR))
	requestKey := fmt.Sprintf("/%s/SERVER/%s/%s/WORKSPACES", provider.Config.ReleaseName, provider.Config.ServerIndex,
		appType)

	t.Run("key is missing initially", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		ch := provider.WorkspaceIDs(ctx)

		_, err := etcdClient.Put(ctx, requestKey, `{"workspaces": "1,2", "ack_key": "test-ack/1"}`)
		require.NoError(t, err)
		m, ok := <-ch

		require.True(t, ok)
		require.NoError(t, m.Err())
		require.Equal(t, []string{"1", "2"}, m.WorkspaceIDs())
		require.NoError(t, m.Ack(ctx))

		resp, err := etcdClient.Get(ctx, "test-ack/1")
		require.NoError(t, err)
		require.JSONEq(t, `{"status":"RELOADED"}`, string(resp.Kvs[0].Value))
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	_, err := etcdClient.Put(ctx, requestKey, `{"workspaces": "1,2", "ack_key": "test-ack/1"}`)
	require.NoError(t, err)

	ch := provider.WorkspaceIDs(ctx)

	t.Log("Should get initial state")
	{
		m, ok := <-ch
		require.True(t, ok)
		require.NoError(t, m.Err())
		require.Equal(t, []string{"1", "2"}, m.WorkspaceIDs())
		require.NoError(t, m.Ack(ctx))

		resp, err := etcdClient.Get(ctx, "test-ack/1")
		require.NoError(t, err)
		require.JSONEq(t, `{"status":"RELOADED"}`, string(resp.Kvs[0].Value))
	}

	t.Log("update should be received")
	{
		_, err := etcdClient.Put(ctx, requestKey, `{"workspaces": "1,2,5", "ack_key": "test-ack/2"}`)
		require.NoError(t, err)

		m, ok := <-ch
		require.True(t, ok)
		require.NoError(t, m.Err())
		require.Equal(t, []string{"1", "2", "5"}, m.WorkspaceIDs())
		require.NoError(t, m.Ack(ctx))

		resp, err := etcdClient.Get(ctx, "test-ack/2")
		require.NoError(t, err)
		require.JSONEq(t, `{"status":"RELOADED"}`, string(resp.Kvs[0].Value))
	}

	t.Log("error if update with invalid JSON ")
	{
		_, err := etcdClient.Put(ctx, requestKey, `{"mode''`)
		require.NoError(t, err)

		m, ok := <-ch
		require.True(t, ok)
		require.Error(t, m.Err())
	}

	t.Log("channel should close after context cancellation")
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
