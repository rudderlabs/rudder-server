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

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/app/cluster/state"
	"github.com/rudderlabs/rudder-server/testhelper"
	thEtcd "github.com/rudderlabs/rudder-server/testhelper/etcd"
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
	config.Reset()
	logger.Reset()
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

	t.Run("goes back to initial state when interrupted (DEGRADED)", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		_, err := etcdClient.Put(ctx, modeRequestKey, `{"mode": "DEGRADED", "ack_key": "test-ack/1"}`)
		require.NoError(t, err)
		// Was in degraded mode previously before interruption/Expected to be in degraded mode when starts

		ch := provider.ServerMode(ctx)
		m, ok := <-ch

		require.True(t, ok)
		require.NoError(t, m.Err())
		require.Equal(t, servermode.DegradedMode, m.Mode()) // should start in degraded mode
		require.NoError(t, m.Ack(ctx))

		resp, err := etcdClient.Get(ctx, "test-ack/1")
		require.NoError(t, err)
		require.JSONEq(t, `{"status":"DEGRADED"}`, string(resp.Kvs[0].Value))
	})

	t.Run("goes back to initial state when interrupted (NORMAL)", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		_, err := etcdClient.Put(ctx, modeRequestKey, `{"mode": "NORMAL", "ack_key": "test-ack/1"}`)
		require.NoError(t, err)
		// Was in normal mode previously before interruption/Expected to be in normal mode when starts

		ch := provider.ServerMode(ctx)
		m, ok := <-ch

		require.True(t, ok)
		require.NoError(t, m.Err())
		require.Equal(t, servermode.NormalMode, m.Mode()) // should start in normal mode
		require.NoError(t, m.Ack(ctx))

		resp, err := etcdClient.Get(ctx, "test-ack/1")
		require.NoError(t, err)
		require.JSONEq(t, `{"status":"NORMAL"}`, string(resp.Kvs[0].Value))
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
