package state

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/rudderlabs/rudder-server/app/cluster"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/types/servermode"
	"github.com/rudderlabs/rudder-server/utils/types/workspace"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

var (
	json = jsoniter.ConfigCompatibleWithStandardLibrary
)

var (
	etcdGetTimeout   time.Duration
	etcdWatchTimeout time.Duration
	keepaliveTime    time.Duration
	keepaliveTimeout time.Duration
	dialTimeout      time.Duration
	envConfigOnce    sync.Once
)

const (
	modeRequestKeyPattern        = `/%s/server/%s/mode`       // /<releaseName>/server/<serverIndex>/mode
	workspacesRequestsKeyPattern = `/%s/server/%s/workspaces` // /<releaseName>/server/<serverIndex>/workspaces
)

var _ cluster.ModeProvider = &ETCDManager{}

type ETCDConfig struct {
	ReleaseName          string
	ServerIndex          string
	Endpoints            []string
	dialKeepAliveTime    time.Duration
	dialKeepAliveTimeout time.Duration
	etcdWatchTimeout     time.Duration
	etcdGetTimeout       time.Duration
	dialTimeout          time.Duration
}

type modeRequestValue struct {
	Mode   servermode.Mode `json:"mode"`
	AckKey string          `json:"ack_key"`
}

type modeAckValue struct {
	Status servermode.Mode `json:"status"`
}

type workspacesRequestsValue struct {
	Workspaces string `json:"workspaces"` // comma separated workspaces
	AckKey     string `json:"ack_key"`
}

type workspacesAckValue struct {
	Status string `json:"status"`
}

func EnvETCDConfig() *ETCDConfig {
	endpoints := strings.Split(config.GetEnv("ETCD_HOSTS", "127.0.0.1:2379"), `,`)
	releaseName := config.GetEnv("RELEASE_NAME", `multitenantv1`)
	serverIndex := config.GetInstanceID()

	// TODO: do we need these:
	// workspaceWatchKey := config.GetEnv("WORKSPACE_RELOAD_TRIGGER_KEY", "")
	// workspaceFetchKey := config.GetEnv("WORKSPACE_FETCH_KEY", "")

	envConfigOnce.Do(func() {
		config.RegisterDurationConfigVariable(time.Duration(15), &etcdGetTimeout, true, time.Second, "etcd.getTimeout")
		config.RegisterDurationConfigVariable(time.Duration(3), &etcdWatchTimeout, true, time.Second, "etcd.watchTimeout")
		config.RegisterDurationConfigVariable(time.Duration(30), &keepaliveTime, true, time.Second, "etcd.keepaliveTime")
		config.RegisterDurationConfigVariable(time.Duration(10), &keepaliveTimeout, true, time.Second, "etcd.keepaliveTimeout")
		config.RegisterDurationConfigVariable(time.Duration(20), &dialTimeout, true, time.Second, "etcd.dialTimeout")
	})

	return &ETCDConfig{
		Endpoints:            endpoints,
		ReleaseName:          releaseName,
		ServerIndex:          serverIndex,
		etcdWatchTimeout:     etcdWatchTimeout,
		dialTimeout:          dialTimeout,
		dialKeepAliveTime:    keepaliveTime,
		dialKeepAliveTimeout: keepaliveTimeout,
		etcdGetTimeout:       etcdGetTimeout,
	}
}

type ETCDManager struct {
	Config  *ETCDConfig
	Client  *clientv3.Client
	once    sync.Once
	initErr error
}

func (manager *ETCDManager) init() error {
	manager.once.Do(func() {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:            manager.Config.Endpoints,
			DialTimeout:          manager.Config.dialTimeout,
			DialKeepAliveTime:    manager.Config.dialKeepAliveTime,
			DialKeepAliveTimeout: manager.Config.dialKeepAliveTimeout,
			DialOptions: []grpc.DialOption{
				grpc.WithBlock(), // block until the underlying connection is up
			},
		})
		if err != nil {
			manager.initErr = err
			return
		}
		manager.Client = cli
	})

	return manager.initErr
}

func (manager *ETCDManager) Ping() error {
	if err := manager.init(); err != nil {
		return err
	}
	return nil
}

func (manager *ETCDManager) prepareMode(raw []byte) servermode.ModeRequest {
	var req modeRequestValue
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return servermode.ModeError(fmt.Errorf("unmarshal mode request: %w", err))
	}

	mode := servermode.Mode(req.Mode)
	if !mode.Valid() {
		return servermode.ModeError(fmt.Errorf("invalid mode: %s", mode))
	}

	return servermode.NewModeRequest(
		mode,
		func() error {
			ackValue, err := json.MarshalToString(modeAckValue{
				Status: mode,
			})
			if err != nil {
				return err
			}
			_, err = manager.Client.Put(context.Background(), req.AckKey, ackValue)
			return err
		})
}

func errChModeRequest(err error) <-chan servermode.ModeRequest {
	ch := make(chan servermode.ModeRequest, 1)
	ch <- servermode.ModeError(err)
	close(ch)
	return ch
}

func (manager *ETCDManager) ServerMode(ctx context.Context) <-chan servermode.ModeRequest {
	if err := manager.init(); err != nil {
		return errChModeRequest(err)
	}

	modeRequestKey := fmt.Sprintf(modeRequestKeyPattern, manager.Config.ReleaseName, manager.Config.ServerIndex)

	resultChan := make(chan servermode.ModeRequest, 1)
	resp, err := manager.Client.Get(ctx, modeRequestKey)
	if err != nil {
		return errChModeRequest(err)
	}

	if len(resp.Kvs) == 0 {
		return errChModeRequest(errors.New("no workspace found"))
	}

	resultChan <- manager.prepareMode(resp.Kvs[0].Value)

	etcdWatchChan := manager.Client.Watch(ctx, modeRequestKey, clientv3.WithRev(resp.Header.Revision+1))
	go func() {
		for watchResp := range etcdWatchChan {
			if watchResp.Err() != nil {
				resultChan <- servermode.ModeError(watchResp.Err())
				continue
			}

			for _, event := range watchResp.Events {
				switch event.Type {
				case mvccpb.PUT:
					resultChan <- manager.prepareMode(event.Kv.Value)
				default:
					resultChan <- servermode.ModeError(fmt.Errorf("unknown event type %q", event.Type))
				}
			}
		}
		close(resultChan)
	}()

	return resultChan
}

func errChWorkspacesRequest(err error) <-chan workspace.WorkspacesRequest {
	ch := make(chan workspace.WorkspacesRequest, 1)
	ch <- workspace.WorkspacesError(err)
	close(ch)
	return ch
}

func (manager *ETCDManager) prepareWorkspace(raw []byte) workspace.WorkspacesRequest {
	var req workspacesRequestsValue
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return workspace.WorkspacesError(err)
	}

	return workspace.NewWorkspacesRequest(
		strings.Split(req.Workspaces, ","),
		func() error {
			ackValue, err := json.MarshalToString(workspacesAckValue{
				Status: "RELOADED",
			})
			if err != nil {
				return err
			}
			_, err = manager.Client.Put(context.Background(), req.AckKey, ackValue)
			return err
		})
}

func (manager *ETCDManager) WorkspaceIDs(ctx context.Context) <-chan workspace.WorkspacesRequest {
	if err := manager.init(); err != nil {
		return errChWorkspacesRequest(err)
	}

	modeRequestKey := fmt.Sprintf(workspacesRequestsKeyPattern, manager.Config.ReleaseName, manager.Config.ServerIndex)

	resultChan := make(chan workspace.WorkspacesRequest, 1)
	resp, err := manager.Client.Get(ctx, modeRequestKey)
	if err != nil {
		return errChWorkspacesRequest(err)
	}

	if len(resp.Kvs) == 0 {
		return errChWorkspacesRequest(errors.New("no workspace found"))
	}

	resultChan <- manager.prepareWorkspace(resp.Kvs[0].Value)
	etcdWatchChan := manager.Client.Watch(ctx, modeRequestKey, clientv3.WithRev(resp.Header.Revision+1))

	go func() {
		for watchResp := range etcdWatchChan {
			if watchResp.Err() != nil {
				resultChan <- workspace.WorkspacesError(watchResp.Err())
				continue
			}

			for _, event := range watchResp.Events {
				switch event.Type {
				case mvccpb.PUT:
					resultChan <- manager.prepareWorkspace(event.Kv.Value)
				default:
					resultChan <- workspace.WorkspacesError(fmt.Errorf("unknown event type %q", event.Type))
				}
			}
		}
		close(resultChan)
	}()

	return resultChan
}

func (manager *ETCDManager) Close() {
	if manager.Client != nil {
		manager.Client.Close()
	}
}
