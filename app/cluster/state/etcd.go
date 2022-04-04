package state

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/types/servermode"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

var (
	etcdGetTimeout   time.Duration
	etcdWatchTimeout time.Duration
	keepaliveTime    time.Duration
	keepaliveTimeout time.Duration
	dialTimeout      time.Duration
	envConfigOnce    sync.Once
)

type ETCDConfig struct {
	releaseName          string
	serverIndex          string
	workspaceWatchKey    string
	workspaceFetchKey    string
	etcdHosts            []string
	dialKeepAliveTime    time.Duration
	dialKeepAliveTimeout time.Duration
	etcdWatchTimeout     time.Duration
	etcdGetTimeout       time.Duration
	dialTimeout          time.Duration
}

func EnvETCDConfig() *ETCDConfig {
	etcdHosts := strings.Split(config.GetEnv("ETCD_HOST", "127.0.0.1:2379"), `,`)
	releaseName := config.GetEnv("RELEASE_NAME", `multitenantv1`)
	serverIndex := config.GetInstanceID()
	workspaceWatchKey := config.GetEnv("WORKSPACE_RELOAD_TRIGGER_KEY", "")
	workspaceFetchKey := config.GetEnv("WORKSPACE_FETCH_KEY", "")

	envConfigOnce.Do(func() {
		config.RegisterDurationConfigVariable(time.Duration(15), &etcdGetTimeout, true, time.Second, "etcd.getTimeout")
		config.RegisterDurationConfigVariable(time.Duration(3), &etcdWatchTimeout, true, time.Second, "etcd.watchTimeout")
		config.RegisterDurationConfigVariable(time.Duration(30), &keepaliveTime, true, time.Second, "etcd.keepaliveTime")
		config.RegisterDurationConfigVariable(time.Duration(10), &keepaliveTimeout, true, time.Second, "etcd.keepaliveTimeout")
		config.RegisterDurationConfigVariable(time.Duration(20), &dialTimeout, true, time.Second, "etcd.dialTimeout")
	})

	return &ETCDConfig{
		etcdHosts:            etcdHosts,
		releaseName:          releaseName,
		serverIndex:          serverIndex,
		etcdWatchTimeout:     etcdWatchTimeout,
		dialTimeout:          dialTimeout,
		dialKeepAliveTime:    keepaliveTime,
		dialKeepAliveTimeout: keepaliveTimeout,
		etcdGetTimeout:       etcdGetTimeout,
		workspaceWatchKey:    workspaceWatchKey,
		workspaceFetchKey:    workspaceFetchKey,
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
			Endpoints:            manager.Config.etcdHosts,
			DialTimeout:          manager.Config.dialTimeout,
			DialKeepAliveTime:    keepaliveTime,
			DialKeepAliveTimeout: keepaliveTimeout,
			DialOptions: []grpc.DialOption{
				grpc.WithBlock(), // block until the underlying connection is up
			},
		})
		if err != nil {
			manager.Client = cli
		}
		manager.initErr = err
	})

	return manager.initErr
}

func (manager *ETCDManager) Put(ctx context.Context, key string, value string) error {
	if err := manager.init(); err != nil {
		return err
	}

	_, err := manager.Client.Put(ctx, key, value)
	return err
}

func (manager *ETCDManager) Watch(ctx context.Context, key string) (chan interface{}, error) {
	if err := manager.init(); err != nil {
		return nil, err
	}
	resultChan := make(chan interface{}, 1)
	watchChan := manager.Client.Watch(ctx, key)
	resultChan <- watchChan

	return resultChan, nil
}

func (manager *ETCDManager) Get(ctx context.Context, key string) (string, error) {
	if err := manager.init(); err != nil {
		return "", err
	}

	var result string
	val, err := manager.Client.Get(ctx, key)
	if err != nil {
		return "", err
	}
	if len(val.Kvs) > 0 {
		result = string(val.Kvs[0].Value)
	} else {
		result = ``
	}
	return result, nil
}

func (manager *ETCDManager) WatchForWorkspaces(ctx context.Context, key string) (chan string, error) {
	if err := manager.init(); err != nil {
		return nil, err
	}

	resultChan := make(chan string, 1)
	go func(returnChan chan string, ctx context.Context, key string) {
		etcdWatchChan := manager.Client.Watch(ctx, key)
		for watchResp := range etcdWatchChan {
			for _, event := range watchResp.Events {
				switch event.Type {
				case mvccpb.PUT:
					returnChan <- string(event.Kv.Value)
				}
			}
		}
		close(resultChan)
	}(resultChan, ctx, key)

	return resultChan, nil
}

func (manager *ETCDManager) WorkspaceServed() (<-chan servermode.Ack, error) {
	if err := manager.init(); err != nil {
		return nil, err
	}

	resultChan := make(chan servermode.Ack, 1)
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	go func(returnChan chan servermode.Ack) {
		etcdWatchChan := manager.Client.Watch(ctx, manager.Config.workspaceWatchKey)
		for watchResp := range etcdWatchChan {
			for _, event := range watchResp.Events {
				switch event.Type {
				case mvccpb.PUT:
					returnChan <- servermode.WithACK("", string(event.Kv.Value), func() {})
				}
			}
		}
		close(resultChan)
	}(resultChan)

	return resultChan, nil
}

func (manager *ETCDManager) Close() {
	if manager.Client != nil {
		manager.Client.Close()
	}
	// TODO close channels
}
