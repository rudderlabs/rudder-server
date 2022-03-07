package clustercoordinator

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

var (
	etcdGetTimeout   time.Duration
	etcdWatchTimeout time.Duration
	keepaliveTime    time.Duration
	keepaliveTimeout time.Duration
	dialTimeout      time.Duration
)

func init() {
	config.RegisterDurationConfigVariable(time.Duration(15), &etcdGetTimeout, true, time.Second, "etcd.getTimeout")
	config.RegisterDurationConfigVariable(time.Duration(3), &etcdWatchTimeout, true, time.Second, "etcd.watchTimeout")
	config.RegisterDurationConfigVariable(time.Duration(30), &keepaliveTime, true, time.Second, "etcd.keepaliveTime")
	config.RegisterDurationConfigVariable(time.Duration(10), &keepaliveTimeout, true, time.Second, "etcd.keepaliveTimeout")
	config.RegisterDurationConfigVariable(time.Duration(20), &dialTimeout, true, time.Second, "etcd.dialTimeout")
}

type ETCDManager struct {
	Config *ETCDConfig
	Client *clientv3.Client
}

func (manager *ETCDManager) getClient() (*clientv3.Client, error) {
	if manager.Client != nil {
		return manager.Client, nil
	}

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

	return cli, err
}

func (manager *ETCDManager) Put(ctx context.Context, key string, value string) error {
	_, err := manager.Client.Put(ctx, key, value)
	return err
}

func (manager *ETCDManager) Watch(ctx context.Context, key string) chan interface{} {
	resultChan := make(chan interface{}, 1)
	watchChan := manager.Client.Watch(ctx, key)
	resultChan <- watchChan
	return resultChan
}

func (manager *ETCDManager) Get(ctx context.Context, key string) (string, error) {
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

func GetETCDConfig() *ETCDConfig {
	etcdHosts := strings.Split(config.GetEnv("ETCD_HOST", "127.0.0.1:2379"), `,`)
	releaseName := config.GetEnv("RELEASE_NAME", `multitenantv1`)
	serverIndex := config.GetInstanceID()

	podStatusLock := sync.RWMutex{}
	podStatusWaitGroup := &sync.WaitGroup{}
	return &ETCDConfig{
		etcdHosts:            etcdHosts,
		releaseName:          releaseName,
		serverIndex:          serverIndex,
		etcdWatchTimeout:     etcdWatchTimeout,
		dialTimeout:          dialTimeout,
		dialKeepAliveTime:    keepaliveTime,
		dialKeepAliveTimeout: keepaliveTimeout,
		etcdGetTimeout:       etcdGetTimeout,
		podStatusLock:        podStatusLock,
		podStatusWaitGroup:   podStatusWaitGroup,
	}
}

type ETCDConfig struct {
	etcdHosts            []string
	releaseName          string
	serverIndex          string
	etcdGetTimeout       time.Duration
	dialTimeout          time.Duration
	dialKeepAliveTime    time.Duration
	dialKeepAliveTimeout time.Duration
	etcdWatchTimeout     time.Duration
	podStatusLock        sync.RWMutex
	podStatusWaitGroup   *sync.WaitGroup
}
