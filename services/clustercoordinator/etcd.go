package clustercoordinator

import (
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	etcdGetTimeout     time.Duration
	etcdConnectTimeout time.Duration
	etcdWatchTimeout   time.Duration
)

func init() {
	config.RegisterDurationConfigVariable(time.Duration(15), &etcdGetTimeout, true, time.Second, "etcd.getTimeout")
	config.RegisterDurationConfigVariable(time.Duration(3), &etcdConnectTimeout, true, time.Second, "etcd.connTimeOut")
	config.RegisterDurationConfigVariable(time.Duration(3), &etcdWatchTimeout, true, time.Second, "etcd.watchTimeout")
}

type ETCDManager struct {
	Config  *ETCDConfig
	session *clientv3.Client
}

func GetETCDConfig() *ETCDConfig {
	etcdHosts := strings.Split(config.GetEnv("ETCD_HOST", "127.0.0.1:2379"), `,`)
	releaseName := config.GetEnv("RELEASE_NAME", `multitenantv1`)
	serverIndex := config.GetInstanceID()

	podStatusLock := sync.RWMutex{}
	podStatusWaitGroup := &sync.WaitGroup{}
	return &ETCDConfig{
		etcdHosts:          etcdHosts,
		releaseName:        releaseName,
		serverIndex:        serverIndex,
		etcdWatchTimeout:   etcdWatchTimeout,
		etcdConnectTimeout: etcdConnectTimeout,
		etcdGetTimeout:     etcdGetTimeout,
		podStatusLock:      podStatusLock,
		podStatusWaitGroup: podStatusWaitGroup,
	}
}

type ETCDConfig struct {
	etcdHosts          []string
	releaseName        string
	serverIndex        string
	etcdGetTimeout     time.Duration
	etcdConnectTimeout time.Duration
	etcdWatchTimeout   time.Duration
	podStatusLock      sync.RWMutex
	podStatusWaitGroup *sync.WaitGroup
}
