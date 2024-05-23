package state

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/app/cluster"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types/servermode"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var (
	keepaliveTime    time.Duration
	keepaliveTimeout time.Duration
	dialTimeout      time.Duration
	envConfigOnce    sync.Once
)

const (
	modeRequestKeyPattern = `/%s/SERVER/%s/MODE` // /<releaseName>/server/<serverIndex>/mode

	defaultACKTimeout = 15 * time.Second
)

var _ cluster.ChangeEventProvider = &ETCDManager{}

type ETCDConfig struct {
	ReleaseName          string
	ServerIndex          string
	Endpoints            []string
	dialKeepAliveTime    time.Duration
	dialKeepAliveTimeout time.Duration
	ACKTimeout           time.Duration
	dialTimeout          time.Duration
}

type modeRequestValue struct {
	Mode   servermode.Mode `json:"mode"`
	AckKey string          `json:"ack_key"`
}

type modeAckValue struct {
	Status servermode.Mode `json:"status"`
}

func EnvETCDConfig() *ETCDConfig {
	endpoints := strings.Split(config.GetString("ETCD_HOSTS", "127.0.0.1:2379"), `,`)
	releaseName := config.GetReleaseName()
	serverIndex := misc.GetInstanceID()
	var ackTimeout time.Duration

	envConfigOnce.Do(func() {
		ackTimeout = config.GetDurationVar(15, time.Second, "etcd.ackTimeout")
		keepaliveTime = config.GetDurationVar(30, time.Second, "etcd.keepaliveTime")
		keepaliveTimeout = config.GetDurationVar(10, time.Second, "etcd.keepaliveTimeout")
		dialTimeout = config.GetDurationVar(20, time.Second, "etcd.dialTimeout")
	})

	return &ETCDConfig{
		Endpoints:            endpoints,
		ReleaseName:          releaseName,
		ServerIndex:          serverIndex,
		ACKTimeout:           ackTimeout,
		dialTimeout:          dialTimeout,
		dialKeepAliveTime:    keepaliveTime,
		dialKeepAliveTimeout: keepaliveTimeout,
	}
}

type ETCDManager struct {
	Config     *ETCDConfig
	Client     *clientv3.Client
	once       sync.Once
	initErr    error
	logger     logger.Logger
	ackTimeout time.Duration
}

func (manager *ETCDManager) init() error {
	manager.once.Do(func() {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:            manager.Config.Endpoints,
			DialTimeout:          manager.Config.dialTimeout,
			DialKeepAliveTime:    manager.Config.dialKeepAliveTime,
			DialKeepAliveTimeout: manager.Config.dialKeepAliveTimeout,
		})
		if err != nil {
			endpoints := strings.Join(manager.Config.Endpoints, `,`)
			manager.initErr = fmt.Errorf("etcd client connect (%q): %w", endpoints, err)
			return
		}
		manager.Client = cli
		if manager.logger == nil {
			manager.logger = logger.NewLogger().Child("etcd")
		}

		manager.ackTimeout = manager.Config.ACKTimeout
		if manager.ackTimeout == 0 {
			manager.ackTimeout = defaultACKTimeout
		}
	})

	return manager.initErr
}

// Ping ensures the connection to etcd is alive
func (manager *ETCDManager) Ping() error {
	if err := manager.init(); err != nil {
		return err
	}

	_, err := manager.Client.Cluster.MemberList(context.Background())
	if err != nil {
		return fmt.Errorf("ping: get cluster member list: %w", err)
	}

	return nil
}

func (manager *ETCDManager) unmarshalMode(raw []byte) servermode.ChangeEvent {
	var req modeRequestValue
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return servermode.ChangeEventError(fmt.Errorf("unmarshal mode request: %w", err))
	}

	mode := req.Mode
	if !mode.Valid() {
		return servermode.ChangeEventError(fmt.Errorf("invalid mode: %s", mode))
	}

	return servermode.NewChangeEvent(
		mode,
		func(ctx context.Context) error {
			ctx, cancel := context.WithTimeout(ctx, manager.ackTimeout)
			defer cancel()

			ackValue, err := json.MarshalToString(modeAckValue{
				Status: mode,
			})
			if err != nil {
				return fmt.Errorf("marshal ack value: %w", err)
			}
			manager.logger.Infof("Mode Change Acknowledgement Key: %s", req.AckKey)
			_, err = manager.Client.Put(ctx, req.AckKey, ackValue)
			if err != nil {
				manager.logger.Errorf("Failed to acknowledge mode change for key: %s", req.AckKey)
				return fmt.Errorf("put value to ack key %q: %w", req.AckKey, err)
			} else {
				manager.logger.Debugf("Mode change for key %q acknowledged", req.AckKey)
			}
			return err
		})
}

func errChModeRequest(err error) <-chan servermode.ChangeEvent {
	ch := make(chan servermode.ChangeEvent, 1)
	ch <- servermode.ChangeEventError(err)
	close(ch)
	return ch
}

func (manager *ETCDManager) ServerMode(ctx context.Context) <-chan servermode.ChangeEvent {
	if err := manager.init(); err != nil {
		return errChModeRequest(err)
	}

	modeRequestKey := fmt.Sprintf(modeRequestKeyPattern, manager.Config.ReleaseName, manager.Config.ServerIndex)
	manager.logger.Infof("Mode Lookup Key: %s", modeRequestKey)
	revision := int64(0)

	resultChan := make(chan servermode.ChangeEvent, 1)
	resp, err := manager.Client.Get(ctx, modeRequestKey)
	if err != nil {
		return errChModeRequest(err)
	}

	if len(resp.Kvs) != 0 {
		resultChan <- manager.unmarshalMode(resp.Kvs[0].Value)
		revision = resp.Header.Revision + 1
	}

	etcdWatchChan := manager.Client.Watch(ctx, modeRequestKey, clientv3.WithRev(revision))
	go func() {
		for watchResp := range etcdWatchChan {
			if watchResp.Err() != nil {
				resultChan <- servermode.ChangeEventError(watchResp.Err())
				continue
			}

			for _, event := range watchResp.Events {
				switch event.Type {
				case mvccpb.PUT:
					select {
					case resultChan <- manager.unmarshalMode(event.Kv.Value):
					case <-ctx.Done():
					}
				default:
					manager.logger.Warnf("unknown event type %s", event.Type)
				}
			}
		}
		close(resultChan)
	}()

	return resultChan
}

func (manager *ETCDManager) Close() {
	if manager.Client != nil {
		_ = manager.Client.Close()
	}
}

func NewETCDDynamicProvider() *ETCDManager {
	return &ETCDManager{
		Config: EnvETCDConfig(),
	}
}
