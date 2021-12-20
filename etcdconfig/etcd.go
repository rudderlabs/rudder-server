package etcdconfig

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	cli                *clientv3.Client
	etcdHosts          []string
	podWorkspacesKey   string
	migrationStatusKey string
	connectTimeout     time.Duration
	etcdGetTimeout     time.Duration
	etcdWatchTimeout   time.Duration
	pkgLogger          logger.LoggerI
)

func Init() {
	loadConfig()
}

type EtcdService struct {
	cli                    *clientv3.Client
	workSpaceConfigChannel chan map[string]string
}

func loadConfig() {
	etcdHosts = strings.Split(config.GetEnv("ETCD_HOST", "127.0.0.1:2379"), `,`)
	podWorkspacesKey = config.GetEnv("POD_WORKSPACES_KEY", `/workspaces/RSPodIdentifier`)
	migrationStatusKey = config.GetEnv("POD_MIGRATION_STATUS_KEY", `/migration/RSPodIdentifier`)
	config.RegisterDurationConfigVariable(time.Duration(15), &etcdGetTimeout, true, time.Second, "ETCD_GET_TIMEOUT")
	config.RegisterDurationConfigVariable(time.Duration(3), &connectTimeout, true, time.Second, "ETCD_CONN_TIMEOUT")
	config.RegisterDurationConfigVariable(time.Duration(3), &etcdWatchTimeout, true, time.Second, "ETCD_WATCH_TIMEOUT")
	pkgLogger = logger.NewLogger().Child("etcd")
	connectToETCD()
}

func connectToETCD() {}

//returns a channel watching for changes in workspaces that this pod serves
func WatchForWorkspaces(ctx context.Context) chan map[string]string {
	returnChan := make(chan map[string]string)
	go func(returnChan chan map[string]string, ctx context.Context) {
		defer cli.Close()
		etcdWatchChan := cli.Watch(ctx, podWorkspacesKey)
		for watchResp := range etcdWatchChan {
			for _, event := range watchResp.Events {
				switch event.Type {
				case mvccpb.PUT:
					returnChan <- map[string]string{
						"type":       "PUT",
						"workSpaces": string(event.Kv.Value),
					}
				case mvccpb.DELETE:
					returnChan <- map[string]string{
						"type":       "DELETE",
						"workSpaces": "",
					}
					//we can close this channel now..?
				}
			}
		}
	}(returnChan, ctx)
	return returnChan
}

//returns the initial workspaces this pod must serve
//
//along with a watchChan to watch further updates in the workspaces
func GetWorkspaces(ctx context.Context) (string, chan map[string]string) {
	clientReturnChan := make(chan *clientv3.Client)
	errChan := make(chan error)
	go GetEtcdClient(ctx, clientReturnChan, errChan)

	select {
	case cli := <-clientReturnChan:
		initialWorkspaces, err := cli.Get(ctx, podWorkspacesKey)
		if err != nil {
			panic(err)
		}
		workSpaceString := initialWorkspaces.Kvs[0].Value
		pkgLogger.Info(string(workSpaceString))

		watchChan := WatchForWorkspaces(ctx)

		return string(workSpaceString), watchChan
	case <-time.After(connectTimeout):
		panic("Couldn't find etcd Client")
	case err := <-errChan:
		panic(err)
	}
}

func GetEtcdClient(ctx context.Context, clientReturnChan chan *clientv3.Client, errChan chan error) {
	var err error
	cli, err = clientv3.New(clientv3.Config{
		Endpoints:   etcdHosts,
		DialTimeout: connectTimeout,
	})
	if err != nil {
		panic(err)
	}

	statusRes, err := cli.Status(ctx, etcdHosts[0])
	pkgLogger.Info(statusRes)
	if err != nil {
		errChan <- err
		return
	} else if statusRes == nil {
		errChan <- errors.New("statusRes is nil")
		return
	}
	etcdHeartBeat(ctx)
	clientReturnChan <- cli
}

func etcdHeartBeat(ctx context.Context) {
	client := cli
	etcdConnectTimeout := connectTimeout
	rruntime.Go(func() {
		for {
			func(etcdclient *clientv3.Client, etcdConnectTimeout time.Duration) {
				ctxHeartBeat, cancel := context.WithTimeout(context.Background(), etcdConnectTimeout)
				defer cancel()
				heartBeatChan := make(chan bool)

				go func(ctx context.Context, etcdclient *clientv3.Client, heartBeatChan chan bool) {
					heartBeatFunc(ctx, etcdclient, heartBeatChan)
				}(ctxHeartBeat, client, heartBeatChan)

				select {
				case <-ctxHeartBeat.Done():
					panic(ctxHeartBeat.Err())
				case <-heartBeatChan:
					time.Sleep(1 * time.Second)
				}
			}(client, etcdConnectTimeout)
		}
	})
}

func heartBeatFunc(ctxHeartBeat context.Context, client *clientv3.Client, heartBeatChan chan bool) {
	lease, err := client.Lease.Grant(ctxHeartBeat, 2)
	if err != nil {
		panic(err)
	}

	_, err = client.Put(ctxHeartBeat, `instancePrefix`+`serverName`, `serverName`, clientv3.WithLease(lease.ID))
	if err != nil {
		panic(err)
	}
	heartBeatChan <- true
}

func WatchForMigration(ctx context.Context) chan map[string]string {
	migrationStatusChannel := make(chan map[string]string)
	go func(migrationStatusChan chan map[string]string, ctx context.Context) {
		defer cli.Close()
		watchCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		etcdMigrationStatusChannel := cli.Watch(watchCtx, migrationStatusKey)
		for watchResp := range etcdMigrationStatusChannel {
			for _, event := range watchResp.Events {
				switch event.Type {
				case mvccpb.PUT:
					if string(event.Kv.Value) == "migration" {
						migrationStatusChan <- map[string]string{
							"type":      "PUT",
							"processor": "pause",
						}
					} else if string(event.Kv.Value) == "steady" {
						migrationStatusChan <- map[string]string{
							"type":      "PUT",
							"processor": "resume",
						}
					}
				case mvccpb.DELETE:
					//This pod's status has been deleted from etcd store, pod no longer needed..?
					migrationStatusChan <- map[string]string{
						"type":      "DELETE",
						"processor": "STOP",
					}
				}
			}
		}
	}(migrationStatusChannel, ctx)
	return migrationStatusChannel
}
