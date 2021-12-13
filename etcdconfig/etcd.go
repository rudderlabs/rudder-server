package etcdconfig

import (
	"context"

	"github.com/rudderlabs/rudder-server/config"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	etcdHost         string
	podWorkspacesKey string
)

func Init() {
	loadConfig()
}

func loadConfig() {
	etcdHost = config.GetEnv("ETCD_HOST", "127.0.0.1:2379")
	podWorkspacesKey = config.GetEnv("POD_WORKSPACES_KEY", `/workspaces/RSPodIdentifier`)
}

//returns a channel watching for changes in workspaces that this pod serves
func WatchForWorkspaces(ctx context.Context) chan map[string]string {
	returnChan := make(chan map[string]string)
	go func(returnChan chan map[string]string, ctx context.Context) {
		cli, _ := clientv3.New(clientv3.Config{
			Endpoints: []string{etcdHost},
		})
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
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints: []string{etcdHost},
	})
	defer cli.Close()
	initialWorkspaces, err := cli.Get(ctx, podWorkspacesKey)
	if err != nil {
		panic(err)
	}
	workSpaceString := initialWorkspaces.Kvs[0].Value

	watchChan := WatchForWorkspaces(ctx)

	return string(workSpaceString), watchChan
}
