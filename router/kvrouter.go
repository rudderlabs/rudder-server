package router

import (
	"encoding/json"
	"reflect"
	"sync"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/kvstoremanager"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/tidwall/gjson"
)

var (
	kvConfigSubscriberLock sync.RWMutex
	kvManagerLockMap       map[string]*sync.RWMutex
	kvManagerMap           map[string]*KVDestination
)

type kvHandleT struct {
	destType string
}

type KVDestination struct {
	config  types.ConfigT
	manager kvstoremanager.KVStoreManager
}

func init() {
	kvManagerLockMap = make(map[string]*sync.RWMutex)
	kvManagerMap = make(map[string]*KVDestination)
}

func (h *kvHandleT) Setup(destType string) {
	h.destType = destType
	rruntime.Go(func() {
		h.backendConfigSubscriber()
	})
}

func (h *kvHandleT) send(payload json.RawMessage, destID string) (statusCode int, respBody string) {
	key := gjson.GetBytes(payload, "message.key").String()
	result := gjson.GetBytes(payload, "message.fields").Map()
	fields := make(map[string]interface{})
	for k, v := range result {
		fields[k] = v.Str
	}
	kvManagerLockMap[destID].RLock()
	defer kvManagerLockMap[destID].RUnlock()
	_, err := kvManagerMap[destID].manager.HMSet(key, fields)
	statusCode = 200
	// TODO: How to map response to status code?
	if err != nil {
		statusCode = 400
		respBody = err.Error()
	}
	return
}

func (h *kvHandleT) backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, "backendConfig")
	for {
		config := <-ch
		allSources := config.Data.(backendconfig.SourcesT)
		for _, source := range allSources.Sources {
			if len(source.Destinations) > 0 {
				for _, destination := range source.Destinations {
					if destination.DestinationDefinition.Name == h.destType && misc.ContainsString(kvstoremanager.KVStoreDestinations, h.destType) {
						l, ok := kvManagerLockMap[destination.ID]
						if !ok {
							l = &sync.RWMutex{}
							kvManagerLockMap[destination.ID] = l
						}
						l.Lock()
						if d, ok := kvManagerMap[destination.ID]; ok {
							if !reflect.DeepEqual(d.config, destination.Config) {
								d.manager.Close()
								m := kvstoremanager.New(kvstoremanager.SettingsT{
									Provider: h.destType,
									Config:   destination.Config,
								})
								kvManagerMap[destination.ID] = &KVDestination{config: destination.Config, manager: m}
							}
						} else {
							m := kvstoremanager.New(kvstoremanager.SettingsT{
								Provider: h.destType,
								Config:   destination.Config,
							})
							kvManagerMap[destination.ID] = &KVDestination{config: destination.Config, manager: m}
						}
						l.Unlock()
					}
				}
			}
		}
	}
}
