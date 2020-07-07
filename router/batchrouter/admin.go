package batchrouter

import "github.com/rudderlabs/rudder-server/admin"

type BatchRouterAdmin struct {
	handles map[string]*HandleT
}

var adminInstance *BatchRouterAdmin

func init() {
	adminInstance = &BatchRouterAdmin{
		handles: make(map[string]*HandleT),
	}
	admin.RegisterStatusHandler("batchrouters", adminInstance)
}

func (ba *BatchRouterAdmin) registerBatchRouter(name string, handle *HandleT) {
	ba.handles[name] = handle
}

// Status function is used for debug purposes by the admin interface
func (ba *BatchRouterAdmin) Status() interface{} {
	statusList := make([]map[string]interface{}, 0)
	for name := range ba.handles {
		batchrouterStatus := make(map[string]interface{})
		batchrouterStatus["name"] = name
		statusList = append(statusList, batchrouterStatus)
	}
	return statusList
}
