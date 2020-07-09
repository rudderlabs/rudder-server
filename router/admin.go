package router

import "github.com/rudderlabs/rudder-server/admin"

type RouterAdmin struct {
	handles map[string]*HandleT
}

var adminInstance *RouterAdmin

func init() {
	adminInstance = &RouterAdmin{
		handles: make(map[string]*HandleT),
	}
	admin.RegisterStatusHandler("routers", adminInstance)
}

func (ra *RouterAdmin) registerRouter(name string, handle *HandleT) {
	ra.handles[name] = handle
}

// Status function is used for debug purposes by the admin interface
func (ra *RouterAdmin) Status() interface{} {
	statusList := make([]map[string]interface{}, 0)
	for name, router := range ra.handles {
		routerStatus := router.perfStats.Status()
		routerStatus["name"] = name
		routerStatus["success-count"] = router.successCount
		routerStatus["failure-count"] = router.failCount
		statusList = append(statusList, routerStatus)
	}
	return statusList
}
