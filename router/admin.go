package router

import (
	"encoding/json"
	"fmt"
	"github.com/rudderlabs/rudder-server/admin"
)

type RouterAdmin struct {
	handles map[string]*HandleT
}

var adminInstance *RouterAdmin

func init() {
	adminInstance = &RouterAdmin{
		handles: make(map[string]*HandleT),
	}
	admin.RegisterStatusHandler("routers", adminInstance)
	admin.RegisterAdminHandler("Router", &RouterRpcHandler{})
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

type RouterRpcHandler struct {
}

func (r *RouterRpcHandler) SetDrainJobsConfig(dHandle DrainConfig, reply *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("Internal Rudder Server Error. Error: %w", r)
		}
	}()

	_, err = SetDrainJobIDs(dHandle.MinDrainJobID, dHandle.MaxDrainJobID, dHandle.DrainDestinationID)
	if err == nil {
		*reply = fmt.Sprintf("Drain config updated")
	}
	return err
}

func (r *RouterRpcHandler) GetDrainJobsConfig(noArgs struct{}, reply *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("Internal Rudder Server Error. Error: %w", r)
		}
	}()
	drainHandler := GetDrainJobHandler()
	formattedOutput, err := json.MarshalIndent(drainHandler, "", "  ")
	*reply = string(formattedOutput)
	return err
}
