package router

import (
	"github.com/rudderlabs/rudder-go-kit/config"
)

func getRouterConfigBool(key, destType string, defaultValue bool) bool {
	return config.GetBoolVar(defaultValue, "Router."+destType+"."+key, "Router."+key)
}

func getRouterConfigInt(key, destType string, defaultValue int) int {
	return config.GetIntVar(defaultValue, 1, "Router."+destType+"."+key, "Router."+key)
}
