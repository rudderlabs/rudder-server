package router

import (
	"github.com/rudderlabs/rudder-go-kit/config"
)

func getRouterConfigBool(key, destType string, defaultValue bool) bool {
	destOverrideFound := config.IsSet("Router." + destType + "." + key)
	if destOverrideFound {
		return config.GetBool("Router."+destType+"."+key, defaultValue)
	} else {
		return config.GetBool("Router."+key, defaultValue)
	}
}

func getRouterConfigInt(key, destType string, defaultValue int) int {
	destOverrideFound := config.IsSet("Router." + destType + "." + key)
	if destOverrideFound {
		return config.GetInt("Router."+destType+"."+key, defaultValue)
	} else {
		return config.GetInt("Router."+key, defaultValue)
	}
}
