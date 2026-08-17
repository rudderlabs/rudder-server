package router

import (
	"github.com/rudderlabs/rudder-go-kit/config"
)

func getRouterConfigBool(key, destType string, defaultValue bool) bool {
	return config.GetBoolVar(defaultValue, getRouterConfigKeys(key, destType)...)
}

func getRouterConfigInt(key, destType string, defaultValue int) int {
	return config.GetIntVar(defaultValue, 1, getRouterConfigKeys(key, destType)...)
}

func getHierarchicalRouterConfigInt(destType string, defaultValue int, keys ...string) int {
	orderedKeys := make([]string, 0, len(keys)*2)
	for i := range keys {
		orderedKeys = append(orderedKeys, "Router."+destType+"."+keys[i])
		orderedKeys = append(orderedKeys, "Router."+keys[i])
	}
	return config.GetIntVar(defaultValue, 1, orderedKeys...)
}

// getPartitionRouterConfigInt returns the value of a partition-scoped router configuration key, in order
// of precedence: Router.<destType>.<partition>.<key>, Router.<destType>.<key>, Router.<key>
func getPartitionRouterConfigInt(key, destType, partition string, defaultValue int) int {
	return config.GetIntVar(defaultValue, 1, getPartitionRouterConfigKeys(key, destType, partition)...)
}

func getReloadableRouterConfigInt(key, destType string, defaultValue int) config.ValueLoader[int] {
	return config.GetReloadableIntVar(defaultValue, 1, getRouterConfigKeys(key, destType)...)
}

func getRouterConfigKeys(key, destType string) []string {
	return []string{"Router." + destType + "." + key, "Router." + key}
}

// getPartitionRouterConfigKeys returns the configuration keys for a partition-scoped router setting, in order
// of precedence: Router.<destType>.<partition>.<key>, Router.<destType>.<key>, Router.<key>
func getPartitionRouterConfigKeys(key, destType, partition string) []string {
	if partition == "" { // no isolation, thus no partition-scoped configuration
		return getRouterConfigKeys(key, destType)
	}
	return append([]string{"Router." + destType + "." + partition + "." + key}, getRouterConfigKeys(key, destType)...)
}

func getHierarchicalRouterConfigKeys(destType string, keys ...string) []string {
	orderedKeys := make([]string, 0, len(keys)*2)
	for i := range keys {
		orderedKeys = append(orderedKeys, "Router."+destType+"."+keys[i])
	}
	for i := range keys {
		orderedKeys = append(orderedKeys, "Router."+keys[i])
	}
	return orderedKeys
}
