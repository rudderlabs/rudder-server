package cgroup

import (
	"strconv"
)

// GetMemoryUsage returns cgroup (v1 or v2) memory usage
func GetMemoryUsage(basePath string) int64 {
	n, err := getMemStat(basePath, "memory.usage_in_bytes")
	if err == nil {
		return n
	}
	n, err = getMemStatV2(basePath, "memory.current")
	if err != nil {
		return 0
	}
	return n
}

// GetMemoryLimit returns the cgroup's (v1 or v2) memory limit, or [totalMem] if there is no limit set.
// If using cgroups v1, hierarchical memory limit is also taken into consideration if there is no limit set.
//
// - https://www.kernel.org/doc/Documentation/cgroup-v1/memory.txt
//
// - https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html#memory-interface-files
func GetMemoryLimit(basePath string, totalMem int) int {
	getLimit := func() int64 {
		// cgroups v1
		n, err := getMemStat(basePath, "memory.limit_in_bytes")
		if err == nil {
			if n <= 0 || int64(int(n)) != n || int(n) > totalMem {
				// try to get hierarchical limit
				n = GetHierarchicalMemoryLimit(basePath)
			}
			return n
		}

		// cgroups v2
		n, err = getMemStatV2(basePath, "memory.max")
		if err != nil {
			return 0
		}
		return n
	}
	limit := getLimit()

	// if the number in not within expected boundaries, return totalMem
	if limit <= 0 || int64(int(limit)) != limit || int(limit) > totalMem {
		return totalMem
	}
	return int(limit)
}

func getMemStatV2(basePath, statName string) (int64, error) {
	// See https: //www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html#memory-interface-files
	return getStatGeneric(statName, basePath+"/sys/fs/cgroup", basePath+"/proc/self/cgroup", "")
}

func getMemStat(basePath, statName string) (int64, error) {
	return getStatGeneric(statName, basePath+"/sys/fs/cgroup/memory", basePath+"/proc/self/cgroup", "memory")
}

// GetHierarchicalMemoryLimit returns hierarchical memory limit
// https://www.kernel.org/doc/Documentation/cgroup-v1/memory.txt
func GetHierarchicalMemoryLimit(basePath string) int64 {
	data, err := getFileContents("memory.stat", basePath+"/sys/fs/cgroup/memory", basePath+"/proc/self/cgroup", "memory")
	if err != nil {
		return 0
	}
	memStat, err := grepFirstMatch(data, "hierarchical_memory_limit", 1, " ")
	if err != nil {
		return 0
	}
	n, err := strconv.ParseInt(memStat, 10, 64)
	if err != nil {
		return 0
	}
	return n
}
