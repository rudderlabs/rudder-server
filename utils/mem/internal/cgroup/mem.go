package cgroup

import (
	"strconv"
)

// GetMemoryUsage returns cgroup (v1 or v2) memory usage
func GetMemoryUsage(basePath string) int64 {
	n, err := getMemStatCgroup1(basePath, "memory.usage_in_bytes")
	if err == nil {
		wss := getWSSMemoryCgroup1(basePath, n)
		rss := getRSSMemoryCgroup1(basePath)
		if wss > rss {
			return wss
		}
		return rss
	}
	n, err = getMemStatCgroup2(basePath, "memory.current")
	if err != nil {
		return 0
	}
	return getWSSMemoryCgroup2(basePath, n)
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
		n, err := getMemStatCgroup1(basePath, "memory.limit_in_bytes")
		if err == nil {
			if n <= 0 || int64(int(n)) != n || int(n) > totalMem {
				// try to get hierarchical limit
				n = GetHierarchicalMemoryLimitCgroup1(basePath)
			}
			return n
		}

		// cgroups v2
		n, err = getMemStatCgroup2(basePath, "memory.max")
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

func getMemStatCgroup2(basePath, statName string) (int64, error) {
	// See https: //www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html#memory-interface-files
	return getStatGeneric(statName, basePath+"/sys/fs/cgroup", basePath+"/proc/self/cgroup", "")
}

func getMemStatCgroup1(basePath, statName string) (int64, error) {
	return getStatGeneric(statName, basePath+"/sys/fs/cgroup/memory", basePath+"/proc/self/cgroup", "memory")
}

// GetHierarchicalMemoryLimitCgroup1 returns hierarchical memory limit
// https://www.kernel.org/doc/Documentation/cgroup-v1/memory.txt
func GetHierarchicalMemoryLimitCgroup1(basePath string) int64 {
	return memStatCgroup1(basePath, "hierarchical_memory_limit")
}

func getRSSMemoryCgroup1(basePath string) int64 {
	return memStatCgroup1(basePath, "total_rss")
}

func getWSSMemoryCgroup1(basePath string, used int64) int64 {
	inactive := memStatCgroup1(basePath, "total_inactive_file")
	if used < inactive {
		return 0
	}
	return used - inactive
}

func getWSSMemoryCgroup2(basePath string, used int64) int64 {
	inactive := memStatCgroup2(basePath, "inactive_file")
	if used < inactive {
		return 0
	}
	return used - inactive
}

func memStatCgroup1(basePath, key string) int64 {
	data, err := getFileContents("memory.stat", basePath+"/sys/fs/cgroup/memory", basePath+"/proc/self/cgroup", "memory")
	if err != nil {
		return 0
	}
	memStat, err := grepFirstMatch(data, key, 1, " ")
	if err != nil {
		return 0
	}
	n, err := strconv.ParseInt(memStat, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func memStatCgroup2(basePath, key string) int64 {
	data, err := getFileContents("memory.stat", basePath+"/sys/fs/cgroup", basePath+"/proc/self/cgroup", "")
	if err != nil {
		return 0
	}
	memStat, err := grepFirstMatch(data, key, 1, " ")
	if err != nil {
		return 0
	}
	n, err := strconv.ParseInt(memStat, 10, 64)
	if err != nil {
		return 0
	}
	return n
}
