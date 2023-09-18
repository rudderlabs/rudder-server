package mem

import (
	"fmt"

	gomem "github.com/shirou/gopsutil/v3/mem"

	"github.com/rudderlabs/rudder-go-kit/mem/internal/cgroup"
)

// Stat represents memory statistics (cgroup aware)
type Stat struct {
	// Total memory in bytes
	Total uint64
	// Available memory in bytes
	Available uint64
	// Available memory in percentage
	AvailablePercent float64
	// Used memory in bytes
	Used uint64
	// Used memory in percentage
	UsedPercent float64
}

// Get current memory statistics
func Get() (*Stat, error) {
	return _default.Get()
}

var _default *collector

func init() {
	_default = &collector{}
}

type collector struct {
	basePath string
}

// Get current memory statistics
func (c *collector) Get() (*Stat, error) {
	var stat Stat
	mem, err := gomem.VirtualMemory()
	if err != nil {
		return nil, fmt.Errorf("failed to get memory statistics: %w", err)
	}

	cgroupLimit := cgroup.GetMemoryLimit(c.basePath, int(mem.Total))
	if cgroupLimit < int(mem.Total) { // if cgroup limit is set read memory statistics from cgroup
		stat.Total = uint64(cgroupLimit)
		stat.Used = uint64(cgroup.GetMemoryUsage(c.basePath))
		if stat.Used > stat.Total {
			stat.Used = stat.Total
		}
		stat.Available = stat.Total - stat.Used
	} else {
		stat.Total = mem.Total
		stat.Available = mem.Available
		stat.Used = stat.Total - stat.Available
	}
	stat.AvailablePercent = float64(stat.Available) * 100 / float64(stat.Total)
	stat.UsedPercent = float64(stat.Used) * 100 / float64(stat.Total)
	return &stat, nil
}
