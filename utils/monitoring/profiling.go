package monitoring

import (
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var profiler struct {
	cpuprofile string
	memprofile string
	f          *os.File
}

//StartCPUProfiling starts cpu profiling written to given file path
func StartCPUProfiling(filePath string) {
	var err error
	profiler.cpuprofile = filePath
	profiler.f, err = os.Create(filePath)
	misc.AssertError(err)
	runtime.SetBlockProfileRate(1)
	err = pprof.StartCPUProfile(profiler.f)
	misc.AssertError(err)
	return
}

//StopCPUProfiling stops cpu profiling and closes the file to which profile is written to
func StopCPUProfiling() {
	if profiler.cpuprofile != "" {
		logger.Info("Stopping CPU profile")
		pprof.StopCPUProfile()
		profiler.f.Close()
	}
}

//StartMemoryProfiling registers the file path to which memory profile needs to be collected
//As pprof memory profiling is to be collected at an instant of time, we defer it to stop call
func StartMemoryProfiling(filePath string) {
	profiler.memprofile = filePath
}

//StopMemoryProfiling collects memory profile into file which was registered with this module using StartMemoryProfiling call
func StopMemoryProfiling() {
	if profiler.memprofile != "" {
		f, err := os.Create(profiler.memprofile)
		misc.AssertError(err)
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		err = pprof.WriteHeapProfile(f)
		misc.AssertError(err)
	}
}
