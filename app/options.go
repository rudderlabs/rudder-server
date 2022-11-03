package app

import (
	"flag"
	"os"
)

// Options contains application's initialisation options
type Options struct {
	NormalMode      bool
	DegradedMode    bool
	ClearDB         bool
	Cpuprofile      string
	Memprofile      string
	VersionFlag     bool
	EnterpriseToken string
}

// LoadOptions loads application's initialisation options based on command line flags and environment
func LoadOptions(args []string) *Options {
	flagSet := flag.NewFlagSet(args[0], flag.ExitOnError)
	// Parse command line options
	normalMode := flagSet.Bool("normal-mode", false, "a bool")
	degradedMode := flagSet.Bool("degraded-mode", false, "a bool")
	clearDB := flagSet.Bool("cleardb", false, "a bool")
	cpuprofile := flagSet.String("cpuprofile", "", "write cpu profile to `file`")
	memprofile := flagSet.String("memprofile", "", "write memory profile to `file`")
	versionFlag := flagSet.Bool("v", false, "Print the current version and exit")

	serverMode := os.Getenv("RSERVER_MODE")
	if serverMode == "normal" {
		*normalMode = true
	} else if serverMode == "degraded" {
		*degradedMode = true
	}

	// Ignore errors; flagSet is set for ExitOnError.
	_ = flagSet.Parse(args[1:])

	return &Options{
		NormalMode:   *normalMode,
		DegradedMode: *degradedMode,
		ClearDB:      *clearDB,
		Cpuprofile:   *cpuprofile,
		Memprofile:   *memprofile,
		VersionFlag:  *versionFlag,
	}
}
