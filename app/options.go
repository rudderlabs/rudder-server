package app

import (
	"flag"
	"os"

	"github.com/rudderlabs/rudder-server/config"
)

// Options contains application's initialisation options
type Options struct {
	NormalMode      bool
	DegradedMode    bool
	MaintenanceMode bool
	MigrationMode   string
	ClearDB         bool
	Cpuprofile      string
	Memprofile      string
	VersionFlag     bool
}

// LoadOptions loads application's initialisation options based on command line flags and environment
func LoadOptions() *Options {
	// Parse command line options
	normalMode := flag.Bool("normal-mode", false, "a bool")
	degradedMode := flag.Bool("degraded-mode", false, "a bool")
	maintenanceMode := flag.Bool("maintenance-mode", false, "a bool")
	migrationModeFlag := flag.String("migration-mode", "", "mode of migration. import/export/import-export")
	clearDB := flag.Bool("cleardb", false, "a bool")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to `file`")
	memprofile := flag.String("memprofile", "", "write memory profile to `file`")
	versionFlag := flag.Bool("v", false, "Print the current version and exit")

	serverMode := os.Getenv("RSERVER_MODE")
	if serverMode == "normal" {
		*normalMode = true
	} else if serverMode == "degraded" {
		*degradedMode = true
	} else if serverMode == "maintenance" {
		*maintenanceMode = true
	}

	flag.Parse()

	return &Options{
		NormalMode:      *normalMode,
		DegradedMode:    *degradedMode,
		MaintenanceMode: *maintenanceMode,
		MigrationMode:   getMigrationMode(*migrationModeFlag),
		ClearDB:         *clearDB,
		Cpuprofile:      *cpuprofile,
		Memprofile:      *memprofile,
		VersionFlag:     *versionFlag,
	}
}

func getMigrationMode(flag string) string {
	if flag != "" {
		return flag
	}

	return config.GetEnv("MIGRATION_MODE", "")
}
