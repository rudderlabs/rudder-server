package version

import (
	"encoding/json"
	"fmt"
)

var version = "Not an official release. Get the latest release from the github repo."
var major, minor, commit, buildDate, builtBy, gitURL, patch string

// Version contains aggregated versioning information
type Version struct {
	Version   string
	Major     string
	Minor     string
	Patch     string
	Commit    string
	BuildDate string
	BuiltBy   string
	GitURL    string
}

// Current returns the application's version, as set by build time flags
// TODO: fill in details about ldflags
func Current() Version {
	return Version{
		Version:   version,
		Major:     major,
		Minor:     minor,
		Patch:     patch,
		Commit:    commit,
		BuildDate: buildDate,
		BuiltBy:   builtBy,
		GitURL:    gitURL,
	} // return map[string]interface{}{"Version": version, "Major": major, "Minor": minor, "Patch": patch, "Commit": commit, "BuildDate": buildDate, "BuiltBy": builtBy, "GitUrl": gitURL}
}

// PrintVersion prints the application's version on stdout.
func PrintVersion() {
	version := Current()
	versionFormatted, _ := json.MarshalIndent(&version, "", " ")
	fmt.Printf("Version Info %s\n", versionFormatted)
}
