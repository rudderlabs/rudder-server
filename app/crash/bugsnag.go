package crash

import (
	"context"
	"runtime"

	"github.com/bugsnag/bugsnag-go"
	"github.com/rudderlabs/rudder-server/app/version"
	"github.com/rudderlabs/rudder-server/config"
)

var ctx context.Context

// SetupBugsnag will initialize bugsnag integration
func SetupBugsnag() {
	bugsnag.Configure(bugsnag.Configuration{
		APIKey:       config.GetEnv("BUGSNAG_KEY", ""),
		ReleaseStage: config.GetEnv("GO_ENV", "development"),
		// The import paths for the Go packages containing your source files
		ProjectPackages: []string{"main", "github.com/rudderlabs/rudder-server"},
		// more configuration options
		AppType:      "rudder-server",
		AppVersion:   version.Current().Version,
		PanicHandler: func() {},
	})
}

// StartBugsnagSession will return a Context for a new Bugsnag session
func StartBugsnagSession() context.Context {
	return bugsnag.StartSession(context.Background())
}

// BugsnagHandler is crash Handler that forwards PanicInformation to Bugsnag
func BugsnagHandler(pi PanicInformation) {
	bugsnag.AutoNotify(pi.Context, bugsnag.SeverityError, bugsnag.MetaData{
		"GoRoutines": {
			"Number": runtime.NumGoroutine(),
		},
	})
}
