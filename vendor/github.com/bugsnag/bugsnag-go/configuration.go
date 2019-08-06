package bugsnag

import (
	"log"
	"net/http"
	"path/filepath"
	"strings"
)

// Endpoints hold the HTTP endpoints of the notifier.
type Endpoints struct {
	Sessions string
	Notify   string
}

// Configuration sets up and customizes communication with the Bugsnag API.
type Configuration struct {
	// Your Bugsnag API key, e.g. "c9d60ae4c7e70c4b6c4ebd3e8056d2b8". You can
	// find this by clicking Settings on https://bugsnag.com/.
	APIKey string

	// Deprecated: Use Endpoints (with an 's') instead.
	// The Endpoint to notify about crashes. This defaults to
	// "https://notify.bugsnag.com/", if you're using Bugsnag Enterprise then
	// set it to your internal Bugsnag endpoint.
	Endpoint string
	// Endpoints define the HTTP endpoints that the notifier should notify
	// about crashes and sessions. These default to notify.bugsnag.com for
	// error reports and sessions.bugsnag.com for sessions.
	// If you are using bugsnag on-premise you will have to set these to your
	// Event Server and Session Server endpoints. If the notify endpoint is set
	// but the sessions endpoint is not, session tracking will be disabled
	// automatically to avoid leaking session information outside of your
	// server configuration, and a warning will be logged.
	Endpoints Endpoints

	// The current release stage. This defaults to "production" and is used to
	// filter errors in the Bugsnag dashboard.
	ReleaseStage string
	// A specialized type of the application, such as the worker queue or web
	// framework used, like "rails", "mailman", or "celery"
	AppType string
	// The currently running version of the app. This is used to filter errors
	// in the Bugsnag dasboard. If you set this then Bugsnag will only re-open
	// resolved errors if they happen in different app versions.
	AppVersion string

	// AutoCaptureSessions can be set to false to disable automatic session
	// tracking. If you want control over what is deemed a session, you can
	// switch off automatic session tracking with this configuration, and call
	// bugsnag.StartSession() when appropriate for your application. See the
	// official docs for instructions and examples of associating handled
	// errors with sessions and ensuring error rate accuracy on the Bugsnag
	// dashboard. This will default to true, but is stored as an interface to enable
	// us to detect when this option has not been set.
	AutoCaptureSessions interface{}

	// The hostname of the current server. This defaults to the return value of
	// os.Hostname() and is graphed in the Bugsnag dashboard.
	Hostname string

	// The Release stages to notify in. If you set this then bugsnag-go will
	// only send notifications to Bugsnag if the ReleaseStage is listed here.
	NotifyReleaseStages []string

	// packages that are part of your app. Bugsnag uses this to determine how
	// to group errors and how to display them on your dashboard. You should
	// include any packages that are part of your app, and exclude libraries
	// and helpers. You can list wildcards here, and they'll be expanded using
	// filepath.Glob. The default value is []string{"main*"}
	ProjectPackages []string

	// The SourceRoot is the directory where the application is built, and the
	// assumed prefix of lines on the stacktrace originating in the parent
	// application. When set, the prefix is trimmed from callstack file names
	// before ProjectPackages for better readability and to better group errors
	// on the Bugsnag dashboard. The default value is $GOPATH/src or $GOROOT/src
	// if $GOPATH is unset. At runtime, $GOROOT is the root used during the Go
	// build.
	SourceRoot string

	// Any meta-data that matches these filters will be marked as [FILTERED]
	// before sending a Notification to Bugsnag. It defaults to
	// []string{"password", "secret"} so that request parameters like password,
	// password_confirmation and auth_secret will not be sent to Bugsnag.
	ParamsFilters []string

	// The PanicHandler is used by Bugsnag to catch unhandled panics in your
	// application. The default panicHandler uses mitchellh's panicwrap library,
	// and you can disable this feature by passing an empty: func() {}
	PanicHandler func()

	// The logger that Bugsnag should log to. Uses the same defaults as go's
	// builtin logging package. bugsnag-go logs whenever it notifies Bugsnag
	// of an error, and when any error occurs inside the library itself.
	Logger interface {
		Printf(format string, v ...interface{}) // limited to the functions used
	}
	// The http Transport to use, defaults to the default http Transport. This
	// can be configured if you are in an environment
	// that has stringent conditions on making http requests.
	Transport http.RoundTripper
	// Whether bugsnag should notify synchronously. This defaults to false which
	// causes bugsnag-go to spawn a new goroutine for each notification.
	Synchronous bool
	// Whether the notifier should send all sessions recorded so far to Bugsnag
	// when repanicking to ensure that no session information is lost in a
	// fatal crash.
	flushSessionsOnRepanic bool
	// TODO: remember to update the update() function when modifying this struct
}

func (config *Configuration) update(other *Configuration) *Configuration {
	if other.APIKey != "" {
		config.APIKey = other.APIKey
	}
	if other.Hostname != "" {
		config.Hostname = other.Hostname
	}
	if other.AppType != "" {
		config.AppType = other.AppType
	}
	if other.AppVersion != "" {
		config.AppVersion = other.AppVersion
	}
	if other.SourceRoot != "" {
		config.SourceRoot = other.SourceRoot
	}
	if other.ReleaseStage != "" {
		config.ReleaseStage = other.ReleaseStage
	}
	if other.ParamsFilters != nil {
		config.ParamsFilters = other.ParamsFilters
	}
	if other.ProjectPackages != nil {
		config.ProjectPackages = other.ProjectPackages
	}
	if other.Logger != nil {
		config.Logger = other.Logger
	}
	if other.NotifyReleaseStages != nil {
		config.NotifyReleaseStages = other.NotifyReleaseStages
	}
	if other.PanicHandler != nil {
		config.PanicHandler = other.PanicHandler
	}
	if other.Transport != nil {
		config.Transport = other.Transport
	}
	if other.Synchronous {
		config.Synchronous = true
	}

	if other.AutoCaptureSessions != nil {
		config.AutoCaptureSessions = other.AutoCaptureSessions
	}
	config.updateEndpoints(other.Endpoint, &other.Endpoints)
	return config
}

// IsAutoCaptureSessions identifies whether or not the notifier should
// automatically capture sessions as requests come in. It's a convenience
// wrapper that allows automatic session capturing to be enabled by default.
func (config *Configuration) IsAutoCaptureSessions() bool {
	if config.AutoCaptureSessions == nil {
		return true // enabled by default
	}
	if val, ok := config.AutoCaptureSessions.(bool); ok {
		return val
	}
	// It has been configured to *something* (although not a valid value)
	// assume the user wanted to disable this option.
	return false
}

func (config *Configuration) updateEndpoints(endpoint string, endpoints *Endpoints) {

	if endpoint != "" {
		config.Logger.Printf("WARNING: the 'Endpoint' Bugsnag configuration parameter is deprecated in favor of 'Endpoints'")
		config.Endpoints.Notify = endpoint
		config.Endpoints.Sessions = ""
	}
	if endpoints.Notify != "" {
		config.Endpoints.Notify = endpoints.Notify
		if endpoints.Sessions == "" {
			config.Logger.Printf("WARNING: Bugsnag notify endpoint configured without also configuring the sessions endpoint. No sessions will be recorded")
			config.Endpoints.Sessions = ""
		}
	}
	if endpoints.Sessions != "" {
		if endpoints.Notify == "" {
			panic("FATAL: Bugsnag sessions endpoint configured without also changing the notify endpoint. Bugsnag cannot identify where to report errors")
		}
		config.Endpoints.Sessions = endpoints.Sessions
	}
}

func (config *Configuration) merge(other *Configuration) *Configuration {
	return config.clone().update(other)
}

func (config *Configuration) clone() *Configuration {
	clone := *config
	return &clone
}

func (config *Configuration) isProjectPackage(pkg string) bool {
	for _, p := range config.ProjectPackages {
		if d, f := filepath.Split(p); f == "**" {
			if strings.HasPrefix(pkg, d) {
				return true
			}
		}

		if match, _ := filepath.Match(p, pkg); match {
			return true
		}
	}
	return false
}

func (config *Configuration) stripProjectPackages(file string) string {
	trimmedFile := file
	if strings.HasPrefix(trimmedFile, config.SourceRoot) {
		trimmedFile = strings.TrimPrefix(trimmedFile, config.SourceRoot)
	}
	for _, p := range config.ProjectPackages {
		if len(p) > 2 && p[len(p)-2] == '/' && p[len(p)-1] == '*' {
			p = p[:len(p)-1]
		} else if p[len(p)-1] == '*' && p[len(p)-2] == '*' {
			p = p[:len(p)-2]
		} else {
			p = p + "/"
		}
		if strings.HasPrefix(trimmedFile, p) {
			return strings.TrimPrefix(trimmedFile, p)
		}
	}

	return trimmedFile
}

func (config *Configuration) logf(fmt string, args ...interface{}) {
	if config != nil && config.Logger != nil {
		config.Logger.Printf(fmt, args...)
	} else {
		log.Printf(fmt, args...)
	}
}

func (config *Configuration) notifyInReleaseStage() bool {
	if config.NotifyReleaseStages == nil {
		return true
	}
	if config.ReleaseStage == "" {
		return true
	}
	for _, r := range config.NotifyReleaseStages {
		if r == config.ReleaseStage {
			return true
		}
	}
	return false
}
