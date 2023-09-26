package logger

const (
	levelEvent = iota // Logs Event
	levelDebug        // Most verbose logging level
	levelInfo         // Logs about state of the application
	levelWarn         // Logs about warnings
	levelError        // Logs about errors which dont immediately halt the application
	levelFatal        // Logs which crashes the application
)

var levelMap = map[string]int{
	"EVENT": levelEvent,
	"DEBUG": levelDebug,
	"INFO":  levelInfo,
	"WARN":  levelWarn,
	"ERROR": levelError,
	"FATAL": levelFatal,
}
