# Go Rules

## rudder-go-kit

https://github.com/rudderlabs/rudder-go-kit opinionated library for rudderstack. Should be used for all Golang projects,
for common things like logging, config, http, etc.

### HTTP

import `github.com/rudderlabs/rudder-go-kit/httputil`

#### Request

When doing an HTTP request you must always close the response body. To do this:

DO NOT use:

```go
    defer resp.Body.Close()
```

DO use:

```go
    defer func() { httputil.CloseResponse(resp) }()
```

#### Handlers

Always use the standard http.Handler interface and return proper handlers:

```go
func SomeHandler() http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // handler logic
    })
}
```

### Logging

Always use rudder-go-kit for logging.

```go
import (
	"github.com/rudderlabs/rudder-go-kit/logger"
    obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)
```

Non-sugared methods SHOULD BE used along with common fields from rudder-observability-kit when available.

For example if a `sourceId` is available you can use `obskit.SourceID("some-source-id")`.

Non-sugared log methods has `n` suffix.

DO NOT:

```go
   fmt.Println("foo")
   fmt.Printf("foo %d\n", i)
```

DO NOT:

```go
log.Info("starting", port, port)
log.Infow("starting", port, port)
log.Infof("starting port %d", port)
```

DO NOT (string formatting in message):

```go
log.Infon("starting on port %d", logger.NewIntField("port", int64(port)))
```

DO (descriptive message with structured fields):

```go
log.Infon("starting", logger.NewIntField("port", int64(port)))
```

Avoid `logger.NewField` and use the strong-typed counterparts instead like `logger.NewStringField`,
`logger.NewBooldField` etc...

Avoid using reflection like `fmt.Sprintf` to convert values into string, for example DO NOT:

```go
m := make(map[string]string)
m["a"] = "b"
log.Infon("starting", logger.NewIntField("map", fmt.Sprintf("%v", m)))
```

**ABSOLUTELY NO REFLECTION**: Never use `fmt.Sprintf`, `fmt.Printf`, or any other reflection-based formatting in logging calls. Instead:
- For string slices: use `strings.Join(slice, ", ")`
- For custom types: implement a `String() string` method that manually builds the output
- For primitive types: use appropriate `logger.NewXXXField` constructors directly
- For complex data: break down into individual fields or implement custom string methods
- Never use logger.NewField since it takes `any`

#### Error Logging

Always use `obskit.Error` for error logging:

```go
log.Errorn("operation failed", obskit.Error(err))
```

#### Field Selection Guidelines

**For code in the /warehouse folder:**
- Use fields from `logfield` package when available (e.g., `logfield.SourceID`, `logfield.TableName`)
- Use `logfield.Query` if in the code we're logging queries e.g. `sqlStatement`
- For errors, always use `obskit.Error(err)`
- Import: `"github.com/rudderlabs/rudder-server/warehouse/logfield"`

**For code outside the warehouse folder:**
- Use fields from `obskit` when available (e.g., `obskit.SourceID("some-id")`)
- For errors, use `obskit.Error(err)`
- For fields not available in `obskit`, use `logger.NewStringField`, `logger.NewIntField`, etc.

Examples:

```go
// In warehouse folder
log.Infon("processing table",
    logger.NewStringField(logfield.SourceID, sourceID),
    logger.NewStringField(logfield.TableName, tableName),
)

// Outside warehouse folder
log.Infon("processing source",
    obskit.SourceID(sourceID),
    logger.NewStringField("tableName", tableName),
)
```

#### Migrating Existing Logging Code

When converting existing sugared logging calls to non-sugared methods:

- **ONLY** change the method calls (e.g., `Infof` → `Infon`, `Warnw` → `Warnn`)
- **PRESERVE** existing message formats, prefixes, and casing
- **CONVERT** parameters to proper field constructors following the field selection guidelines above
- **DO NOT** modify message content unless explicitly requested

Example migration:
```go
// Before
log.Infof("AZ: Creating table for destination %s: %v", destID, query)

// After
log.Infon("AZ: Creating table for destination",
    logger.NewStringField(logfield.DestinationID, destID),
    logger.NewStringField(logfield.Query, query),
)
```

### Config

Always use rudder-go-kit for configuration.

import `github.com/rudderlabs/rudder-go-kit/config`

#### Init Config

Only init config once in program's main file. Use DI in all other places. DO NOT use global / singleton config pattern.

You should use a service name prefix:

```go
import (
	kitconfig "github.com/rudderlabs/rudder-go-kit/config"
)

conf := kitconfig.New(kitconfig.WithEnvPrefix("SERVICE_NAME"))
```

#### Getting Values

Always provide default values:

```go
port := conf.GetInt("HTTP.Port", 8080)
timeout := conf.GetDuration("HTTP.ShutdownTimeout", 10, time.Second)
enabled := conf.GetBool("Feature.Enabled", false)
name := conf.GetString("Service.Name", "default-service")
```

#### Config during testing

On your test, create a new conf, so you can manipulate it without side-effects.

Only use `Set()` in tests:

```go
// In tests only
conf.Set("HTTP.Port", port)
conf.Set("Profiler.Enabled", false)
```

### Stats

Always use rudder-go-kit for metrics and statistics.

```go
import (
    "github.com/rudderlabs/rudder-go-kit/stats"
    "github.com/rudderlabs/rudder-go-kit/stats/metric"
)
```

#### Stats Init

Always configure stats with service information:

```go
stat := stats.NewStats(conf, logFactory, svcMetric.NewManager(), []stats.Option{
    stats.WithServiceName(serviceName),
    stats.WithServiceVersion(version),
    stats.WithDefaultHistogramBuckets(customBuckets),
})
```

```go
if err := stat.Start(ctx, stats.DefaultGoRoutineFactory); err != nil {
    log.Errorn("Failed to start Stats", obskit.Error(err))
    return err
}
defer stat.Stop()
```

Only init in program's main. Use DI in all other places

#### Usage

TODO

## Context

### Function Signatures

Always pass context as first parameter:

```go
func runWith(ctx context.Context, conf *kitconfig.Config, log logger.Logger) error {
    // function body
}
```

### Cancellation

Always handle context cancellation properly:

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
```

## Testing

### Test Structure

Always use testify/require for assertions:

```go
import "github.com/stretchr/testify/require"

func TestSomething(t *testing.T) {
    require.NoError(t, err, "descriptive error message")
    require.Equal(t, expected, actual, "descriptive error message")
}
```

#### Async Testing

Always use `require.Eventually` for async operations:

```go
require.Eventually(t, func() bool {
    // condition to check
    return someCondition
}, 10*time.Second, 100*time.Millisecond, "descriptive timeout message")
```

Avoid using other `require` functions inside `Eventually` and `Never`. The lambda function invoked by `Eventually` and
`Never` should just return a `bool` and must not make other assertions via `require` or via `t`.

## Error Handling

### Never Ignore Errors

Always handle errors appropriately:

DO NOT:

```go
result, _ := someFunction()
```

ALWAYS DO:

```go
result, err := someFunction()
if err != nil {
    // handle error here
}
```

### Error Wrapping

In case you can not handle the error and you need to propagate it. You SHOULD consider wrapping it using
`fmt.Errorf("...: %w)`, if additional context would make it easier for understanding the error.

Avoid using "failure", or "error", or similar word when wrapping. Instead focus on describing the behaviour tha cause the issue.

DO NOT:

```go
if err != nil {
    return fmt.Errorf("failed to start server on port %d: %w", port, err)
}
```

DO:

```go
if err != nil {
    return fmt.Errorf("starting server on port %d: %w", port, err)
}
```
