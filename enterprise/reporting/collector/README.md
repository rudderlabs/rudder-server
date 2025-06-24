# Metrics Collector System

This package implements a new metrics collection system for the Rudder Server reporting functionality. The system is designed to be flexible, extensible, and support multiple types of metrics collection through a unified interface.

## Overview

The new system consists of:

1. **MetricsCollector Interface** - A unified interface that all collectors implement
2. **MetricsCollectorMediator** - A mediator that forwards events to multiple collectors
3. **Specialized Collectors** - Different collectors for different types of metrics

## Architecture

### MetricsCollector Interface

The `MetricsCollector` interface defines the contract for all collectors:

```go
type MetricsCollector interface {
    CurrentStage() string
    Collect(events ...*reportingtypes.OutMetricEvent) error
    NextStage(stage string) error
    End(terminal bool) error
    Merge(other MetricsCollector) (MetricsCollector, error)
    Flush(ctx context.Context, tx *sql.Tx) error
}
```

### MetricsCollectorMediator

The `MetricsCollectorMediator` implements the `MetricsCollector` interface and forwards events to all registered collectors. This allows the Rudder Server code to make a single call to collect different types of metrics.

## Configuration

The system supports configuration-based enabling/disabling of different collector types:

```go
// Configuration keys for enabling/disabling collectors
Reporting.defaultMetrics.enabled    // Default: true
Reporting.errorDetails.enabled      // Default: false
Reporting.mtu.enabled               // Default: false
Reporting.dataMapper.enabled        // Default: false
```

## Available Collectors

### 1. DefaultMetricsCollector

- **Purpose**: Collects metrics for billing and UI features
- **Target Table**: `reports` table
- **Stages**: All stages
- **Usage**: Collects all events and aggregates them by connection details and status
- **Config Key**: `Reporting.defaultMetrics.enabled`

### 2. ErrorDetailsMetricsCollector

- **Purpose**: Collects error details for the integrations team
- **Target Table**: `error_detail_reports` table
- **Stages**: All stages
- **Usage**: Only collects events with status codes >= 400 or specific error codes
- **Config Key**: `Reporting.errorDetails.enabled`

### 3. MTUMetricsCollector

- **Purpose**: Collects MTU (Monthly Tracked Users) metrics
- **Target Table**: `tracked_users_reports` table
- **Stages**: Only `gateway` stage
- **Usage**: Tracks unique users for MTU counting
- **Config Key**: `Reporting.mtu.enabled`

### 4. DataMapperCollector

- **Purpose**: Collects metrics for the upcoming DataMapper feature
- **Target Table**: New DataMapper outbox table
- **Stages**: Only `gateway` stage
- **Usage**: Collects DataMapper-specific metrics and transformation times
- **Config Key**: `Reporting.dataMapper.enabled`

## Usage Examples

### Using Configuration-Based Collector Creation

```go
// Create a config instance
conf := config.New()

// Create some input events for the gateway stage
inputEvents := []*reportingtypes.InMetricEvent{
    {
        ConnectionLabels: reportingtypes.ConnectionLabels{
            SourceLabels: reportingtypes.SourceLabels{
                SourceID: "source-1",
                // ... other fields
            },
            // ... other labels
        },
        EventLabels: reportingtypes.EventLabels{
            EventType: "track",
            EventName: "Page Viewed",
        },
        Event: reportingtypes.Event{
            ID: "event-1",
        },
    },
}

// Create a mediator with enabled collectors based on configuration
// and initialize it with the gateway stage and input events
mediator := NewMetricsCollectorMediatorWithConfig(reportingtypes.GATEWAY, inputEvents, conf)

// Create some output events for collection
outputEvents := []*reportingtypes.OutMetricEvent{
    {
        ConnectionLabels: reportingtypes.ConnectionLabels{
            // ... connection labels
        },
        EventLabels: reportingtypes.EventLabels{
            EventType: "track",
            EventName: "Page Viewed",
        },
        StatusLabels: reportingtypes.StatusLabels{
            Status:     "success",
            StatusCode: 200,
            ErrorType:  "",
        },
        Event: reportingtypes.Event{
            ID: "event-1",
        },
    },
}

// Collect output events
mediator.Collect(outputEvents...)

// Move to next stage
mediator.NextStage(reportingtypes.DEST_TRANSFORMER)

// Collect more events for the new stage
mediator.Collect(outputEvents...)

// End the collection
mediator.End(true)

// Flush to database
ctx := context.Background()
mediator.Flush(ctx, tx)
```

### Manual Collector Creation

```go
// Create individual collectors manually
defaultCollector := NewDefaultMetricsCollector()
errorDetailsCollector := NewErrorDetailsMetricsCollector()
mtuCollector := NewMTUMetricsCollector()
dataMapperCollector := NewDataMapperCollector()

// Create a mediator that forwards events to all collectors
mediator := NewMetricsCollectorMediator(
    defaultCollector,
    errorDetailsCollector,
    mtuCollector,
    dataMapperCollector,
)

// Use the mediator as before...
```

## Key Features

### Stage Management

Each collector maintains its current stage and can only collect events for the active stage. This ensures proper event flow through the processing pipeline.

### Event Filtering

Different collectors can filter events based on their requirements:

- **DefaultMetricsCollector**: Collects all events
- **ErrorDetailsMetricsCollector**: Only collects error events
- **MTUMetricsCollector**: Only collects from gateway stage
- **DataMapperCollector**: Only collects from gateway stage

### Configuration-Driven

The system can be configured to enable/disable specific collectors without code changes, making it easy to control which metrics are collected in different environments.

### Input/Output Event Support

The system supports both input events (`InMetricEvent`) and output events (`OutMetricEvent`), allowing for comprehensive metrics collection throughout the processing pipeline.

### Merging Support

Collectors can be merged together, which is useful for combining metrics from different processing batches or workers.

### Thread Safety

All collectors are thread-safe and use proper locking mechanisms to ensure concurrent access safety.

### Database Integration

Each collector has a `Flush` method that writes the collected metrics to the appropriate database table. The actual database write logic needs to be implemented based on the specific requirements.

## Migration from Old System

The new system replaces the old `DefaultMetricsCollector` that used a `MetricsStore`. The new system:

1. **Separates concerns**: Each collector handles its specific metric type
2. **Improves flexibility**: Easy to add new collectors without modifying existing code
3. **Enhances performance**: Collectors can filter events early, reducing unnecessary processing
4. **Simplifies usage**: Single mediator interface for all metric collection
5. **Configuration-driven**: Easy to enable/disable collectors via configuration
6. **Input/Output support**: Supports both input and output events for comprehensive metrics

## Future Enhancements

1. **Database Implementation**: Implement actual database write logic in the `Flush` methods
2. **Configuration**: Add more configuration options for collector behavior
3. **Metrics**: Add metrics collection for the collector system itself
4. **Testing**: Add comprehensive unit tests for all collectors
5. **Documentation**: Add more detailed API documentation

## Files

- `types.go` - Interface definitions
- `mediator.go` - MetricsCollectorMediator implementation and NewMetricsCollectorMediatorWithConfig function
- `collector.go` - DefaultMetricsCollector implementation
- `error_details_collector.go` - ErrorDetailsMetricsCollector implementation
- `mtu_collector.go` - MTUMetricsCollector implementation
- `datamapper_collector.go` - DataMapperCollector implementation
- `example.go` - Usage examples
- `config_test.go` - Configuration-based tests
- `README.md` - This documentation
