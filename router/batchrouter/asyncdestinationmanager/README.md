# AsyncDestinationManager

This document explains how the AsyncDestinationManager works in RudderStack's batch router and provides a comprehensive guide for onboarding new integrations.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Core Interfaces](#core-interfaces)
- [Integration Types](#integration-types)
- [Onboarding New Integrations](#onboarding-new-integrations)
- [Implementation Patterns](#implementation-patterns)
- [Testing Guidelines](#testing-guidelines)
- [Configuration](#configuration)
- [Examples](#examples)

## Overview

AsyncDestinationManager is a framework within RudderStack's batch router that handles asynchronous data uploads to external destinations. It provides a standardized interface for destinations that require:

1. **Bulk data uploads** (CSV, JSON files)
2. **Asynchronous processing** (polling for completion status)
3. **Complex authentication** (OAuth, token management)
4. **File-based transfers** (SFTP, cloud storage)

The framework abstracts the complexity of async operations while providing hooks for destination-specific customization.

## Architecture

```
┌─────────────────┐    ┌──────────────────────┐    ┌─────────────────────┐
│   BatchRouter   │───▶│ AsyncDestinationMgr  │───▶│  Destination APIs   │
└─────────────────┘    └──────────────────────┘    └─────────────────────┘
                                  │
                                  ▼
                       ┌──────────────────────┐
                       │     File Storage     │
                       │  (Temp files, etc.) │
                       └──────────────────────┘
```

### Key Components

1. **AsyncDestinationManager Interface**: Core contract that all integrations must implement
2. **Manager Factory**: Routes destination types to their specific implementations
3. **Common Utilities**: Shared functionality for file handling, configuration, etc.
4. **Integration Implementations**: Destination-specific logic

## Core Interfaces

### AsyncDestinationManager

The main interface that all integrations must implement:

```go
type AsyncDestinationManager interface {
    AsyncUploadAndTransformManager
    Poll(pollInput AsyncPoll) PollStatusResponse
    GetUploadStats(UploadStatsInput GetUploadStatsInput) GetUploadStatsResponse
}

type AsyncUploadAndTransformManager interface {
    Upload(asyncDestStruct *AsyncDestinationStruct) AsyncUploadOutput
    Transform(job *jobsdb.JobT) (string, error)
}
```

### Key Methods

1. **Transform**: Converts job data into the format expected by the destination
2. **Upload**: Handles the actual data upload to the destination
3. **Poll**: Checks the status of ongoing upload operations
4. **GetUploadStats**: Retrieves detailed statistics about upload results

## Integration Types

### 1. Regular Async Destinations

Destinations that require complex polling and status checking:
- Marketo Bulk Upload
- Bing Ads (Audience & Offline Conversions)
- Eloqua
- Yandex Metrica
- Klaviyo Bulk Upload
- Lytics Bulk Upload
- Snowpipe Streaming

### 2. SFTP Destinations

File-based destinations that use SFTP protocol:
- SFTP (generic file transfer)

### 3. Simple Async Destinations

Destinations that don't require complex polling (using `SimpleAsyncDestinationManager`):
- Basic file uploads
- Fire-and-forget operations

## Onboarding New Integrations

### Step 1: Determine Integration Type

Choose the appropriate integration pattern:

- **Complex Async**: Requires polling, status checking, detailed error handling
- **Simple Async**: Basic upload with immediate completion
- **SFTP**: File-based transfer using SFTP protocol

### Step 2: Create Integration Directory

Create a new directory under `asyncdestinationmanager/`:

```
asyncdestinationmanager/
└── your-destination/
    ├── manager.go          # Main manager implementation
    ├── types.go           # Data structures
    ├── your-destination.go # Core logic
    ├── utils.go           # Helper functions (optional)
    ├── testdata/          # Test files
    └── your-destination_test.go
```

### Step 3: Implement Required Files

#### manager.go
```go
package yourdestination

import (
    "github.com/rudderlabs/rudder-go-kit/logger"
    "github.com/rudderlabs/rudder-go-kit/stats"
    backendconfig "github.com/rudderlabs/rudder-server/backend-config"
    "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

func NewManager(logger logger.Logger, statsFactory stats.Stats, destination *backendconfig.DestinationT) (common.AsyncDestinationManager, error) {
    // Parse destination config
    // Initialize API clients
    // Return your manager implementation
}
```

#### types.go
```go
package yourdestination

// Define your destination-specific types
type DestinationConfig struct {
    ApiKey    string `json:"apiKey"`
    BaseURL   string `json:"baseUrl"`
    // Add other config fields
}

type UploadResponse struct {
    ImportID string `json:"importId"`
    Status   string `json:"status"`
}
```

### Step 4: Implement Core Interface

Choose one of these patterns:

#### Pattern A: Full AsyncDestinationManager Implementation

```go
type YourDestinationManager struct {
    logger       logger.Logger
    statsFactory stats.Stats
    config       DestinationConfig
    // Add other fields
}

func (m *YourDestinationManager) Transform(job *jobsdb.JobT) (string, error) {
    // Convert job payload to your destination format
    return common.GetMarshalledData(string(job.EventPayload), job.JobID)
}

func (m *YourDestinationManager) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
    // Implement upload logic
    // Return appropriate AsyncUploadOutput
}

func (m *YourDestinationManager) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
    // Check upload status using pollInput.ImportId
    // Return status response
}

func (m *YourDestinationManager) GetUploadStats(input common.GetUploadStatsInput) common.GetUploadStatsResponse {
    // Parse upload results and return detailed statistics
}
```

#### Pattern B: Simple Implementation (for fire-and-forget destinations)

```go
func NewManager(logger logger.Logger, statsFactory stats.Stats, destination *backendconfig.DestinationT) (common.AsyncDestinationManager, error) {
    uploader, err := NewYourDestinationUploader(logger, statsFactory, destination)
    if err != nil {
        return nil, err
    }
    return common.SimpleAsyncDestinationManager{UploaderAndTransformer: uploader}, nil
}
```

### Step 5: Register in Manager Factory

Add your destination to `asyncdestinationmanager/manager.go`:

```go
// In utils.go, add to the appropriate slice
var asyncDestinations = []string{
    // ... existing destinations
    "YOUR_DESTINATION_NAME",
}

// In manager.go, add to newRegularManager function
func newRegularManager(...) (common.AsyncDestinationManager, error) {
    switch destination.DestinationDefinition.Name {
    // ... existing cases
    case "YOUR_DESTINATION_NAME":
        return yourdestination.NewManager(conf, logger, statsFactory, destination, backendConfig)
    }
}
```

### Step 6: Add Tests

Create comprehensive tests following the warehouse testing guidelines:

```go
func TestYourDestinationManager(t *testing.T) {
    testCases := []struct {
        name     string
        input    InputType
        expected ExpectedType
        wantErr  bool
    }{
        {
            name:     "successful upload",
            input:    validInput,
            expected: expectedOutput,
            wantErr:  false,
        },
        // Add more test cases
    }
    
    for _, tc := range testCases {
        t.Run(tc.name, func(t *testing.T) {
            t.Parallel()
            // Test implementation
        })
    }
}
```

## Implementation Patterns

### 1. Configuration Parsing

```go
func parseDestinationConfig(destination *backendconfig.DestinationT) (YourDestinationConfig, error) {
    var config YourDestinationConfig
    jsonConfig, err := jsonrs.Marshal(destination.Config)
    if err != nil {
        return config, fmt.Errorf("error marshalling destination config: %v", err)
    }
    err = jsonrs.Unmarshal(jsonConfig, &config)
    if err != nil {
        return config, fmt.Errorf("error unmarshalling destination config: %v", err)
    }
    return config, nil
}
```

### 2. File Processing

```go
func processUploadFile(filePath string) ([]YourDataType, error) {
    file, err := os.Open(filePath)
    if err != nil {
        return nil, err
    }
    defer file.Close()
    
    var data []YourDataType
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        var item YourDataType
        if err := jsonrs.Unmarshal(scanner.Bytes(), &item); err != nil {
            return nil, err
        }
        data = append(data, item)
    }
    return data, scanner.Err()
}
```

### 3. Error Handling

```go
func (m *YourDestinationManager) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
    // ... upload logic
    
    if err != nil {
        return common.AsyncUploadOutput{
            FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
            FailedReason:  fmt.Sprintf("Upload failed: %v", err),
            FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
            DestinationID: asyncDestStruct.Destination.ID,
        }
    }
    
    // Success case
    return common.AsyncUploadOutput{
        ImportingJobIDs:     asyncDestStruct.ImportingJobIDs,
        ImportingParameters: getImportingParameters(importID),
        ImportingCount:      len(asyncDestStruct.ImportingJobIDs),
        DestinationID:       asyncDestStruct.Destination.ID,
    }
}
```

### 4. Polling Implementation

```go
func (m *YourDestinationManager) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
    status, err := m.apiService.GetImportStatus(pollInput.ImportId)
    if err != nil {
        return common.PollStatusResponse{
            StatusCode: http.StatusInternalServerError,
            Error:      err.Error(),
        }
    }
    
    switch status.State {
    case "completed":
        return common.PollStatusResponse{
            StatusCode: http.StatusOK,
            Complete:   true,
        }
    case "failed":
        return common.PollStatusResponse{
            StatusCode: http.StatusOK,
            HasFailed:  true,
            Error:      status.ErrorMessage,
        }
    default:
        return common.PollStatusResponse{
            StatusCode: http.StatusOK,
            InProgress: true,
        }
    }
}
```

## Testing Guidelines

Follow the warehouse testing guidelines:

1. **Use `require` for assertions**: `require.NoError(t, err)`
2. **Table-driven tests** for multiple scenarios
3. **Parallel execution**: Use `t.Parallel()` where safe
4. **Proper cleanup**: Use `t.Cleanup()` for resource cleanup
5. **Mock external services**: Use gomock for API interactions

### Example Test Structure

```go
func TestYourDestination_Upload(t *testing.T) {
    testCases := []struct {
        name           string
        setupMock      func(*MockAPIService)
        asyncDestStruct *common.AsyncDestinationStruct
        expectedOutput common.AsyncUploadOutput
    }{
        {
            name: "successful upload",
            setupMock: func(mock *MockAPIService) {
                mock.EXPECT().Upload(gomock.Any()).Return("import-123", nil)
            },
            // ... test data
        },
    }
    
    for _, tc := range testCases {
        t.Run(tc.name, func(t *testing.T) {
            t.Parallel()
            
            ctrl := gomock.NewController(t)
            defer ctrl.Finish()
            
            mockAPI := NewMockAPIService(ctrl)
            tc.setupMock(mockAPI)
            
            manager := &YourDestinationManager{
                apiService: mockAPI,
                // ... other fields
            }
            
            result := manager.Upload(tc.asyncDestStruct)
            require.Equal(t, tc.expectedOutput, result)
        })
    }
}
```

## Configuration

### Destination-Specific Config

Use the config utilities for destination-specific settings:

```go
// Get destination-specific or fallback to global config
fileSizeLimit := common.GetBatchRouterConfigInt64("MaxUploadLimit", destName, 32*bytesize.MB)
retryCount := common.GetBatchRouterConfigInt64("maxRetries", destName, 3)
```

### Environment Variables

Follow the pattern: `BatchRouter.DESTINATION_NAME.configKey`

Example:
- `BatchRouter.MARKETO_BULK_UPLOAD.maxRetries`
- `BatchRouter.ELOQUA.MaxUploadLimit`

## Examples

### Complete Simple Implementation

See `sftp/manager.go` for a complete example of a simple async destination.

### Complete Complex Implementation

See `marketo-bulk-upload/` for a full-featured async destination with:
- Complex API interactions
- CSV file generation
- Polling with detailed status
- Error handling and statistics

### API Service Pattern

See `klaviyobulkupload/apiService.go` for an example of separating API interactions into a dedicated service.

## Best Practices

1. **Error Handling**: Always provide detailed error messages with context
2. **Logging**: Use structured logging with appropriate log levels
3. **Metrics**: Emit relevant metrics for monitoring and debugging
4. **Resource Cleanup**: Always clean up temporary files and connections
5. **Configuration**: Use the common config utilities for consistency
6. **Testing**: Write comprehensive tests covering success and failure scenarios
7. **Documentation**: Document any destination-specific requirements or limitations

## Troubleshooting

### Common Issues

1. **Import not registered**: Ensure your destination is added to `manager.go`
2. **Config parsing errors**: Verify your config struct matches the destination configuration
3. **File handling issues**: Always use proper file cleanup with `defer` or `t.Cleanup()`
4. **Authentication failures**: Check API key handling and token refresh logic
5. **Polling timeouts**: Implement appropriate timeout handling in polling logic

### Debugging Tips

1. Enable debug logging: Set log level to debug for detailed operation logs
2. Check file contents: Verify the format of generated upload files
3. Monitor metrics: Use the emitted metrics to track upload success/failure rates
4. Test with small batches: Start with small data sets to verify the integration works

## Migration from Legacy Destinations

If migrating an existing destination to AsyncDestinationManager:

1. Identify the current upload pattern (sync vs async)
2. Extract configuration parsing logic
3. Separate file generation from upload logic
4. Implement the AsyncDestinationManager interface
5. Add comprehensive tests
6. Update destination routing in the manager factory

---

For questions or issues with AsyncDestinationManager integrations, refer to existing implementations in the `asyncdestinationmanager/` directory or consult the team documentation.
