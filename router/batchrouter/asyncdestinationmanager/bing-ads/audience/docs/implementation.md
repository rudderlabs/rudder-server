# Bing Ads Audience - Implementation Details

## Table of Contents

- [1. Architecture Overview](#1-architecture-overview)
- [2. Manager Initialization](#2-manager-initialization)
- [3. Transform Implementation](#3-transform-implementation)
- [4. Upload Implementation](#4-upload-implementation)
- [5. Poll Implementation](#5-poll-implementation)
- [6. GetUploadStats Implementation](#6-getuploadstats-implementation)
- [7. Utility Functions](#7-utility-functions)
- [8. Error Handling Patterns](#8-error-handling-patterns)
- [9. File Format Specifications](#9-file-format-specifications)

---

## 1. Architecture Overview

### 1.1 Component Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Batch Router                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                    AsyncDestinationManager                          │    │
│  │  ┌────────────────────────────────────────────────────────────────┐ │    │
│  │  │                  BingAdsBulkUploader                           │ │    │
│  │  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │ │    │
│  │  │  │  Transform   │  │    Upload    │  │        Poll          │  │ │    │
│  │  │  │              │  │              │  │                      │  │ │    │
│  │  │  │ Extract JSON │  │ Create CSV   │  │ Check upload status  │  │ │    │
│  │  │  │ Add metadata │  │ Create ZIP   │  │ Parse results        │  │ │    │
│  │  │  └──────────────┘  │ Upload via   │  └──────────────────────┘  │ │    │
│  │  │                    │ Bulk API     │                            │ │    │
│  │  │                    └──────────────┘                            │ │    │
│  │  │  ┌──────────────────────────────────────────────────────────┐  │ │    │
│  │  │  │                    GetUploadStats                        │  │ │    │
│  │  │  │  Download result file → Parse errors → Return stats      │  │ │    │
│  │  │  └──────────────────────────────────────────────────────────┘  │ │    │
│  │  └────────────────────────────────────────────────────────────────┘ │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         External Dependencies                                │
│  ┌───────────────────┐  ┌───────────────────┐  ┌───────────────────────┐   │
│  │  bing-ads-go-sdk  │  │   OAuth Handler   │  │  Microsoft Bulk API   │   │
│  │                   │  │                   │  │                       │   │
│  │  - BulkService    │  │  - FetchToken     │  │  - GetBulkUploadUrl   │   │
│  │  - Session        │  │  - RefreshToken   │  │  - UploadBulkFile     │   │
│  │                   │  │                   │  │  - GetBulkUploadStatus│   │
│  └───────────────────┘  └───────────────────┘  └───────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Data Flow

```
Input (JSON-L file from Batch Router)
    │
    ▼
┌─────────────────────┐
│ Transform           │  Extracts message, adds job_id metadata
└─────────────────────┘
    │
    ▼
┌─────────────────────┐
│ Upload              │
│  ├─ Parse JSON-L    │
│  ├─ Group by Action │  (Add, Remove, Replace)
│  ├─ Create CSV      │  (Microsoft Bulk Format v6.0)
│  ├─ Create ZIP      │
│  └─ Upload to API   │  Returns ImportId(s)
└─────────────────────┘
    │
    ▼
┌─────────────────────┐
│ Poll                │  Checks status until Complete/Failed
│  └─ Returns status  │  + ResultFileUrl if errors
└─────────────────────┘
    │
    ▼
┌─────────────────────┐
│ GetUploadStats      │  Downloads result, parses errors
│  └─ Returns         │  Succeeded/Aborted job IDs
└─────────────────────┘
```

---

## 2. Manager Initialization

### 2.1 NewManager Function

**File:** `manager.go`

```go
func NewManager(
    conf *config.Config,
    logger logger.Logger,
    statsFactory stats.Stats,
    destination *backendconfig.DestinationT,
    backendConfig backendconfig.BackendConfig,
) (*BingAdsBulkUploader, error) {
    // Create OAuth v2 client with configuration
    oauthClientV2 := oauthv2.NewOAuthHandler(backendConfig,
        oauthv2.WithLogger(logger),
        oauthv2.WithCPClientTimeout(conf.GetDuration("HttpClient.oauth.timeout", 30, time.Second)),
        oauthv2.WithStats(statsFactory),
        oauthv2.WithOauthBreakerOptions(oauthv2.ConfigToOauthBreakerOptions("BatchRouter.BING_ADS", conf)),
    )
    return newManagerInternal(logger, statsFactory, destination, oauthClientV2)
}
```

### 2.2 Internal Manager Creation

```go
func newManagerInternal(
    logger logger.Logger,
    statsFactory stats.Stats,
    destination *backendconfig.DestinationT,
    oauthHandler oauthv2.OAuthHandler,
) (*BingAdsBulkUploader, error) {
    // 1. Parse destination configuration
    destConfig := DestinationConfig{}
    jsonConfig, err := jsonrs.Marshal(destination.Config)
    if err != nil {
        return nil, fmt.Errorf("error in marshalling destination config: %v", err)
    }
    err = jsonrs.Unmarshal(jsonConfig, &destConfig)
    if err != nil {
        return nil, fmt.Errorf("error in unmarshalling destination config: %v", err)
    }

    // 2. Create token source for OAuth
    token := common.TokenSource{
        WorkspaceID:        destination.WorkspaceID,
        DestinationDefName: destination.DestinationDefinition.Name,
        AccountID:          destConfig.RudderAccountID,
        OauthHandler:       oauthHandler,
        DestinationID:      destination.ID,
        CurrentTime:        time.Now,
    }

    // 3. Generate initial token
    secret, err := token.GenerateTokenV2()
    if err != nil {
        return nil, fmt.Errorf("failed to generate oauth token: %v", err)
    }

    // 4. Configure Bing Ads SDK session
    sessionConfig := bingads.SessionConfig{
        DeveloperToken: secret.DeveloperToken,
        AccountId:      destConfig.CustomerAccountID,
        CustomerId:     destConfig.CustomerID,
        HTTPClient:     http.DefaultClient,
        TokenSource:    &token,
    }
    session := bingads.NewSession(sessionConfig)

    // 5. Create and return uploader
    clientNew := Client{}
    bingUploader := NewBingAdsBulkUploader(
        logger,
        statsFactory,
        destination.DestinationDefinition.Name,
        bingads.NewBulkService(session),
        &clientNew,
    )
    return bingUploader, nil
}
```

### 2.3 BingAdsBulkUploader Structure

**File:** `types.go`

```go
type BingAdsBulkUploader struct {
    destName      string                 // Destination name for config lookups
    service       bingads.BulkServiceI   // Bing Ads Bulk API service
    logger        logger.Logger          // Logger instance
    statsFactory  stats.Stats            // Stats factory for metrics
    client        Client                 // HTTP client wrapper
    fileSizeLimit int64                  // Max file size (default: 100MB)
    eventsLimit   int64                  // Max events (default: 4,000,000)
}
```

### 2.4 Uploader Constructor

**File:** `bulk_uploader.go`

```go
func NewBingAdsBulkUploader(
    logger logger.Logger,
    statsFactory stats.Stats,
    destName string,
    service bingads.BulkServiceI,
    client *Client,
) *BingAdsBulkUploader {
    return &BingAdsBulkUploader{
        destName:      destName,
        service:       service,
        logger:        logger.Child("BingAds").Child("BingAdsBulkUploader"),
        statsFactory:  statsFactory,
        client:        *client,
        fileSizeLimit: common.GetBatchRouterConfigInt64("MaxUploadLimit", destName, 100*bytesize.MB),
        eventsLimit:   common.GetBatchRouterConfigInt64("MaxEventsLimit", destName, 4000000),
    }
}
```

---

## 3. Transform Implementation

### 3.1 Transform Function

**File:** `bulk_uploader.go`

```go
func (*BingAdsBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
    return common.GetMarshalledData(gjson.GetBytes(job.EventPayload, "body.JSON").String(), job.JobID)
}
```

### 3.2 GetMarshalledData Utility

**File:** `router/batchrouter/asyncdestinationmanager/common/common.go`

```go
func GetMarshalledData(payload string, jobID int64) (string, error) {
    var asyncJob AsyncJob
    
    // Parse the payload into message field
    err := jsonrs.Unmarshal([]byte(payload), &asyncJob.Message)
    if err != nil {
        return "", err
    }
    
    // Add metadata with job ID
    asyncJob.Metadata = make(map[string]interface{})
    asyncJob.Metadata["job_id"] = jobID
    
    // Marshal back to JSON string
    responsePayload, err := jsonrs.Marshal(asyncJob)
    if err != nil {
        return "", err
    }
    return string(responsePayload), nil
}
```

### 3.3 Transform Output Format

**Input (job.EventPayload):**
```json
{
    "body": {
        "JSON": {
            "action": "Add",
            "list": [
                {"email": "user@example.com", "hashedEmail": "abc123..."}
            ]
        }
    }
}
```

**Output (transformed line):**
```json
{
    "message": {
        "action": "Add",
        "list": [
            {"email": "user@example.com", "hashedEmail": "abc123..."}
        ]
    },
    "metadata": {
        "job_id": 12345
    }
}
```

---

## 4. Upload Implementation

### 4.1 Upload Function Overview

**File:** `bulk_uploader.go`

```go
func (b *BingAdsBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
    destination := asyncDestStruct.Destination
    var failedJobs []int64
    var successJobs []int64
    var importIds []string
    var errors []string

    // Get audience ID from config
    audienceId, _ := misc.MapLookup(destination.Config, "audienceId").(string)

    // Create ZIP files for each action type
    actionFiles, err := b.createZipFile(asyncDestStruct.FileName, audienceId)
    if err != nil {
        return common.AsyncUploadOutput{
            FailedJobIDs:  append(asyncDestStruct.FailedJobIDs, asyncDestStruct.ImportingJobIDs...),
            FailedReason:  fmt.Sprintf("got error while transforming the file. %v", err.Error()),
            FailedCount:   len(asyncDestStruct.FailedJobIDs) + len(asyncDestStruct.ImportingJobIDs),
            DestinationID: destination.ID,
        }
    }

    // Track events exceeding limits
    uploadRetryableStat := b.statsFactory.NewTaggedStat("events_over_prescribed_limit", stats.CountType, map[string]string{
        "module":   "batch_router",
        "destType": b.destName,
    })

    // Process each action file
    for _, actionFile := range actionFiles {
        uploadRetryableStat.Count(len(actionFile.FailedJobIDs))
        
        // Skip empty files
        _, err := os.Stat(actionFile.ZipFilePath)
        if err != nil || actionFile.EventCount == 0 {
            continue
        }

        // Step 1: Get upload URL
        urlResp, err := b.service.GetBulkUploadUrl()
        if err != nil {
            b.logger.Errorn("Error in getting bulk upload url", obskit.Error(err))
            failedJobs = append(append(failedJobs, actionFile.SuccessfulJobIDs...), actionFile.FailedJobIDs...)
            errors = append(errors, fmt.Sprintf("%s:error in getting bulk upload url: %s", actionFile.Action, err.Error()))
            continue
        }

        // Step 2: Upload the file
        uploadTimeStat := b.statsFactory.NewTaggedStat("async_upload_time", stats.TimerType, map[string]string{
            "module":   "batch_router",
            "destType": b.destName,
        })
        startTime := time.Now()
        uploadBulkFileResp, errorDuringUpload := b.service.UploadBulkFile(urlResp.UploadUrl, actionFile.ZipFilePath)
        uploadTimeStat.Since(startTime)

        if errorDuringUpload != nil {
            b.logger.Errorn("error in uploading the bulk file", obskit.Error(errorDuringUpload))
            failedJobs = append(append(failedJobs, actionFile.SuccessfulJobIDs...), actionFile.FailedJobIDs...)
            errors = append(errors, fmt.Sprintf("%s:error in uploading the bulk file: %v", actionFile.Action, errorDuringUpload))
            
            // Cleanup failed upload file
            err = os.Remove(actionFile.ZipFilePath)
            if err != nil {
                b.logger.Errorn("Error in removing zip file", obskit.Error(err))
            }
            continue
        }

        // Track successful upload
        importIds = append(importIds, uploadBulkFileResp.RequestId)
        failedJobs = append(failedJobs, actionFile.FailedJobIDs...)
        successJobs = append(successJobs, actionFile.SuccessfulJobIDs...)
    }

    // Build import parameters
    var parameters common.ImportParameters
    parameters.ImportId = strings.Join(importIds, commaSeparator)
    importParameters, err := jsonrs.Marshal(parameters)
    if err != nil {
        b.logger.Errorn("Errored in Marshalling parameters", obskit.Error(err))
    }
    allErrors := router_utils.EnhanceJSON([]byte(`{}`), "error", strings.Join(errors, commaSeparator))

    // Cleanup all ZIP files
    for _, actionFile := range actionFiles {
        err = os.Remove(actionFile.ZipFilePath)
        if err != nil {
            b.logger.Errorn("Error in removing zip file", obskit.Error(err))
        }
    }

    return common.AsyncUploadOutput{
        ImportingJobIDs:     successJobs,
        FailedJobIDs:        append(asyncDestStruct.FailedJobIDs, failedJobs...),
        FailedReason:        string(allErrors),
        ImportingParameters: importParameters,
        ImportingCount:      len(successJobs),
        FailedCount:         len(asyncDestStruct.FailedJobIDs) + len(failedJobs),
        DestinationID:       destination.ID,
    }
}
```

### 4.2 CreateZipFile Function

**File:** `util.go`

```go
func (b *BingAdsBulkUploader) createZipFile(filePath, audienceId string) ([]*ActionFileInfo, error) {
    if audienceId == "" {
        return nil, fmt.Errorf("audienceId is empty")
    }

    textFile, err := os.Open(filePath)
    if err != nil {
        return nil, err
    }
    defer textFile.Close()

    // Create action files for each type: Replace, Remove, Add
    actionFiles := map[string]*ActionFileInfo{}
    for _, actionType := range actionTypes {  // actionTypes = [3]string{"Replace", "Remove", "Add"}
        actionFiles[actionType], err = createActionFile(audienceId, actionType)
        if err != nil {
            return nil, err
        }
    }

    // Read and process each line
    scanner := bufio.NewScanner(textFile)
    scanner.Buffer(nil, 50000*1024)  // 50MB buffer for large lines
    
    for scanner.Scan() {
        line := scanner.Text()
        var data Data
        if err := jsonrs.Unmarshal([]byte(line), &data); err != nil {
            return nil, err
        }

        // Track payload size
        payloadSizeStat := b.statsFactory.NewTaggedStat("payload_size", stats.HistogramType,
            map[string]string{
                "module":   "batch_router",
                "destType": b.destName,
            })
        payloadSizeStat.Observe(float64(len(data.Message.List)))

        // Add to appropriate action file
        actionFile := actionFiles[data.Message.Action]
        err := b.populateZipFile(actionFile, audienceId, line, data)
        if err != nil {
            return nil, err
        }
    }

    if scannerErr := scanner.Err(); scannerErr != nil {
        return nil, scannerErr
    }

    // Convert CSVs to ZIPs and collect results
    actionFilesList := []*ActionFileInfo{}
    for _, actionType := range actionTypes {
        actionFile := actionFiles[actionType]
        actionFile.CSVWriter.Flush()
        
        err := convertCsvToZip(actionFile)
        if err != nil {
            actionFile.FailedJobIDs = append(actionFile.FailedJobIDs, actionFile.SuccessfulJobIDs...)
            actionFile.SuccessfulJobIDs = []int64{}
        }
        
        if actionFile.EventCount > 0 {
            actionFilesList = append(actionFilesList, actionFile)
        }
    }
    
    return actionFilesList, nil
}
```

### 4.3 PopulateZipFile Function

```go
func (b *BingAdsBulkUploader) populateZipFile(actionFile *ActionFileInfo, audienceId, line string, data Data) error {
    newFileSize := actionFile.FileSize + int64(len(line))
    
    // Check limits
    if newFileSize < b.fileSizeLimit && actionFile.EventCount < b.eventsLimit {
        actionFile.FileSize = newFileSize
        actionFile.EventCount += 1
        
        // Write each email as a CSV row
        for _, uploadData := range data.Message.List {
            clientIdI := newClientID(data.Metadata.JobID, uploadData.HashedEmail)
            clientIdStr := clientIdI.ToString()
            
            err := actionFile.CSVWriter.Write([]string{
                "Customer List Item",           // Type
                "",                             // Status
                "",                             // Id
                audienceId,                     // Parent Id
                clientIdStr,                    // Client Id (jobId<<>>hashedEmail)
                "",                             // Modified Time
                "",                             // Name
                "",                             // Description
                "",                             // Scope
                "",                             // Audience
                "",                             // Action Type
                "Email",                        // Sub Type
                uploadData.HashedEmail,         // Text (the hashed email)
            })
            if err != nil {
                return err
            }
        }
        actionFile.SuccessfulJobIDs = append(actionFile.SuccessfulJobIDs, data.Metadata.JobID)
    } else {
        // Event exceeds limits, mark as failed for retry
        actionFile.FailedJobIDs = append(actionFile.FailedJobIDs, data.Metadata.JobID)
    }
    return nil
}
```

---

## 5. Poll Implementation

### 5.1 Poll Function

**File:** `bulk_uploader.go`

```go
func (b *BingAdsBulkUploader) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
    var cumulativeResp common.PollStatusResponse
    var completionStatus []bool
    var failedJobURLs []string
    var pollStatusCode []int
    var cumulativeProgressStatus, cumulativeFailureStatus bool
    var cumulativeStatusCode int

    // Split comma-separated import IDs
    requestIdsArray := lo.Reject(strings.Split(pollInput.ImportId, commaSeparator), func(url string, _ int) bool {
        return url == ""
    })

    // Poll each import
    for _, requestId := range requestIdsArray {
        resp := b.pollSingleImport(requestId)
        
        pollStatusCode = append(pollStatusCode, resp.StatusCode)
        completionStatus = append(completionStatus, resp.Complete)
        failedJobURLs = append(failedJobURLs, resp.FailedJobParameters)
        cumulativeProgressStatus = cumulativeProgressStatus || resp.InProgress
        cumulativeFailureStatus = cumulativeFailureStatus || resp.HasFailed
    }

    // Determine cumulative status code
    if lo.Contains(pollStatusCode, 500) {
        cumulativeStatusCode = 500
    } else {
        cumulativeStatusCode = 200
    }

    // Build cumulative response
    cumulativeResp = common.PollStatusResponse{
        Complete:            !lo.Contains(completionStatus, false),
        InProgress:          cumulativeProgressStatus,
        StatusCode:          cumulativeStatusCode,
        HasFailed:           cumulativeFailureStatus,
        FailedJobParameters: strings.Join(failedJobURLs, commaSeparator),
    }

    return cumulativeResp
}
```

### 5.2 PollSingleImport Function

```go
func (b *BingAdsBulkUploader) pollSingleImport(requestId string) common.PollStatusResponse {
    uploadStatusResp, err := b.service.GetBulkUploadStatus(requestId)
    if err != nil {
        return common.PollStatusResponse{
            StatusCode: 500,
            HasFailed:  true,
        }
    }

    switch uploadStatusResp.RequestStatus {
    case "Completed":
        return common.PollStatusResponse{
            Complete:   true,
            StatusCode: 200,
        }
    case "CompletedWithErrors":
        return common.PollStatusResponse{
            Complete:            true,
            StatusCode:          200,
            HasFailed:           true,
            FailedJobParameters: uploadStatusResp.ResultFileUrl,
        }
    case "FileUploaded", "InProgress", "PendingFileUpload":
        return common.PollStatusResponse{
            InProgress: true,
            StatusCode: 200,
        }
    default:  // "Failed" or unknown
        return common.PollStatusResponse{
            HasFailed:  true,
            StatusCode: 500,
        }
    }
}
```

### 5.3 Poll Response Logic Table

| Scenario | Complete | InProgress | HasFailed | StatusCode |
|----------|----------|------------|-----------|------------|
| All Completed | true | false | false | 200 |
| All CompletedWithErrors | true | false | true | 200 |
| Any InProgress | false | true | varies | 200 |
| Any Failed | false | varies | true | 500 |
| Mixed Success/Fail | varies | varies | true | 500 |

---

## 6. GetUploadStats Implementation

### 6.1 GetUploadStats Function

**File:** `bulk_uploader.go`

```go
func (b *BingAdsBulkUploader) GetUploadStats(uploadStatsInput common.GetUploadStatsInput) common.GetUploadStatsResponse {
    importList := uploadStatsInput.ImportingList
    
    // Build initial event list from importing jobs
    var initialEventList []int64
    for _, job := range importList {
        initialEventList = append(initialEventList, job.JobID)
    }

    eventStatsResponse := common.GetUploadStatsResponse{}
    var abortedJobIDs []int64
    var cumulativeAbortedReasons map[int64]string

    // Process each result file URL
    fileURLs := lo.Reject(strings.Split(uploadStatsInput.FailedJobParameters, commaSeparator), func(url string, _ int) bool {
        return url == ""
    })

    for _, fileURL := range fileURLs {
        // Download and extract result file
        filePaths, err := b.downloadAndGetUploadStatusFile(fileURL)
        if err != nil {
            b.logger.Errorn("Error in downloading and unzipping the file", obskit.Error(err))
            return common.GetUploadStatsResponse{
                StatusCode: 500,
            }
        }

        // Parse the result file
        response, err := b.getUploadStatsOfSingleImport(filePaths[0])
        if err != nil {
            b.logger.Errorn("Error in getting upload stats of single import", obskit.Error(err))
            return common.GetUploadStatsResponse{
                StatusCode: 500,
            }
        }

        // Accumulate results
        cumulativeAbortedReasons = lo.Assign(cumulativeAbortedReasons, response.Metadata.AbortedReasons)
        abortedJobIDs = append(abortedJobIDs, response.Metadata.AbortedKeys...)
        eventStatsResponse.StatusCode = response.StatusCode
    }

    // Calculate succeeded jobs by difference
    eventStatsResponse.Metadata = common.EventStatMeta{
        AbortedKeys:    abortedJobIDs,
        AbortedReasons: cumulativeAbortedReasons,
        SucceededKeys:  getSuccessJobIDs(abortedJobIDs, initialEventList),
    }

    return eventStatsResponse
}
```

### 6.2 GetUploadStatsOfSingleImport Function

```go
func (b *BingAdsBulkUploader) getUploadStatsOfSingleImport(filePath string) (common.GetUploadStatsResponse, error) {
    // Read CSV records
    records, err := b.readPollResults(filePath)
    if err != nil {
        return common.GetUploadStatsResponse{}, err
    }

    // Process error records
    clientIDErrors, err := processPollStatusData(records)
    if err != nil {
        return common.GetUploadStatsResponse{}, err
    }

    eventStatsResponse := common.GetUploadStatsResponse{
        StatusCode: 200,
        Metadata: common.EventStatMeta{
            AbortedKeys:    lo.Keys(clientIDErrors),
            AbortedReasons: getAbortedReasons(clientIDErrors),
        },
    }

    // Emit metrics
    eventsAbortedStat := b.statsFactory.NewTaggedStat("failed_job_count", stats.CountType, map[string]string{
        "module":   "batch_router",
        "destType": b.destName,
    })
    eventsAbortedStat.Count(len(eventStatsResponse.Metadata.AbortedKeys))

    eventsSuccessStat := b.statsFactory.NewTaggedStat("success_job_count", stats.CountType, map[string]string{
        "module":   "batch_router",
        "destType": b.destName,
    })
    eventsSuccessStat.Count(len(eventStatsResponse.Metadata.SucceededKeys))

    return eventStatsResponse, nil
}
```

### 6.3 ProcessPollStatusData Function

**File:** `util.go`

```go
func processPollStatusData(records [][]string) (map[int64]map[string]struct{}, error) {
    clientIDIndex := -1
    errorIndex := -1
    typeIndex := 0

    // Find column indices from header
    if len(records) > 0 {
        header := records[0]
        for i, column := range header {
            switch column {
            case "Client Id":
                clientIDIndex = i
            case "Error":
                errorIndex = i
            }
        }
    }

    clientIDErrors := make(map[int64]map[string]struct{})

    // Process data rows, filtering for error rows
    for _, record := range records[1:] {
        rowname := record[typeIndex]
        
        // Only process "Customer List Item Error" rows
        if typeIndex < len(record) && strings.Contains(rowname, "Customer List Item Error") {
            if clientIDIndex >= 0 && clientIDIndex < len(record) {
                // Parse client ID (format: jobId<<>>hashedEmail)
                clientId, err := newClientIDFromString(record[clientIDIndex])
                if err != nil {
                    return nil, err
                }

                // Add error to job's error set
                errorSet, ok := clientIDErrors[clientId.JobID]
                if !ok {
                    errorSet = make(map[string]struct{})
                    clientIDErrors[clientId.JobID] = errorSet
                }
                errorSet[record[errorIndex]] = struct{}{}
            }
        }
    }

    return clientIDErrors, nil
}
```

---

## 7. Utility Functions

### 7.1 ClientID Management

```go
// ClientID struct for tracking job-email pairs
type ClientID struct {
    JobID       int64
    HashedEmail string
}

const clientIDSeparator = "<<>>"

// Create new ClientID
func newClientID(jobID int64, hashedEmail string) ClientID {
    return ClientID{
        JobID:       jobID,
        HashedEmail: hashedEmail,
    }
}

// Convert to string for CSV
func (c *ClientID) ToString() string {
    return fmt.Sprintf("%d%s%s", c.JobID, clientIDSeparator, c.HashedEmail)
}

// Parse from string
func newClientIDFromString(clientID string) (*ClientID, error) {
    clientIDParts := strings.Split(clientID, clientIDSeparator)
    if len(clientIDParts) != 2 {
        return nil, fmt.Errorf("invalid client id: %s", clientID)
    }
    jobID, err := strconv.ParseInt(clientIDParts[0], 10, 64)
    if err != nil {
        return nil, fmt.Errorf("invalid job id in clientId: %s", clientID)
    }
    return &ClientID{
        JobID:       jobID,
        HashedEmail: clientIDParts[1],
    }, nil
}
```

### 7.2 Action File Template

```go
func CreateActionFileTemplate(csvFile *os.File, audienceId, actionType string) (*csv.Writer, error) {
    csvWriter := csv.NewWriter(csvFile)
    err := csvWriter.WriteAll([][]string{
        // Header row
        {"Type", "Status", "Id", "Parent Id", "Client Id", "Modified Time", "Name", "Description", "Scope", "Audience", "Action Type", "Sub Type", "Text"},
        // Format version row
        {"Format Version", "", "", "", "", "", "6.0", "", "", "", "", "", ""},
        // Customer List definition row
        {"Customer List", "", audienceId, "", "", "", "", "", "", "", actionType, "", ""},
    })
    if err != nil {
        return nil, fmt.Errorf("error in writing csv header: %v", err)
    }
    return csvWriter, nil
}
```

### 7.3 ZIP File Operations

```go
// Convert CSV to ZIP
func convertCsvToZip(actionFile *ActionFileInfo) error {
    if actionFile.EventCount == 0 {
        os.Remove(actionFile.CSVFilePath)
        os.Remove(actionFile.ZipFilePath)
        return nil
    }

    zipFile, err := os.Create(actionFile.ZipFilePath)
    if err != nil {
        return err
    }
    defer zipFile.Close()

    zipWriter := zip.NewWriter(zipFile)
    csvFileInZip, err := zipWriter.Create(filepath.Base(actionFile.CSVFilePath))
    if err != nil {
        return err
    }

    csvFile, err := os.Open(actionFile.CSVFilePath)
    if err != nil {
        return err
    }
    csvFile.Seek(0, 0)

    if _, err = io.Copy(csvFileInZip, csvFile); err != nil {
        return err
    }

    if err = zipWriter.Close(); err != nil {
        return err
    }

    return os.Remove(actionFile.CSVFilePath)
}
```

---

## 8. Error Handling Patterns

### 8.1 OAuth Token Errors

```go
// In manager.go
secret, err := token.GenerateTokenV2()
if err != nil {
    return nil, fmt.Errorf("failed to generate oauth token: %v", err)
}
```

### 8.2 Upload Errors

```go
// Errors are collected per action file
if errorDuringUpload != nil {
    b.logger.Errorn("error in uploading the bulk file", obskit.Error(errorDuringUpload))
    failedJobs = append(append(failedJobs, actionFile.SuccessfulJobIDs...), actionFile.FailedJobIDs...)
    errors = append(errors, fmt.Sprintf("%s:error in uploading the bulk file: %v", actionFile.Action, errorDuringUpload))
    continue  // Process next action file
}
```

### 8.3 File Size Exceeded

```go
// Events exceeding limits go to FailedJobIDs for retry
if newFileSize >= b.fileSizeLimit || actionFile.EventCount >= b.eventsLimit {
    actionFile.FailedJobIDs = append(actionFile.FailedJobIDs, data.Metadata.JobID)
}
```

---

## 9. File Format Specifications

### 9.1 Input JSON-L Format

```json
{
    "message": {
        "action": "Add|Remove|Replace",
        "list": [
            {
                "email": "plaintext@email.com",
                "hashedEmail": "sha256_hash_of_lowercase_email"
            }
        ]
    },
    "metadata": {
        "job_id": 12345
    }
}
```

### 9.2 Output CSV Format (Microsoft Bulk v6.0)

```csv
Type,Status,Id,Parent Id,Client Id,Modified Time,Name,Description,Scope,Audience,Action Type,Sub Type,Text
Format Version,,,,,,6.0,,,,,,
Customer List,,{audienceId},,,,,,,,{actionType},,
Customer List Item,,,{audienceId},{jobId}<<>>{hash},,,,,,,,Email,{hashedEmail}
Customer List Item,,,{audienceId},{jobId}<<>>{hash},,,,,,,,Email,{hashedEmail}
```

### 9.3 Result CSV Format (Error Rows)

```csv
Type,...,Client Id,...,Error,...
Customer List Item Error,...,{jobId}<<>>{hash},...,EmailMustBeHashed,...
Customer List Item Error,...,{jobId}<<>>{hash},...,InvalidCustomerListId,...
```

---

## Related Documentation

- [README.md](../README.md) - Main integration documentation
- [Business Logic](businesslogic.md) - Data flow and mapping details
