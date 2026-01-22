# Bing Ads Audience - Async Destination Manager

## Table of Contents

- [1. Integration Overview](#1-integration-overview)
- [2. Integration Functionalities](#2-integration-functionalities)
- [3. Implementation Details](#3-implementation-details)
- [4. API Reference](#4-api-reference)
- [5. Rate Limits and Constraints](#5-rate-limits-and-constraints)
- [6. Data Flow and Event Ordering](#6-data-flow-and-event-ordering)
- [7. Error Handling and Retry Logic](#7-error-handling-and-retry-logic)
- [8. Testing and Validation](#8-testing-and-validation)
- [9. Configuration Guide](#9-configuration-guide)
- [10. Advanced Features](#10-advanced-features)
- [11. Monitoring and Observability](#11-monitoring-and-observability)
- [12. Migration and Deprecation](#12-migration-and-deprecation)
- [13. Known Limitations](#13-known-limitations)
- [14. Troubleshooting Guide](#14-troubleshooting-guide)
- [15. FAQ Section](#15-faq-section)

---

## 1. Integration Overview

### 1.1 Basic Information

| Property | Value |
|----------|-------|
| **Destination Name** | BingAds Audience |
| **Internal Name** | `BINGADS_AUDIENCE` |
| **Integration Type** | Async Destination Manager (Batch Router) |
| **Implementation Language** | Go |
| **Primary Use Case** | Customer List / Audience Sync for retargeting |
| **Status** | Generally Available (Beta for Reverse ETL) |

### 1.2 Repository Locations

```
- Implementation: rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/audience/
- Configuration: rudder-integrations-config/src/configurations/destinations/bingads_audience/
- Authentication: rudder-auth/src/routes/auth/bingads_audience.ts
- SDK: github.com/rudderlabs/bing-ads-go-sdk
```

### 1.3 Key Files

| File | Purpose |
|------|---------|
| `manager.go` | Main entry point, creates BingAdsBulkUploader instance |
| `bulk_uploader.go` | Core business logic for Upload, Poll, GetUploadStats |
| `types.go` | Data structures and configuration types |
| `util.go` | Helper functions for file processing and polling |
| `common/token.go` | OAuth token management |

---

## 2. Integration Functionalities

### 2.1 Destination Configuration

Source: `rudder-integrations-config/src/configurations/destinations/bingads_audience/schema.json`

| Field Name | Type | Required | Pattern/Validation | Description |
|------------|------|----------|-------------------|-------------|
| `rudderAccountId` | string | Yes | - | RudderStack OAuth account identifier for token management |
| `customerAccountId` | string | Yes | `^[0-9]+$` or env variable | Bing Ads Customer Account ID (found as `aid` in URL) |
| `customerId` | string | Yes | `^[0-9]+$` or env variable | Bing Ads Customer ID (found as `cid` in URL) |
| `audienceId` | string | Yes | `^[0-9]+$` or env variable | Target Customer List audience ID |
| `hashEmail` | boolean | No | Default: `true` | Whether RudderStack should hash emails before sending |

**Usage in Code:**
```go
// manager.go
destConfig := DestinationConfig{}
jsonConfig, err := jsonrs.Marshal(destination.Config)
err = jsonrs.Unmarshal(jsonConfig, &destConfig)

// Accessed fields:
destConfig.CustomerAccountID  // For API session
destConfig.CustomerID         // For API session
destConfig.RudderAccountID    // For OAuth token
```

### 2.2 Implementation Pattern

**Manager Type:** Full AsyncDestinationManager

**Interface Methods Implemented:**
| Method | Implemented | Description |
|--------|-------------|-------------|
| `Transform(job *jobsdb.JobT) (string, error)` | ✅ | Extracts payload and marshals with job metadata |
| `Upload(asyncDestStruct *AsyncDestinationStruct) AsyncUploadOutput` | ✅ | Creates CSV/ZIP files and uploads via Bulk API |
| `Poll(pollInput AsyncPoll) PollStatusResponse` | ✅ | Polls upload status for completion |
| `GetUploadStats(input GetUploadStatsInput) GetUploadStatsResponse` | ✅ | Downloads result file and parses success/failure stats |

### 2.3 Supported Message Types and Source Types

Source: `rudder-integrations-config/src/configurations/destinations/bingads_audience/db-config.json`

**Supported Source Types:**
- `cloud`
- `warehouse`
- `shopify`

**Supported Message Types:**
| Source Type | Message Types |
|-------------|---------------|
| `cloud` | `audiencelist` |

**Connection Modes:**
| Source Type | Supported Modes |
|-------------|-----------------|
| `cloud` | `cloud` |
| `warehouse` | `cloud` |
| `shopify` | `cloud` |

### 2.4 Authentication

**Auth Type:** OAuth 2.0

**OAuth Configuration:**
| Property | Value |
|----------|-------|
| Authorization URL | `https://login.microsoftonline.com/common/oauth2/v2.0/authorize` |
| Token URL | `https://login.microsoftonline.com/common/oauth2/v2.0/token` |
| Scopes | `profile email openid offline_access https://ads.microsoft.com/msads.manage` |
| User Profile API | `https://clientcenter.api.bingads.microsoft.com/Api/CustomerManagement/v13/CustomerManagementService.svc` |

**Token Refresh Strategy:**
- Implemented in `rudder-auth/src/controllers/refreshTokenImplementations/bingads_bulk_upload.ts`
- Uses refresh_token grant type
- Handles `invalid_grant` errors with user-friendly messaging

**Token Usage in Upload:**
```go
// common/token.go
type SecretStruct struct {
    AccessToken    string `json:"accessToken"`
    RefreshToken   string `json:"refreshToken"`
    DeveloperToken string `json:"developer_token"`
    ExpirationDate string `json:"expirationDate"`
}

// manager.go - Used to create SDK session
sessionConfig := bingads.SessionConfig{
    DeveloperToken: secret.DeveloperToken,
    AccountId:      destConfig.CustomerAccountID,
    CustomerId:     destConfig.CustomerID,
    HTTPClient:     http.DefaultClient,
    TokenSource:    &token,
}
```

### 2.5 Supported Actions

The integration supports three action types for Customer List management:

| Action | Description |
|--------|-------------|
| `Add` | Add users to the Customer List audience |
| `Remove` | Remove users from the Customer List audience |
| `Replace` | Replace all users in the Customer List audience |

### 2.6 Batching Support

**Yes**, the integration supports batching with the following characteristics:
- Events are batched by action type (Add, Remove, Replace)
- Up to 3 separate ZIP files can be created per upload (one per action type)
- Each file is uploaded independently to the Bulk API

---

## 3. Implementation Details

### 3.1 File Processing

**Input File Format:** JSON (line-delimited/NDJSON)

**Sample Input File Structure:**
```json
{"message":{"action":"Add","list":[{"email":"user@example.com","hashedEmail":"hash1"},{"email":"user2@example.com","hashedEmail":"hash2"}]},"metadata":{"job_id":1}}
{"message":{"action":"Remove","list":[{"email":"user3@example.com","hashedEmail":"hash3"}]},"metadata":{"job_id":2}}
```

**Output File Format:** CSV compressed as ZIP

**CSV Template Structure (Microsoft Bulk API v6.0 Format):**
```csv
Type,Status,Id,Parent Id,Client Id,Modified Time,Name,Description,Scope,Audience,Action Type,Sub Type,Text
Format Version,,,,,,6.0,,,,,
Customer List,,{audienceId},,,,,,,,{actionType},,
Customer List Item,,,{audienceId},{jobId}<<>>{hashedEmail},,,,,,,,Email,{hashedEmail}
```

**File Size Limits:**
| Limit | Default Value | Config Key |
|-------|---------------|------------|
| Max Upload Size | 100 MB | `BatchRouter.BING_ADS.MaxUploadLimit` |
| Max Events | 4,000,000 | `BatchRouter.BING_ADS.MaxEventsLimit` |

### 3.2 Upload Flow

**Step-by-step process:**

1. **Configuration Parsing**
   ```go
   audienceId, _ := misc.MapLookup(destination.Config, "audienceId").(string)
   ```

2. **File Reading and Transformation**
   - Reads JSON-L file created by batch router
   - Groups events by action type (Add/Remove/Replace)
   - Creates up to 3 CSV files

3. **CSV to ZIP Conversion**
   - Each CSV file is compressed into a ZIP file
   - Empty action types are skipped

4. **API Calls (per action file):**
   ```go
   // Step 1: Get Upload URL
   urlResp, err := b.service.GetBulkUploadUrl()
   
   // Step 2: Upload ZIP file
   uploadBulkFileResp, err := b.service.UploadBulkFile(urlResp.UploadUrl, actionFile.ZipFilePath)
   ```

5. **Response Handling**
   - Import IDs from multiple uploads are joined with commas
   - Failed jobs are tracked per action file
   - ZIP files are cleaned up after upload

6. **Return Value**
   ```go
   return common.AsyncUploadOutput{
       ImportingJobIDs:     successJobs,
       FailedJobIDs:        failedJobs,
       ImportingParameters: importParameters,  // Contains comma-separated ImportIds
       ImportingCount:      len(successJobs),
       FailedCount:         len(failedJobs),
       DestinationID:       destination.ID,
   }
   ```

### 3.3 Polling Mechanism

**Polling Endpoint:** Microsoft Bulk API `GetBulkUploadStatus`

**Poll Status States:**
| Status | Meaning | Response |
|--------|---------|----------|
| `Completed` | All records processed successfully | `Complete: true` |
| `CompletedWithErrors` | Processing done with some failures | `Complete: true, HasFailed: true` |
| `FileUploaded` | File received, processing started | `InProgress: true` |
| `InProgress` | Currently processing | `InProgress: true` |
| `PendingFileUpload` | Waiting for file | `InProgress: true` |
| `Failed` | Upload completely failed | `HasFailed: true, StatusCode: 500` |

**Cumulative Response Logic (for multiple imports):**
```go
// From bulk_uploader.go
/*
    1. If any request is in progress → whole request is in progress (retry)
    2. If all requests completed → whole request is completed
    3. If any request completed with errors → completed with errors
    4. If any request failed → whole request failed and retried
    5. If all requests failed → whole request is failed
*/
```

### 3.4 Upload Statistics

**Stats Endpoint:** Downloads result file from `ResultFileUrl`

**Stats Retrieved:**
- Aborted job IDs and reasons (from error rows in result CSV)
- Succeeded job IDs (calculated by difference)

**Error Parsing Logic:**
```go
// Records with Type containing "Customer List Item Error" are parsed
// Client ID format: {jobId}<<>>{hashedEmail}
// Error messages are extracted from the "Error" column
```

**Sample Error Responses:**
| Error Code | Description |
|------------|-------------|
| `InvalidCustomerListId` | The audience ID doesn't exist or is invalid |
| `EmailMustBeHashed` | Email was not properly SHA256 hashed |
| `InvalidAudienceId` | Audience ID format is incorrect |

### 3.5 Validations

**Configuration Validations:**
- `audienceId` must not be empty (checked before file creation)

**Input Data Validations:**
- Email must be SHA256 hashed (64 character hex string)
- Action must be one of: `Add`, `Remove`, `Replace`

**Pre-upload Checks:**
- File size must be under 100MB (configurable)
- Event count must be under 4,000,000 (configurable)
- Events exceeding limits are moved to `FailedJobIDs` for retry

---

## 4. API Reference

### 4.1 Endpoints Used

**1. GetBulkUploadUrl**
| Property | Value |
|----------|-------|
| Endpoint | `https://bulk.api.bingads.microsoft.com/Api/Advertiser/CampaignManagement/v13/BulkService.svc` |
| Method | SOAP POST |
| Purpose | Obtain a pre-signed URL for file upload |
| Headers | `AuthenticationToken`, `DeveloperToken`, `CustomerId`, `AccountId` |

**Request (SOAP):**
```xml
<GetBulkUploadUrlRequest xmlns="https://bingads.microsoft.com/CampaignManagement/v13">
    <AccountId>{accountId}</AccountId>
    <ResponseMode>ErrorsOnly</ResponseMode>
</GetBulkUploadUrlRequest>
```

**Response:**
```xml
<GetBulkUploadUrlResponse>
    <UploadUrl>https://...</UploadUrl>
    <RequestId>guid</RequestId>
</GetBulkUploadUrlResponse>
```

**2. UploadBulkFile**
| Property | Value |
|----------|-------|
| Endpoint | Dynamic URL from GetBulkUploadUrl response |
| Method | POST (multipart/form-data) |
| Purpose | Upload ZIP file containing CSV data |
| Headers | `AuthenticationToken`, `DeveloperToken`, `CustomerId`, `AccountId` |

**Response:**
```json
{
    "TrackingId": "guid",
    "RequestId": "guid"
}
```

**3. GetBulkUploadStatus**
| Property | Value |
|----------|-------|
| Endpoint | `https://bulk.api.bingads.microsoft.com/Api/Advertiser/CampaignManagement/v13/BulkService.svc` |
| Method | SOAP POST |
| Purpose | Check upload processing status |

**Response:**
```xml
<GetBulkUploadStatusResponse>
    <PercentComplete>100</PercentComplete>
    <RequestStatus>Completed|CompletedWithErrors|InProgress|Failed</RequestStatus>
    <ResultFileUrl>https://...</ResultFileUrl>
</GetBulkUploadStatusResponse>
```

### 4.2 API Version

| Property | Value |
|----------|-------|
| Current Version | v13 |
| Bulk Format Version | 6.0 |
| Version Source | SDK Endpoint URL and CSV header |

**Deprecation Status:**
> **⚠️ REQUIRES REVIEW**: Microsoft Advertising API follows a deprecation policy where older API versions are deprecated ~12 months after new versions release. Monitor [Microsoft Advertising API release notes](https://learn.microsoft.com/en-us/advertising/guides/migration-guide) for version updates.

---

## 5. Rate Limits and Constraints

### 5.1 API Rate Limits

Source: [Microsoft Advertising API Documentation](https://learn.microsoft.com/en-us/advertising/guides/services-protocol)

| Limit Type | Value | Notes |
|------------|-------|-------|
| Bulk upload file size | 100 MB compressed | Configurable in RudderStack |
| Records per bulk file | 4,000,000 | Configurable in RudderStack |
| Concurrent uploads | Not officially documented | Recommend sequential uploads per account |
| API calls per minute | Not strictly enforced | Microsoft uses adaptive throttling |

### 5.2 RudderStack Configuration

| Config Key | Default | Description |
|------------|---------|-------------|
| `BatchRouter.BING_ADS.MaxUploadLimit` | 100 MB | Maximum ZIP file size |
| `BatchRouter.BING_ADS.MaxEventsLimit` | 4,000,000 | Maximum events per file |

### 5.3 Data Constraints

| Constraint | Value |
|------------|-------|
| Email format | SHA256 hashed, 64 character lowercase hex |
| Audience ID | Numeric string |
| Customer Account ID | Numeric string |
| Customer ID | Numeric string |

---

## 6. Data Flow and Event Ordering

### 6.1 Event Ordering Requirements

| Scenario | Ordering Required | Reasoning |
|----------|-------------------|-----------|
| User additions to audience | No | Each user addition is idempotent |
| User removals from audience | No | Each removal is idempotent |
| Mixed Add/Remove in single batch | Partial | Within same action type: No ordering needed. Across action types: Order matters if same user appears in both |

**Impact of Out-of-Order Delivery:**
- For the same user appearing in both Add and Remove: Final state depends on which operation completes last
- Recommendation: Use separate syncs for Add and Remove operations

### 6.2 Data Replay Feasibility

**Missing Data Replay:**
| Scenario | Feasible | Notes |
|----------|----------|-------|
| Replay old data | ✅ Yes | No time-based restrictions on audience updates |
| Replay after gap | ✅ Yes | Audiences accept historical user data |

**Re-delivery of Existing Data:**
| Scenario | Behavior |
|----------|----------|
| Duplicate Add | Ignored (user already in list) |
| Duplicate Remove | Ignored (user not in list) |
| Same email, different jobs | Processed individually |

**Deduplication:**
- Microsoft deduplicates by email hash within the Customer List
- RudderStack does not deduplicate at upload time

---

## 7. Error Handling and Retry Logic

### 7.1 Upload Errors

| Error Type | Handling | Retry |
|------------|----------|-------|
| GetBulkUploadUrl failure | All jobs in that action file marked as failed | Yes |
| UploadBulkFile failure | All jobs in that action file marked as failed | Yes |
| File creation error | All jobs marked as failed with reason | Yes |
| OAuth token error | Upload fails with auth error | Yes (token refresh) |

### 7.2 Polling Errors

| Error Type | Status Code | Retry |
|------------|-------------|-------|
| API error | 500 | Yes |
| Request status: Failed | 500 | Yes |
| Request status: Completed | 200 | No (success) |
| Request status: CompletedWithErrors | 200 | No (partial success) |

### 7.3 Error Categories

```go
// Retryable errors: Jobs go back to failed queue
FailedJobIDs: append(asyncDestStruct.FailedJobIDs, failedJobs...)

// Non-retryable errors: Jobs are aborted
AbortedKeys: []int64{...}  // From GetUploadStats when email validation fails
```

### 7.4 Partial Failures

- If one action file (e.g., Add) succeeds and another (e.g., Remove) fails:
  - Successful jobs are marked as `ImportingJobIDs`
  - Failed jobs are marked as `FailedJobIDs`
  - Each action file is processed independently

---

## 8. Testing and Validation

### 8.1 Test Coverage

**Test Files:**
- `bingads_test.go` - Main test file with comprehensive scenarios
- `bingads_suite_test.go` - Ginkgo test suite setup

**Test Scenarios Covered:**
| Scenario | Test Name |
|----------|-----------|
| Partial success upload | `TestBingAdsUploadPartialSuccessCase` |
| Failed GetBulkUploadUrl | `TestBingAdsUploadFailedGetBulkUploadUrl` |
| Empty upload URL response | `TestBingAdsUploadEmptyGetBulkUploadUrl` |
| Failed bulk file upload | `TestBingAdsUploadFailedUploadBulkFile` |
| Successful poll | `TestBingAdsPollSuccessCase` |
| Failed poll | `TestBingAdsPollFailureCase` |
| Partial failure poll | `TestBingAdsPollPartialFailureCase` |
| Pending poll status | `TestBingAdsPollPendingStatusCase` |
| Failed poll status | `TestBingAdsPollFailedStatusCase` |
| Mixed success/failure poll | `TestBingAdsPollSuccessAndFailedStatusCase` |
| Upload stats retrieval | `TestBingAdsGetUploadStats` |
| Wrong audience ID error | `TestBingAdsGetUploadStats for wrong audience Id` |
| Manager initialization | `TestNewManagerInternal` |
| No tracking ID response | `TestBingAdsUploadNoTrackingId` |

### 8.2 Test Data Location

```
testdata/
├── uploadData.txt                                              # Sample input file
├── status-check.zip                                            # Sample result file
└── BulkUpload-02-28-2024-c7a38716-4d65-44a7-bf28-...-Results.csv  # Error result sample
```

### 8.3 Running Tests

```bash
# Run all tests for bing-ads audience
go test ./router/batchrouter/asyncdestinationmanager/bing-ads/audience/... -v

# Run specific test
go test ./router/batchrouter/asyncdestinationmanager/bing-ads/audience/... -v -run TestBingAdsUploadPartialSuccessCase

# Run with race detection
go test ./router/batchrouter/asyncdestinationmanager/bing-ads/audience/... -race
```

---

## 9. Configuration Guide

### 9.1 Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `BatchRouter.BING_ADS.MaxUploadLimit` | Maximum file size for upload | 100 MB |
| `BatchRouter.BING_ADS.MaxEventsLimit` | Maximum events per upload | 4,000,000 |
| `HttpClient.oauth.timeout` | OAuth HTTP client timeout | 30 seconds |

### 9.2 Destination Config in RudderStack UI

**Required Fields:**
1. **Account Settings** - OAuth connection to Bing Ads
2. **Customer Account ID** - From Microsoft Advertising URL (`aid` parameter)
3. **Customer ID** - From Microsoft Advertising URL (`cid` parameter)
4. **Audience ID** - From Tools > Audiences in Microsoft Advertising

**Optional Fields:**
1. **Hash Email** - Default: enabled. Disable if sending pre-hashed emails

**Finding Credentials:**
1. Log into Microsoft Advertising
2. Navigate to Campaigns tab
3. Look at URL: `https://ui.ads.microsoft.com/campaign/Campaigns.m?cid={customerId}&aid={customerAccountId}...`
4. For Audience ID: Tools > Audiences > Select your Customer List

---

## 10. Advanced Features

### 10.1 Visual Mapper Support

From `db-config.json`:
```json
"supportsVisualMapper": true
```

Visual Mapper allows mapping warehouse columns to Bing Ads fields through the RudderStack UI.

### 10.2 Sync Behaviors

```json
"syncBehaviours": ["mirror"]
```

**Mirror Mode:** Syncs the complete state of the source to the destination, adding new users and removing users no longer in source.

### 10.3 Audience Support

```json
"isAudienceSupported": true
```

The destination is specifically designed for audience/customer list management.

### 10.4 CDK v2 Enabled

```json
"cdkV2Enabled": true
```

Uses CDK v2 for transformer processing before batch router.

### 10.5 Save Destination Response

```json
"saveDestinationResponse": true
```

Destination responses are persisted for debugging and audit purposes.

---

## 11. Monitoring and Observability

### 11.1 Metrics Emitted

| Metric Name | Type | Tags | Description |
|-------------|------|------|-------------|
| `events_over_prescribed_limit` | Counter | `module: batch_router`, `destType: BING_ADS` | Events exceeding file/event limits |
| `async_upload_time` | Timer | `module: batch_router`, `destType: BING_ADS` | Time taken for bulk file upload |
| `payload_size` | Histogram | `module: batch_router`, `destType: BING_ADS` | Number of emails in each event |
| `failed_job_count` | Counter | `module: batch_router`, `destType: BING_ADS` | Jobs aborted after processing |
| `success_job_count` | Counter | `module: batch_router`, `destType: BING_ADS` | Successfully processed jobs |

### 11.2 Logging

**Key Log Messages:**
| Level | Message | Context |
|-------|---------|---------|
| Error | "Error in getting bulk upload url" | OAuth or API connectivity issues |
| Error | "error in uploading the bulk file" | File upload failures |
| Error | "Error in removing zip file" | Cleanup failures |
| Error | "Error downloading zip file" | Result file download failures |
| Error | "Error opening the CSV file" | Result file parsing issues |

### 11.3 Health Checks

- OAuth token validity is checked before each upload
- Bulk API connectivity is validated during `GetBulkUploadUrl` call
- Poll mechanism provides ongoing status updates

---

## 12. Migration and Deprecation

### 12.1 Current API Version

| Component | Version |
|-----------|---------|
| Microsoft Advertising Bulk API | v13 |
| Bulk File Format | 6.0 |
| Customer Management API | v13 |

### 12.2 Deprecation Timeline

> **⚠️ REQUIRES REVIEW**: Microsoft typically provides 12-month notice before deprecating API versions. Monitor the [Microsoft Advertising API Migration Guide](https://learn.microsoft.com/en-us/advertising/guides/migration-guide) for updates.

**Current Status (as of documentation):**
- v13 is the current production version
- No announced deprecation date

### 12.3 Version Upgrade Considerations

When upgrading API versions:
1. Review bulk file format changes
2. Update SDK endpoint URLs
3. Test with non-production accounts
4. Verify CSV column mappings

---

## 13. Known Limitations

### 13.1 API Limitations

| Limitation | Description |
|------------|-------------|
| Single audience per destination | Each destination config targets one Customer List |
| Email-only identification | Only email (hashed) is supported as user identifier |
| No phone number support | Microsoft Customer List doesn't support phone matching |
| Async processing | Results are not immediate; requires polling |

### 13.2 Implementation Limitations

| Limitation | Description |
|------------|-------------|
| No incremental updates within job | Each job is atomic; partial job success not possible |
| Sequential action processing | Add, Remove, Replace files are created and uploaded separately |
| No real-time rate limit handling | If rate limited, jobs fail and retry |

### 13.3 Data Constraints

| Constraint | Limit |
|------------|-------|
| Email must be SHA256 hashed | Required by Microsoft |
| Maximum 4M records per upload | Configurable but API limited |
| Maximum 100MB file size | Configurable but API limited |

---

## 14. Troubleshooting Guide

### 14.1 Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `unable to get bulk upload url, check your credentials` | OAuth token invalid/expired | Re-authorize the destination in RudderStack |
| `unable to upload bulk file, check your credentials` | Token or permissions issue | Verify account permissions in Microsoft Advertising |
| `InvalidCustomerListId` | Audience ID doesn't exist | Verify audience ID in Microsoft Advertising |
| `EmailMustBeHashed` | Unhashed email sent | Enable "Hash Email" in destination config |
| `audienceId is empty` | Missing config | Configure audience ID in destination settings |

### 14.2 Debugging Steps

1. **Enable debug logging:**
   ```bash
   export RUDDER_LOG_LEVEL=debug
   ```

2. **Verify OAuth connection:**
   - Check destination status in RudderStack dashboard
   - Re-authorize if showing errors

3. **Test connectivity:**
   - Ensure network allows outbound to `*.bingads.microsoft.com`
   - Check firewall rules for HTTPS (443)

4. **Validate configuration:**
   - Verify Customer Account ID matches Microsoft Advertising
   - Verify Customer ID matches Microsoft Advertising
   - Verify Audience ID exists and is a Customer List type

### 14.3 Support Resources

| Resource | URL |
|----------|-----|
| RudderStack Docs | https://www.rudderstack.com/docs/destinations/reverse-etl-destinations/bing-ads-audience/ |
| Microsoft Advertising API | https://learn.microsoft.com/en-us/advertising/guides/ |
| Microsoft Bulk API Reference | https://learn.microsoft.com/en-us/advertising/bulk-service/bulk-service-reference |

---

## 15. FAQ Section

### Q1: Can I target multiple audiences with one destination?
**A:** No, each destination configuration targets exactly one Customer List (audience). Create multiple destinations for multiple audiences.

### Q2: What happens if the same email appears in both Add and Remove arrays?
**A:** They are processed in separate bulk uploads. The final state depends on which upload completes last. Avoid sending the same email in both arrays within the same sync.

### Q3: How long does it take for audience changes to reflect in Microsoft Advertising?
**A:** Bulk uploads are processed asynchronously. Typically:
- Small files (<1000 records): 1-5 minutes
- Large files: Can take up to several hours
The Poll mechanism tracks this progress.

### Q4: Why are my emails showing as "EmailMustBeHashed" errors?
**A:** Microsoft requires SHA256 hashed lowercase emails. Ensure:
1. "Hash Email" is enabled in destination config (if sending plaintext)
2. If pre-hashing, use: `SHA256(lowercase(email))`

### Q5: Can I use this destination for Search Remarketing Lists?
**A:** Yes, Customer Lists can be used for Search remarketing, Audience Ads, and Display campaigns within Microsoft Advertising.

### Q6: What's the difference between Customer List and other audience types?
**A:** Customer Lists allow you to upload your own customer data (emails). Other audience types (In-market, Custom, etc.) are Microsoft-managed segments based on user behavior.

### Q7: How do I handle rate limiting?
**A:** Microsoft uses adaptive throttling. If you encounter rate limits:
1. Jobs will fail and retry automatically via batch router
2. Consider reducing batch frequency in source configuration
3. Monitor `failed_job_count` metric for patterns

---

## Related Documentation

- [Implementation Details](docs/implementation.md) - Detailed code walkthrough
- [Business Logic](docs/businesslogic.md) - Data flow and mapping details
- [RudderStack Bing Ads Audience Docs](https://www.rudderstack.com/docs/destinations/reverse-etl-destinations/bing-ads-audience/)
- [Microsoft Advertising API Documentation](https://learn.microsoft.com/en-us/advertising/guides/)
