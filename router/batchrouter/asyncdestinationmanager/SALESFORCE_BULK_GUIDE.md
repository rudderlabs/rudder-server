# RFC: Salesforce Bulk Upload Destination

**Author**: Josh Etsenake (Hercules Team @ Fullscript)  
**Type**: Open Source Contribution  
**Status**: Draft  
**Created**: October 1, 2025

---

**Context**: This RFC proposes a new destination implementation as an open source contribution from the Hercules team at Fullscript, a RudderStack enterprise customer. We're interested in implementing this feature and contributing it back to the RudderStack community.

## Table of Contents
- [Summary](#summary)
- [Motivation](#motivation)
- [Why Bulk API 2.0?](#why-bulk-api-20)
- [Why OAuth 2.0?](#why-oauth-20)
- [Technical Design](#technical-design)
- [Architecture](#architecture)
- [Implementation Plan](#implementation-plan)
- [Code Structure & Examples](#code-structure--examples)
- [Testing Strategy](#testing-strategy)
- [Configuration & Deployment](#configuration--deployment)
- [References](#references)

---

## Summary

We (Hercules team at Fullscript) propose implementing a **Salesforce Bulk Upload** destination as an open source contribution to RudderStack. This destination will use the existing AsyncDestinationManager framework to enable high-volume bulk synchronization of customer data to Salesforce objects, primarily for **RETL (Reverse ETL) warehouse syncs**.

## Motivation

### Problem We're Solving

At Fullscript, we need to sync large volumes of data (100K-500K+ records) from our data warehouse to Salesforce. The existing real-time Salesforce destination in rudder-transformer uses the REST API for per-event syncs, which is inefficient for our use case:

- High API call volume (one call per record)
- Rate limiting issues at scale
- Poor throughput for bulk operations
- Increased API usage costs

### Proposed Solution

Implement a Salesforce Bulk Upload destination that:
- Uses Salesforce Bulk API 2.0 for batch operations
- Handles RETL warehouse syncs efficiently
- Supports high-volume event streams
- Leverages RudderStack's existing AsyncDestinationManager framework
- Follows established patterns (Marketo Bulk Upload, Bing Ads)

### Why We're Contributing This

1. **We need it**: Fullscript has this use case in production
2. **Community benefit**: Other RudderStack customers likely need this too
3. **Open source alignment**: RudderStack's async destination framework makes this straightforward
4. **Knowledge sharing**: We can leverage RudderStack team's expertise while contributing back

### Why AsyncDestinationManager?

The Salesforce Bulk API 2.0 workflow aligns perfectly with our AsyncDestinationManager pattern:

```
Salesforce Bulk API Flow:
Create Job → Upload CSV → Close Job → Poll Status → Get Results

AsyncDestinationManager Flow:
Transform → Upload → Poll → GetUploadStats
```

### Existing Reference Implementations

RudderStack has similar implementations that prove this pattern works:
- **Marketo Bulk Upload**: CSV generation, polling, client credentials auth
- **Eloqua**: Multi-step job creation, field mapping
- **Bing Ads**: OAuth 2.0 integration with RudderStack's OAuth v2 service

### Key Architectural Decision: Transformer Reuse

**We can reuse the existing Salesforce transformer instead of creating a new one.** This is already a proven pattern in RudderStack:

```javascript
// rudder-transformer/src/constants/destinationCanonicalNames.js
const DestHandlerMap = {
  salesforce_oauth: 'salesforce',         // ← Multiple destination types
  salesforce_oauth_sandbox: 'salesforce', // ← share same transformer
  salesforce_bulk_upload: 'salesforce',   // ← We just add this!
};
```

**Why this approach:**
- ✅ Existing Salesforce transformer already has RETL/VDM support (`mappedToDestination` logic)
- ✅ Zero new transformer code needed
- ✅ Users get visual field mapping UI (not manual typing)
- ✅ Consistent UX with regular Salesforce destination
- ✅ Future transformer improvements benefit both destinations

**Note**: Some destinations like Marketo have separate transformers for their bulk variants. This appears to be a legacy pattern from before VDM support was widespread. We'll take advantage of the existing Salesforce transformer's VDM capabilities instead.

**Comparison of Approaches:**

| Aspect | Marketo Pattern (Separate) | Our Approach (Reuse) |
|--------|---------------------------|----------------------|
| Transformer Code | New transformer needed | Reuse existing ✅ |
| Lines of Code | +79 lines JavaScript | +1 line alias ✅ |
| VDM Support | No ❌ | Yes ✅ |
| Field Mapping UI | Manual typing | Visual mapper ✅ |
| RETL Experience | Requires knowing field names | Auto-suggested fields ✅ |
| Maintenance | Two transformers | One transformer ✅ |
| Future Updates | Must update both | Automatic ✅ |

By reusing the existing Salesforce transformer, we get a significantly better user experience for RETL field mapping while writing less code.

### Use Cases

1. **RETL Warehouse Syncs (Primary)**
   - Sync 100K-500K+ records from Snowflake/BigQuery to Salesforce
   - Automated scheduled syncs
   - Field mapping from warehouse tables to Salesforce objects

2. **High-Volume Event Streams (Secondary)**
   - SaaS products generating 50K+ events/day
   - Too much volume for real-time Salesforce REST API
   - Batched async upload for efficiency

### Scope of Work

| Phase | Description |
|-------|-------------|
| Core Structure | Package setup, types, destination registration |
| Transformer Aliasing | Add `salesforce_bulk_upload` to `DestHandlerMap` (reuse existing transformer) |
| Authentication | OAuth 2.0 integration via existing OAuth v2 service |
| CSV & Upload | Extract transformed data, CSV generation, Bulk API upload logic |
| Polling & Status | Job status polling, result tracking |
| Stats & Errors | Detailed statistics, error handling |
| Testing | Unit tests, integration tests, sandbox testing |

**Note**: No new transformer code needed - we'll reuse the existing Salesforce transformer which already supports VDM/RETL.

### Proposed File Structure

**rudder-server:**
```
router/batchrouter/asyncdestinationmanager/salesforce-bulk/
├── manager.go              # Factory (~50 lines)
├── salesforce_bulk.go      # Main logic (~300 lines)
├── api_service.go          # Salesforce API client (~250 lines)
├── auth_service.go         # OAuth handling (~100 lines)
├── types.go                # Data structures (~150 lines)
├── utils.go                # CSV generation (~200 lines)
└── salesforce_bulk_test.go # Tests (~400 lines)
```

**rudder-transformer:**
```
src/constants/destinationCanonicalNames.js
└── Add: salesforce_bulk_upload: 'salesforce'  # 1 line - reuses existing transformer!
```

**Estimated Total: ~1,450 lines of code** (all in rudder-server - no new transformer code!)

### Success Criteria

**Performance Targets:**
- Process 100K records in < 5 minutes
- CSV generation < 10 seconds
- Upload success rate > 99%

**Reliability Requirements:**
- Handle OAuth token expiration gracefully
- Retry on transient failures (429, 5xx)
- Detailed error reporting for debugging

**Observability:**
- Emit metrics for upload time, success rate, record counts
- Log Salesforce job IDs for traceability
- Track API usage to avoid limits
- Monitor OAuth token refresh success rate

**RETL/VDM Support:**
- ✅ Visual Data Mapper (VDM) v1 support (inherited from existing Salesforce transformer)
- ✅ `mappedToDestination` flow support
- ✅ Visual field mapping UI (not manual typing)
- ✅ Consistent UX with regular Salesforce destination

---

## Why Bulk API 2.0?

### TL;DR: **Yes, use Bulk API 2.0 - it's objectively better for RudderStack's use case**

### Key Improvements Over 1.0

| Feature | Bulk API 1.0 | Bulk API 2.0 | Winner |
|---------|-------------|-------------|--------|
| **Batch Management** | Manual - you create batches | Automatic - Salesforce handles it | 2.0 ✅ |
| **File Size Limit** | 10MB per batch | 100MB per file | 2.0 ✅ |
| **Records Per Job** | 10K per batch, unlimited batches | 150M records per job | 2.0 ✅ |
| **Status Checking** | Check each batch separately | Single endpoint for whole job | 2.0 ✅ |
| **API Complexity** | More complex workflow | Simpler workflow | 2.0 ✅ |
| **Supported Formats** | CSV, XML, JSON | CSV, JSON only | 1.0 (if you need XML) |
| **Processing Control** | Serial/Parallel modes | Automatic | 1.0 (if you need control) |

### Workflow Comparison

**Bulk API 1.0 (Complex):**
```
1. Create Job
2. Add Batch 1 (max 10K records, 10MB)
3. Add Batch 2 (max 10K records, 10MB)
4. Add Batch N...
5. Close Job
6. Poll Batch 1 status
7. Poll Batch 2 status
8. Poll Batch N status...
9. Get Batch 1 results
10. Get Batch 2 results...
```

**Bulk API 2.0 (Simple):**
```
1. Create Job
2. Upload Data (up to 100MB)
3. Close Job
4. Poll Job status (one call)
5. Get results (one call)
```

### Why 2.0 is Perfect for RudderStack

**1. AsyncDestinationManager already handles batching**
- RudderStack's BatchRouter batches events
- Don't need Salesforce's manual batch management
- Just generate one CSV → upload → let Salesforce handle internal batching

**2. Simpler polling**
- One job status API call vs checking multiple batch statuses
- Fits perfectly with `Poll()` method in AsyncDestinationManager
- Less complexity in code

**3. Higher limits = better performance**
- 100MB files vs 10MB = fewer jobs needed
- 150M records per job vs 10K per batch
- Critical for large RETL warehouse syncs

**4. CSV is all we need**
- RudderStack generates CSV anyway for field mapping
- No need for XML support
- JSON support is bonus (future enhancement)

### Real-World Example

**Scenario: Sync 500K leads from Snowflake to Salesforce**

**With Bulk API 1.0:**
```go
// You manage batching manually
leads := 500000
batchSize := 10000  // Max per batch
numBatches := 50    // 500K / 10K = 50 batches!

jobID := createJob()
batchIDs := []string{}

// Create 50 batches
for i := 0; i < numBatches; i++ {
    batch := leads[i*10000:(i+1)*10000]
    batchID := addBatch(jobID, batch)
    batchIDs = append(batchIDs, batchID)
}
closeJob(jobID)

// Poll 50 batches
for _, batchID := range batchIDs {
    status := pollBatchStatus(jobID, batchID)
    // Handle each batch result separately
}
```

**With Bulk API 2.0:**
```go
// Salesforce handles batching automatically
leads := 500000  // All of them at once!

jobID := createJob("Contact", "insert")
uploadData(jobID, leads)  // One upload!
closeJob(jobID)           // Triggers processing
pollJobStatus(jobID)      // One status check!
getResults(jobID)         // One result call!
```

**50 API calls → 4 API calls!** 🎉

### When Would You Use 1.0?

Honestly, **almost never** for RudderStack's use case. Only if:
- Supporting very old Salesforce orgs (pre-2018)
- Specific requirement for XML format (we don't need it)
- Need fine-grained serial/parallel processing control (we don't)

### Decision: Bulk API 2.0 ✅

**Bulk API 2.0 is:**
- ✅ Simpler to implement
- ✅ Better performance (10x file size limit)
- ✅ Easier to maintain (less polling logic)
- ✅ Future-proof (Salesforce's recommended version)
- ✅ Perfect for AsyncDestinationManager pattern

---

## Overview & Fundamentals

### What Is This Destination?

The **Salesforce Bulk Upload** destination enables bulk synchronization of customer data to Salesforce objects using the Bulk API 2.0. It's designed for:

1. **High-volume data loads** (thousands to millions of records)
2. **RETL warehouse syncs** (primary use case)
3. **Asynchronous processing** with status polling
4. **CSV-based data ingestion** (internal format)
5. **Better throughput** than REST API for batch operations

### How Users Will Use It

**Step 1: Configure RETL Source**
```
User sets up warehouse connection:
- Snowflake table: customer_profiles
- Columns: email, first_name, last_name, company
```

**Step 2: Configure Salesforce Bulk Destination**
```
Field Mapping:
  email → Email
  first_name → FirstName
  last_name → LastName
  company → Company

Operation: Insert (or Update/Upsert)
Object: Contact
```

**Step 3: Data Flows Automatically**
```
Warehouse Data → RudderStack → Salesforce
(CSV conversion happens internally)
```

**Users never touch CSV files!**

### Salesforce Bulk API 2.0 Workflow

```
1. Create Job → 2. Upload CSV Data → 3. Close Job → 4. Poll Status → 5. Get Results
```

### API Endpoints

```
Base URL: https://[instance].salesforce.com/services/data/v57.0/jobs/ingest

POST   /jobs/ingest                      - Create a new ingest job
PUT    /jobs/ingest/{jobId}/batches      - Upload CSV data
PATCH  /jobs/ingest/{jobId}              - Close/abort a job
GET    /jobs/ingest/{jobId}              - Get job status
GET    /jobs/ingest/{jobId}/successfulResults  - Get successful records
GET    /jobs/ingest/{jobId}/failedResults      - Get failed records
DELETE /jobs/ingest/{jobId}              - Delete a job
```

### Job States

- `Open` - Job created, ready for data upload
- `UploadComplete` - Data uploaded, job closed, ready to process
- `InProgress` - Salesforce processing data
- `JobComplete` - All processing complete
- `Failed` - Job failed
- `Aborted` - Job was aborted

### Request/Response Examples

**Create Job:**
```json
POST /services/data/v57.0/jobs/ingest
{
  "object": "Contact",
  "contentType": "CSV",
  "operation": "insert",
  "lineEnding": "LF"
}

Response:
{
  "id": "7504W00000bCHhqQAG",
  "state": "Open",
  "object": "Contact",
  "operation": "insert",
  "createdDate": "2024-01-15T10:30:00.000+0000"
}
```

**Upload Data:**
```
PUT /services/data/v57.0/jobs/ingest/7504W00000bCHhqQAG/batches
Content-Type: text/csv

FirstName,LastName,Email
John,Doe,john@example.com
Jane,Smith,jane@example.com
```

**Close Job:**
```json
PATCH /services/data/v57.0/jobs/ingest/7504W00000bCHhqQAG
{
  "state": "UploadComplete"
}
```

**Poll Status:**
```json
GET /services/data/v57.0/jobs/ingest/7504W00000bCHhqQAG

Response:
{
  "id": "7504W00000bCHhqQAG",
  "state": "JobComplete",
  "numberRecordsProcessed": 2,
  "numberRecordsFailed": 0,
  "totalProcessingTime": 1234
}
```

### Supported Operations

- **Insert**: Create new records
- **Update**: Update existing records (requires Id field)
- **Upsert**: Insert or update based on external ID field
- **Delete**: Delete records (requires Id field)

---

## Architecture

### System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           RudderStack Server                                 │
│                                                                              │
│  ┌────────────────┐         ┌──────────────────┐        ┌──────────────┐  │
│  │  Gateway       │────────▶│  JobsDB          │───────▶│ BatchRouter  │  │
│  │  (Events In)   │         │  (Event Storage) │        │              │  │
│  └────────────────┘         └──────────────────┘        └──────┬───────┘  │
│                                                                  │           │
│                                                    ┌─────────────▼───────┐  │
│                                                    │ AsyncDestinationMgr │  │
│                                                    └─────────────┬───────┘  │
│                                                                  │           │
│                                          ┌───────────────────────┤           │
│                                          │                       │           │
│                              ┌───────────▼──────┐   ┌───────────▼──────┐  │
│                              │ Salesforce Bulk  │   │  Other Async     │  │
│                              │    Manager       │   │  Destinations    │  │
│                              └───────────┬──────┘   └──────────────────┘  │
│                                          │                                  │
└──────────────────────────────────────────┼──────────────────────────────────┘
                                           │
                                           │ HTTPS (OAuth 2.0)
                                           │
                              ┌────────────▼──────────────┐
                              │   Salesforce Platform     │
                              │  ┌──────────────────────┐ │
                              │  │  Bulk API 2.0        │ │
                              │  └──────────────────────┘ │
                              └───────────────────────────┘
```

### Salesforce Bulk Manager Internal Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                      SalesforceBulkManager                               │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      Transform Phase                            │   │
│  │  • Parse job payload                                            │   │
│  │  • Extract RudderStack event data                              │   │
│  │  • Return JSON for file storage                                │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                             │                                            │
│                             ▼                                            │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                       Upload Phase                              │   │
│  │                                                                 │   │
│  │  ┌────────────────┐         ┌──────────────────┐              │   │
│  │  │ Read Jobs      │────────▶│ Create CSV File  │              │   │
│  │  │ from File      │         │ • Field Mapping  │              │   │
│  │  └────────────────┘         │ • Size Limits    │              │   │
│  │                              │ • Hash Tracking  │              │   │
│  │                              └────────┬─────────┘              │   │
│  │                                       │                         │   │
│  │                                       ▼                         │   │
│  │                         ┌─────────────────────────┐            │   │
│  │                         │  API Service            │            │   │
│  │                         │  • CreateJob()          │            │   │
│  │                         │  • UploadData()         │            │   │
│  │                         │  • CloseJob()           │            │   │
│  │                         └─────────┬───────────────┘            │   │
│  │                                   │                             │   │
│  │                         ┌─────────▼────────────┐               │   │
│  │                         │  Auth Service        │               │   │
│  │                         │  • GetAccessToken()  │               │   │
│  │                         │  • RefreshToken()    │               │   │
│  │                         └──────────────────────┘               │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                             │                                            │
│                             ▼                                            │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                        Poll Phase                               │   │
│  │  • Call GetJobStatus() every 30s                               │   │
│  │  • Check state: Open → InProgress → JobComplete                │   │
│  │  • Return completion status                                     │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                             │                                            │
│                             ▼                                            │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                   GetUploadStats Phase                          │   │
│  │  • Fetch failed records CSV                                     │   │
│  │  • Fetch successful records CSV                                 │   │
│  │  • Match to original jobs via hash                              │   │
│  │  • Return EventStatMeta                                         │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────┘
```

### Data Flow: RETL to Salesforce

```
┌─────────────────────────────────────────────────────────────────┐
│                Warehouse (Snowflake/BigQuery)                   │
│  Table: customer_profiles                                       │
│  ┌─────────┬────────────┬───────────┬─────────┐               │
│  │ email   │ first_name │ last_name │ company │               │
│  ├─────────┼────────────┼───────────┼─────────┤               │
│  │ john@.. │ John       │ Doe       │ Acme    │               │
│  └─────────┴────────────┴───────────┴─────────┘               │
└────────────────────────────┬────────────────────────────────────┘
                             │ RETL Sync
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    RudderStack Event                            │
│  {                                                              │
│    "type": "record",                                            │
│    "fields": {                                                  │
│      "email": "john@example.com",                              │
│      "first_name": "John",                                      │
│      "last_name": "Doe",                                        │
│      "company": "Acme"                                          │
│    }                                                            │
│  }                                                              │
└────────────────────────────┬────────────────────────────────────┘
                             │ Transform + Field Mapping
                             │ email → Email
                             │ first_name → FirstName
                             │ last_name → LastName
                             │ company → Company
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      CSV File (Internal)                        │
│  Email,FirstName,LastName,Company                              │
│  john@example.com,John,Doe,Acme                                │
└────────────────────────────┬────────────────────────────────────┘
                             │ Upload via Bulk API
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Salesforce Contact                            │
│  {                                                              │
│    Email: "john@example.com",                                  │
│    FirstName: "John",                                           │
│    LastName: "Doe",                                            │
│    Company: "Acme"                                              │
│  }                                                              │
└─────────────────────────────────────────────────────────────────┘
```

### Upload Workflow Sequence

```
RudderStack              Salesforce Bulk         Salesforce
BatchRouter              Manager                 API
    │                        │                      │
    │  Transform(job)        │                      │
    ├───────────────────────▶│                      │
    │  string (JSON)         │                      │
    │◀───────────────────────┤                      │
    │                        │                      │
    │  Upload(asyncDest)     │                      │
    ├───────────────────────▶│                      │
    │                        │  1. POST /oauth/token│
    │                        │─────────────────────▶│
    │                        │  access_token        │
    │                        │◀─────────────────────┤
    │                        │                      │
    │                        │  2. POST /jobs/ingest│
    │                        │  {object:"Contact"}  │
    │                        │─────────────────────▶│
    │                        │  {id:"job-123"}      │
    │                        │◀─────────────────────┤
    │                        │                      │
    │                        │  3. PUT .../batches  │
    │                        │  [CSV Data]          │
    │                        │─────────────────────▶│
    │                        │  200 OK              │
    │                        │◀─────────────────────┤
    │                        │                      │
    │                        │  4. PATCH .../job-123│
    │                        │  {state:"UploadComplete"}
    │                        │─────────────────────▶│
    │                        │  200 OK              │
    │                        │◀─────────────────────┤
    │                        │                      │
    │  AsyncUploadOutput     │                      │
    │  {importing:[jobs]}    │                      │
    │◀───────────────────────┤                      │
    │                        │                      │
    │  Poll({jobId})         │                      │
    ├───────────────────────▶│                      │
    │                        │  5. GET .../job-123  │
    │                        │─────────────────────▶│
    │                        │  {state:"InProgress"}│
    │                        │◀─────────────────────┤
    │  {inProgress:true}     │                      │
    │◀───────────────────────┤                      │
    │                        │                      │
    │  ... wait 30s ...      │                      │
    │                        │                      │
    │  Poll({jobId})         │                      │
    ├───────────────────────▶│                      │
    │                        │  6. GET .../job-123  │
    │                        │─────────────────────▶│
    │                        │  {state:"JobComplete"}
    │                        │◀─────────────────────┤
    │  {complete:true}       │                      │
    │◀───────────────────────┤                      │
    │                        │                      │
    │  GetUploadStats()      │                      │
    ├───────────────────────▶│                      │
    │                        │  7. GET .../failedResults
    │                        │─────────────────────▶│
    │                        │  [CSV failures]      │
    │                        │◀─────────────────────┤
    │  EventStatMeta         │                      │
    │  {succeeded,failed}    │                      │
    │◀───────────────────────┤                      │
```

### RETL Support

**Yes, fully supported!** RETL warehouse syncs work automatically:

1. **Same code path**: RETL jobs flow through the same BatchRouter → AsyncDestinationManager
2. **RETL metadata preserved**: `sourceJobRunID`, `sourceTaskRunID` tracked in job parameters
3. **No special handling needed**: Destination code is source-agnostic

**Flow:**
```
Warehouse → RETL Gateway → JobsDB → BatchRouter → Salesforce Bulk Manager → Salesforce
```

**The implementation handles both:**
- ✅ RETL warehouse syncs (primary use case)
- ✅ High-volume event streams
- ✅ Any other source that writes to JobsDB

---

## Implementation Plan

### Phase 1: Core Structure & Transformer Aliasing

**1. Create package structure in rudder-server**
```
router/batchrouter/asyncdestinationmanager/salesforce-bulk/
├── manager.go                 # Factory and initialization
├── salesforce_bulk.go         # Main manager implementation
├── api_service.go             # Salesforce API interactions
├── auth_service.go            # OAuth authentication
├── types.go                   # Data structures
├── utils.go                   # Helper functions (CSV generation)
├── salesforce_bulk_test.go   # Unit tests
└── testdata/
    └── uploadData.txt         # Test fixtures
```

**2. Alias transformer in rudder-transformer (1 line!)**
```javascript
// rudder-transformer/src/constants/destinationCanonicalNames.js
const DestHandlerMap = {
  ga360: 'ga',
  salesforce_oauth: 'salesforce',
  salesforce_oauth_sandbox: 'salesforce',
  salesforce_bulk_upload: 'salesforce',  // ← Add this - reuses existing transformer!
};
```

This makes `SALESFORCE_BULK_UPLOAD` use the existing `/salesforce/transform.js` code, which already has:
- ✅ VDM/RETL support (`mappedToDestination` handling)
- ✅ Field mapping logic
- ✅ All Salesforce object types (Lead, Contact, Custom Objects)

**3. Register as batch destination in rudder-server**
```go
// utils/misc/misc.go
func BatchDestinations() []string {
    return []string{
        "S3", "MINIO", "GCS",
        "MARKETO_BULK_UPLOAD",
        "SALESFORCE_BULK_UPLOAD",  // ← Routes to BatchRouter
        // ...
    }
}

// router/batchrouter/asyncdestinationmanager/common/utils.go
var asyncDestinations = []string{
    "MARKETO_BULK_UPLOAD",
    "SALESFORCE_BULK_UPLOAD",  // ← Handles as async destination
    // ...
}

// router/batchrouter/asyncdestinationmanager/manager.go
case "SALESFORCE_BULK_UPLOAD":
    return salesforcebulk.NewManager(logger, statsFactory, destination, backendConfig)
```

### Phase 2: Authentication

**Approach: Use OAuth v2 Service (like Bing Ads)**

Instead of handling OAuth ourselves (like Marketo does with client credentials), we'll leverage our existing OAuth v2 service. This means:

**Control Plane handles:**
- User authorization flow
- Token storage (encrypted)
- Automatic token refresh

**Our code handles:**
- Fetching tokens from OAuth v2 service
- Caching tokens in memory
- Detecting token expiration

**Implementation:**
```go
type SalesforceAuthService struct {
    config       DestinationConfig
    logger       logger.Logger
    oauthClient  oauthv2.Authorizer
    workspaceID  string
    accountID    string
    destID       string
    accessToken  string
    instanceURL  string
    tokenExpiry  time.Time
}

func (s *SalesforceAuthService) GetAccessToken() (string, error) {
    // Check cache
    if time.Now().Before(s.tokenExpiry) && s.accessToken != "" {
        return s.accessToken, nil
    }
    
    // Fetch from OAuth service
    params := oauthv2.RefreshTokenParams{
        WorkspaceID:   s.workspaceID,
        DestDefName:   "SALESFORCE_BULK_UPLOAD",
        AccountID:     s.accountID,
        DestinationID: s.destID,
    }
    
    statusCode, authResponse, err := s.oauthClient.FetchToken(&params)
    if err != nil {
        return "", fmt.Errorf("fetching token: %v, status: %d", err, statusCode)
    }
    
    // Parse and cache
    var tokenResp struct {
        AccessToken string `json:"access_token"`
        InstanceURL string `json:"instance_url"`
        ExpiresIn   int    `json:"expires_in"`
    }
    json.Unmarshal(authResponse.Account.Secret, &tokenResp)
    
    s.accessToken = tokenResp.AccessToken
    s.instanceURL = tokenResp.InstanceURL
    s.tokenExpiry = time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second)
    
    return s.accessToken, nil
}
```

**Rationale for OAuth v2 approach:**
- More secure than client credentials (short-lived tokens, easy revocation)
- Better user experience (OAuth flow in Control Plane UI)
- Leverages RudderStack's existing OAuth v2 infrastructure (proven with Bing Ads)
- Minimal code in our contribution (~100 lines vs ~300 for self-managed OAuth)
- Salesforce's recommended authentication method

**Note for RudderStack team**: This approach requires OAuth Connected App setup in Control Plane. We're happy to collaborate on this configuration aspect.

### Phase 2.5: Transform Method (Extract Already-Transformed Data)

The existing Salesforce transformer will have already run in the Processor stage. Our Transform() method just extracts the transformed data:

```go
func (*SalesforceBulkUploader) Transform(job *jobsdb.JobT) (string, error) {
    // Job payload already transformed by /salesforce/transform.js
    // Just extract and wrap with metadata
    return common.GetMarshalledData(
        gjson.GetBytes(job.EventPayload, "body.JSON").String(),
        job.JobID,
    )
}
```

**What the existing Salesforce transformer provides:**
- Field mapping (based on user's VDM configuration or object type)
- Validation
- Data type conversions
- Support for Lead, Contact, and Custom Objects
- `mappedToDestination` flow for RETL

**What we extract:**
- `body.JSON` contains the Salesforce-formatted payload
- We wrap it with job metadata for tracking
- No transformation logic needed here!

**Configuration structure**
```go
type DestinationConfig struct {
    RudderAccountID  string            `json:"rudderAccountId"`  // For OAuth v2 service
    ObjectName       string            `json:"objectName"`       // Contact, Lead, etc.
    Operation        string            `json:"operation"`        // insert, update, upsert, delete
    ExternalIDField  string            `json:"externalIdField"`  // For upsert operations
    FieldMapping     map[string]string `json:"fieldMapping"`     // Field transformations
    APIVersion       string            `json:"apiVersion"`       // Default: v57.0
    
    // Note: No instanceUrl, clientId, clientSecret needed!
    // OAuth v2 service provides these via FetchToken response
}
```

### Phase 3: CSV Generation & Upload

**1. CSV file creation**
- Extract already-transformed Salesforce fields from `body.JSON`
- Size limits (100MB per job - Salesforce Bulk API 2.0 limit)
- Handle overflow jobs  
- Generate hash codes for result tracking

**Key difference from field mapping**: The Salesforce transformer already did the field mapping in the Processor stage. We just need to:
1. Read the transformed payload from `job.EventPayload.body.JSON`
2. Generate CSV with those Salesforce field names
3. Track which job ID maps to which CSV row (for result matching)

**2. Upload flow**
```go
func (s *SalesforceBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
    // 1. Read jobs from file
    input, err := readJobsFromFile(asyncDestStruct.FileName)
    
    // 2. Create CSV with field mapping
    csvFilePath, headers, insertedJobIDs, overflowedJobIDs, err := 
        createCSVFile(destinationID, s.config, input, s.dataHashToJobId)
    defer os.Remove(csvFilePath)
    
    // 3. Create Salesforce job
    jobID, err := s.apiService.CreateJob(
        s.config.ObjectName, 
        s.config.Operation,
        s.config.ExternalIDField,
    )
    
    // 4. Upload CSV data
    err = s.apiService.UploadData(jobID, csvFilePath)
    
    // 5. Close job to start processing
    err = s.apiService.CloseJob(jobID)
    
    // 6. Return importing status
    return common.AsyncUploadOutput{
        ImportingJobIDs:     insertedJobIDs,
        ImportingParameters: json.RawMessage(`{"jobId":"` + jobID + `"}`),
        FailedJobIDs:        overflowedJobIDs,
        ImportingCount:      len(insertedJobIDs),
        DestinationID:       asyncDestStruct.Destination.ID,
    }
}
```

### Phase 4: Polling & Status

**Status polling**
```go
func (s *SalesforceBulkUploader) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
    jobStatus, err := s.apiService.GetJobStatus(pollInput.JobId)
    
    switch jobStatus.State {
    case "JobComplete":
        return common.PollStatusResponse{
            StatusCode:           200,
            Complete:             true,
            HasFailed:            jobStatus.NumberRecordsFailed > 0,
            FailedJobParameters:  buildFailedJobsURL(pollInput.JobId),
        }
    case "InProgress", "UploadComplete":
        return common.PollStatusResponse{
            StatusCode: 200,
            InProgress: true,
        }
    case "Failed", "Aborted":
        return common.PollStatusResponse{
            StatusCode: 200,
            Complete:   true,
            HasFailed:  true,
            Error:      jobStatus.ErrorMessage,
        }
    }
}
```

### Phase 5: Statistics & Error Handling

**Get upload statistics**
```go
func (s *SalesforceBulkUploader) GetUploadStats(input common.GetUploadStatsInput) common.GetUploadStatsResponse {
    // 1. Parse jobId from parameters
    var params struct { JobId string `json:"jobId"` }
    json.Unmarshal(input.Parameters, &params)
    
    // 2. Fetch failed records
    failedRecords, err := s.apiService.GetFailedRecords(params.JobId)
    
    // 3. Fetch successful records  
    successRecords, err := s.apiService.GetSuccessfulRecords(params.JobId)
    
    // 4. Match records to job IDs using hash tracking
    metadata := s.matchRecordsToJobs(input.ImportingList, failedRecords, successRecords)
    
    return common.GetUploadStatsResponse{
        StatusCode: 200,
        Metadata:   metadata,
    }
}
```

### Phase 6: Testing

**1. Unit tests** (following warehouse testing guidelines)
- Table-driven tests
- Mock API service
- Test all error scenarios
- Use `require` assertions

**2. Integration tests**
- Test with Salesforce sandbox
- Test different operations (insert, update, upsert)
- Test OAuth flow
- Test large data sets

---

## Code Structure & Examples

### types.go

```go
package salesforcebulk

import (
    "github.com/rudderlabs/rudder-go-kit/logger"
    "github.com/rudderlabs/rudder-go-kit/stats"
)

type DestinationConfig struct {
    RudderAccountID string            `json:"rudderAccountId"`  // For OAuth v2 service
    ObjectName      string            `json:"objectName"`       // Contact, Lead, etc.
    Operation       string            `json:"operation"`        // insert, update, upsert, delete
    ExternalIDField string            `json:"externalIdField"`  // For upsert operations
    FieldMapping    map[string]string `json:"fieldMapping"`     // Field transformations
    APIVersion      string            `json:"apiVersion"`       // Default: v57.0
    
    // Note: instanceUrl, access tokens provided by OAuth v2 service
}

type SalesforceBulkUploader struct {
    destName        string
    config          DestinationConfig
    logger          logger.Logger
    statsFactory    stats.Stats
    apiService      SalesforceAPIServiceInterface
    authService     SalesforceAuthServiceInterface
    csvHeaders      []string
    dataHashToJobId map[string]int64
}

type JobResponse struct {
    ID                     string  `json:"id"`
    State                  string  `json:"state"`
    Object                 string  `json:"object"`
    Operation              string  `json:"operation"`
    NumberRecordsProcessed int     `json:"numberRecordsProcessed"`
    NumberRecordsFailed    int     `json:"numberRecordsFailed"`
    ErrorMessage           string  `json:"errorMessage,omitempty"`
}

type APIError struct {
    StatusCode int
    Message    string
    Category   string // "RefreshToken", "RateLimit", "BadRequest"
}
```

### manager.go

```go
package salesforcebulk

import (
    "fmt"
    
    "github.com/rudderlabs/rudder-go-kit/logger"
    "github.com/rudderlabs/rudder-go-kit/stats"
    backendconfig "github.com/rudderlabs/rudder-server/backend-config"
    "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

const destName = "SALESFORCE_BULK"

func NewManager(
    logger logger.Logger,
    statsFactory stats.Stats,
    destination *backendconfig.DestinationT,
    backendConfig backendconfig.BackendConfig,
) (common.AsyncDestinationManager, error) {
    config, err := parseDestinationConfig(destination)
    if err != nil {
        return nil, fmt.Errorf("parsing destination config: %w", err)
    }
    
    if config.APIVersion == "" {
        config.APIVersion = "v57.0"
    }
    
    // Initialize OAuth v2 client
    oauthClient := oauthv2.NewOAuthClient(backendConfig)
    
    // Initialize auth service (handles token fetching/caching)
    authService := NewSalesforceAuthService(
        config,
        logger,
        oauthClient,
        destination.WorkspaceID,
        destination.ID,
    )
    
    // Initialize API service
    apiService := NewSalesforceAPIService(authService, logger, config.APIVersion)
    
    return &SalesforceBulkUploader{
        destName:        destName,
        config:          config,
        logger:          logger,
        statsFactory:    statsFactory,
        apiService:      apiService,
        authService:     authService,
        dataHashToJobId: make(map[string]int64),
    }, nil
}
```

### Key Implementation: Field Mapping

```go
func createCSVFile(
    destinationID string,
    config DestinationConfig,
    input []common.AsyncJob,
    dataHashToJobId map[string]int64,
) (string, []string, []int64, []int64, error) {
    
    csvFilePath := fmt.Sprintf("/tmp/salesforce_%s_%d.csv", destinationID, time.Now().Unix())
    csvFile, err := os.Create(csvFilePath)
    if err != nil {
        return "", nil, nil, nil, err
    }
    defer csvFile.Close()
    
    writer := csv.NewWriter(csvFile)
    defer writer.Flush()
    
    // Build headers from field mapping (Salesforce fields)
    var headers []string
    headerMap := make(map[string]int)
    for _, sfField := range config.FieldMapping {
        if _, exists := headerMap[sfField]; !exists {
            headerMap[sfField] = len(headers)
            headers = append(headers, sfField)
        }
    }
    
    writer.Write(headers)
    
    var insertedJobIDs []int64
    var overflowedJobIDs []int64
    currentSize := int64(0)
    maxSize := int64(10 * 1024 * 1024) // 10MB limit
    
    for _, job := range input {
        row := make([]string, len(headers))
        
        // Map RudderStack fields to Salesforce fields
        message := job.Message
        for rsField, sfField := range config.FieldMapping {
            if value, exists := message[rsField]; exists {
                if idx, ok := headerMap[sfField]; ok {
                    row[idx] = fmt.Sprintf("%v", value)
                }
            }
        }
        
        rowSize := int64(len([]byte(strings.Join(row, ",") + "\n")))
        jobID := int64(job.Metadata["job_id"].(float64))
        
        if currentSize+rowSize > maxSize {
            overflowedJobIDs = append(overflowedJobIDs, jobID)
            continue
        }
        
        writer.Write(row)
        currentSize += rowSize
        insertedJobIDs = append(insertedJobIDs, jobID)
        
        // Track hash for result matching
        hash := calculateHashCode(row)
        dataHashToJobId[hash] = jobID
    }
    
    return csvFilePath, headers, insertedJobIDs, overflowedJobIDs, nil
}
```

---

## Testing Strategy

### Unit Tests (Following Warehouse Guidelines)

```go
package salesforcebulk_test

import (
    "testing"
    
    "github.com/stretchr/testify/require"
    "go.uber.org/mock/gomock"
    
    sfbulk "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/salesforce-bulk"
)

func TestSalesforceBulk_Upload(t *testing.T) {
    testCases := []struct {
        name            string
        setupMock       func(*MockSalesforceAPIService)
        asyncDestStruct *common.AsyncDestinationStruct
        expectedOutput  common.AsyncUploadOutput
        wantErr         bool
    }{
        {
            name: "successful upload - insert operation",
            setupMock: func(apiMock *MockSalesforceAPIService) {
                apiMock.EXPECT().CreateJob("Contact", "insert", "").Return("job-123", nil)
                apiMock.EXPECT().UploadData("job-123", gomock.Any()).Return(nil)
                apiMock.EXPECT().CloseJob("job-123").Return(nil)
            },
            asyncDestStruct: createTestAsyncDestStruct(),
            wantErr:         false,
        },
        {
            name: "upload failure - rate limit",
            setupMock: func(apiMock *MockSalesforceAPIService) {
                apiMock.EXPECT().CreateJob(gomock.Any(), gomock.Any(), gomock.Any()).
                    Return("", &sfbulk.APIError{StatusCode: 429, Category: "RateLimit"})
            },
            asyncDestStruct: createTestAsyncDestStruct(),
            wantErr:         true,
        },
    }
    
    for _, tc := range testCases {
        t.Run(tc.name, func(t *testing.T) {
            t.Parallel()
            
            ctrl := gomock.NewController(t)
            defer ctrl.Finish()
            
            mockAPI := NewMockSalesforceAPIService(ctrl)
            tc.setupMock(mockAPI)
            
            uploader := &sfbulk.SalesforceBulkUploader{
                apiService: mockAPI,
            }
            
            result := uploader.Upload(tc.asyncDestStruct)
            
            if tc.wantErr {
                require.NotEmpty(t, result.FailedReason)
            } else {
                require.Empty(t, result.FailedReason)
            }
        })
    }
}
```

### Integration Tests

```go
func TestSalesforceBulk_Integration(t *testing.T) {
    if os.Getenv("SLOW") != "1" {
        t.Skip("Skipping integration tests. Add 'SLOW=1' env var to run.")
    }
    
    // Requires OAuth account set up in Control Plane
    // Note: Can't use env vars for OAuth - must use real OAuth service
    config := sfbulk.DestinationConfig{
        RudderAccountID: "test-sf-account-123",  // OAuth account ID
        ObjectName:      "Contact",
        Operation:       "insert",
        FieldMapping: map[string]string{
            "email":     "Email",
            "firstName": "FirstName",
            "lastName":  "LastName",
        },
    }
    
    // Create manager with OAuth v2 service
    manager, err := sfbulk.NewManager(
        logger.NOP, 
        stats.NOP, 
        createDestination(config),
        backendConfig, // Provides OAuth client
    )
    require.NoError(t, err)
    
    // Test full workflow
    asyncDest := createIntegrationTestData()
    result := manager.Upload(asyncDest)
    require.Empty(t, result.FailedReason)
    require.Greater(t, result.ImportingCount, 0)
}
```

---

## Configuration & Deployment

### Environment Variables

```yaml
# Salesforce Bulk specific configs
BatchRouter.SALESFORCE_BULK.maxRetries: 3
BatchRouter.SALESFORCE_BULK.maxUploadLimit: 10485760  # 10MB
BatchRouter.SALESFORCE_BULK.pollInterval: 30s
BatchRouter.SALESFORCE_BULK.pollTimeout: 1h
BatchRouter.SALESFORCE_BULK.apiVersion: v57.0
```

### Backend Configuration Schema

```json
{
  "configSchema": {
    "rudderAccountId": {
      "type": "string",
      "label": "Salesforce Account",
      "description": "OAuth account configured in RudderStack",
      "required": true,
      "note": "User authorizes via OAuth flow in Control Plane UI"
    },
    "objectName": {
      "type": "string",
      "label": "Salesforce Object",
      "placeholder": "Contact, Account, CustomObject__c",
      "required": true
    },
    "operation": {
      "type": "singleSelect",
      "label": "Operation",
      "options": [
        {"name": "Insert", "value": "insert"},
        {"name": "Update", "value": "update"},
        {"name": "Upsert", "value": "upsert"},
        {"name": "Delete", "value": "delete"}
      ],
      "default": "insert",
      "required": true
    },
    "externalIdField": {
      "type": "string",
      "label": "External ID Field (for Upsert)",
      "placeholder": "Email, CustomExternalId__c",
      "condition": {"operation": "upsert"}
    },
    "fieldMapping": {
      "type": "dynamicFieldMapping",
      "label": "Field Mapping",
      "description": "Map RudderStack fields to Salesforce fields",
      "required": true
    }
  }
}
```

### Salesforce API Limits

| Limit | Value | Impact | Mitigation |
|-------|-------|--------|------------|
| Daily API Requests | 15K - 1M+ | Medium | Monitor usage, implement backoff |
| Concurrent Bulk Jobs | 5-15 | Medium | Queue jobs if limit reached |
| File Size per Job | 100 MB | Low | Handle with overflow |
| Records per Job | 150M | Very Low | Unlikely to hit |

### Deployment Checklist

**Development (Hercules/Fullscript team):**
- [ ] Add `salesforce_bulk_upload: 'salesforce'` to `DestHandlerMap` in rudder-transformer
- [ ] Implement destination code in rudder-server following AsyncDestinationManager pattern
- [ ] Add `SALESFORCE_BULK_UPLOAD` to `batchDestinations` list
- [ ] Add `SALESFORCE_BULK_UPLOAD` to `asyncDestinations` list
- [ ] Register in manager factory
- [ ] Write comprehensive unit tests
- [ ] Test with Salesforce sandbox
- [ ] Verify existing Salesforce transformer handles bulk upload correctly
- [ ] Integration tests passing

**RudderStack Team Collaboration Needed:**
- [ ] OAuth Connected App setup in Control Plane (can existing Salesforce OAuth be reused?)
- [ ] Destination definition added to backend config with `supportsVisualMapper: true`
- [ ] UI configuration schema (similar to regular Salesforce)
- [ ] Code review and merge (both repos)

**Post-deployment:**
- [ ] Monitor API usage
- [ ] Track success/failure rates
- [ ] Monitor OAuth token refresh success rate
- [ ] Review error logs

---

## Collaboration & Support Needed

### What Hercules/Fullscript Team Can Implement

**rudder-server implementation:**
- ✅ All Go code in `salesforce-bulk/` package
- ✅ CSV generation and Bulk API upload logic
- ✅ OAuth v2 service integration
- ✅ Unit tests following warehouse testing guidelines
- ✅ Integration tests with Salesforce sandbox
- ✅ Documentation
- ✅ Testing with Fullscript's production data

**rudder-transformer changes:**
- ✅ One-line addition to `DestHandlerMap` (alias to existing Salesforce transformer)
- ✅ Test that existing transformer works with `SALESFORCE_BULK_UPLOAD` type

### What We Need from RudderStack Team

1. **Control Plane OAuth Setup**
   - Salesforce Connected App configuration
   - OAuth flow integration in Control Plane UI
   - Token storage and refresh infrastructure
   - **Can we reuse existing Salesforce OAuth setup or needs separate Connected App?**

2. **Backend Config**
   - Add `SALESFORCE_BULK_UPLOAD` destination definition
   - UI configuration schema (similar to regular Salesforce)
   - Destination metadata (icon, description, etc.)
   - **VDM/Visual Mapper enablement** (`supportsVisualMapper: true`)

3. **Code Review & Guidance**
   - Review of our rudder-server implementation
   - Confirm transformer aliasing approach (reusing existing Salesforce transformer)
   - Guidance on OAuth v2 service integration details
   - Best practices for batch destination registration

4. **Deployment Support**
   - Merge to both repositories (rudder-server + rudder-transformer)
   - Deployment to RudderStack cloud
   - Monitoring setup


---

## References

**Salesforce Documentation:**
- [Salesforce Bulk API 2.0 Developer Guide](https://developer.salesforce.com/docs/atlas.en-us.api_bulk_v2.meta/api_bulk_v2/)
- [Salesforce OAuth 2.0 Guide](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_web_server_flow.htm)

**RudderStack Codebase (rudder-server):**
- [AsyncDestinationManager README](https://github.com/rudderlabs/rudder-server/blob/master/router/batchrouter/asyncdestinationmanager/README.md)
- [Marketo Bulk Upload Implementation](https://github.com/rudderlabs/rudder-server/tree/master/router/batchrouter/asyncdestinationmanager/marketo-bulk-upload)
- [Eloqua Implementation](https://github.com/rudderlabs/rudder-server/tree/master/router/batchrouter/asyncdestinationmanager/eloqua)
- [Bing Ads OAuth Implementation](https://github.com/rudderlabs/rudder-server/blob/master/router/batchrouter/asyncdestinationmanager/bing-ads/common/token.go)
- [OAuth v2 Service](https://github.com/rudderlabs/rudder-server/tree/master/services/oauth/v2)

**RudderStack Codebase (rudder-transformer):**
- [Existing Salesforce Transformer](https://github.com/rudderlabs/rudder-transformer/tree/master/src/v0/destinations/salesforce)
- [DestHandlerMap Pattern](https://github.com/rudderlabs/rudder-transformer/blob/master/src/constants/destinationCanonicalNames.js)

