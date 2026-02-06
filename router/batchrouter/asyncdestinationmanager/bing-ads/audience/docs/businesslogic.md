# Bing Ads Audience - Business Logic

## Table of Contents

- [1. Overview](#1-overview)
- [2. Data Mappings](#2-data-mappings)
- [3. Event Processing Flow](#3-event-processing-flow)
- [4. Action Types and Behavior](#4-action-types-and-behavior)
- [5. Email Hashing Requirements](#5-email-hashing-requirements)
- [6. Error Scenarios and Handling](#6-error-scenarios-and-handling)
- [7. Common Use Cases](#7-common-use-cases)
- [8. Flow Diagrams](#8-flow-diagrams)

---

## 1. Overview

### 1.1 Purpose

The Bing Ads Audience destination enables syncing customer lists (audiences) to Microsoft Advertising for:
- **Remarketing**: Target users who have previously interacted with your business
- **Customer Match**: Upload first-party customer data for targeting
- **Lookalike Audiences**: Microsoft can create similar audiences based on your customer lists

### 1.2 Key Concepts

| Concept | Description |
|---------|-------------|
| **Customer List** | A Microsoft Advertising audience type that accepts uploaded customer data |
| **Bulk API** | Microsoft's batch processing API for large-scale data operations |
| **Action Types** | Operations on audience members: Add, Remove, Replace |
| **Client ID** | Tracking identifier format: `{jobId}<<>>{hashedEmail}` |

---

## 2. Data Mappings

### 2.1 RudderStack to Bing Ads Mapping

#### audienceList Event Structure

**RudderStack Input:**
```json
{
    "type": "audienceList",
    "properties": {
        "listData": {
            "add": [
                {"email": "user1@example.com"},
                {"email": "user2@example.com"}
            ],
            "remove": [
                {"email": "user3@example.com"}
            ]
        }
    }
}
```

**Transformed Output (after CDK v2 processor):**
```json
{
    "message": {
        "action": "Add",
        "list": [
            {"email": "user1@example.com", "hashedEmail": "sha256_hash1"},
            {"email": "user2@example.com", "hashedEmail": "sha256_hash2"}
        ]
    },
    "metadata": {
        "job_id": 12345
    }
}
```

### 2.2 Field Mapping Table

| RudderStack Field | Transformation | Bing Ads Field | Notes |
|-------------------|----------------|----------------|-------|
| `properties.listData.add[].email` | SHA256 hash (if enabled) | `Text` (Customer List Item) | Lowercase before hashing |
| `properties.listData.remove[].email` | SHA256 hash (if enabled) | `Text` (Customer List Item) | Lowercase before hashing |
| Action type (add/remove) | Mapped to action | `Action Type` (Customer List) | Add, Remove, or Replace |
| `audienceId` (from config) | Direct | `Parent Id` | Target Customer List ID |
| Job ID + Email hash | Concatenated | `Client Id` | Format: `{jobId}<<>>{hash}` |

### 2.3 CSV Column Mapping

| CSV Column | Value Source | Example |
|------------|--------------|---------|
| Type | Fixed | `Customer List Item` |
| Status | Empty | `` |
| Id | Empty | `` |
| Parent Id | Config: `audienceId` | `819689004` |
| Client Id | Generated | `1<<>>abc123def456...` |
| Modified Time | Empty | `` |
| Name | Empty | `` |
| Description | Empty | `` |
| Scope | Empty | `` |
| Audience | Empty | `` |
| Action Type | Empty (set on Customer List row) | `` |
| Sub Type | Fixed | `Email` |
| Text | Hashed email | `abc123def456...` |

---

## 3. Event Processing Flow

### 3.1 End-to-End Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         RudderStack Pipeline                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. SOURCE                                                                   │
│     │                                                                        │
│     │  audienceList event                                                    │
│     │  {                                                                     │
│     │    "type": "audienceList",                                             │
│     │    "properties": {                                                     │
│     │      "listData": {                                                     │
│     │        "add": [{"email": "..."}],                                      │
│     │        "remove": [{"email": "..."}]                                    │
│     │      }                                                                 │
│     │    }                                                                   │
│     │  }                                                                     │
│     ▼                                                                        │
│  2. TRANSFORMER (CDK v2)                                                     │
│     │                                                                        │
│     │  - Validates event structure                                           │
│     │  - Hashes emails (if hashEmail: true)                                  │
│     │  - Splits into separate Add/Remove payloads                            │
│     │  - Formats for batch router                                            │
│     ▼                                                                        │
│  3. BATCH ROUTER                                                             │
│     │                                                                        │
│     │  - Batches multiple events into file                                   │
│     │  - Calls Transform() for each job                                      │
│     │  - Creates JSON-L file                                                 │
│     ▼                                                                        │
│  4. ASYNC DESTINATION MANAGER (This code)                                    │
│     │                                                                        │
│     │  Upload Phase:                                                         │
│     │  ├─ Read JSON-L file                                                   │
│     │  ├─ Group by action type (Add/Remove/Replace)                          │
│     │  ├─ Create CSV files per action                                        │
│     │  ├─ Convert to ZIP                                                     │
│     │  ├─ Get upload URL from Bing                                           │
│     │  └─ Upload ZIP file                                                    │
│     │                                                                        │
│     │  Poll Phase:                                                           │
│     │  ├─ Check upload status                                                │
│     │  ├─ Wait for processing                                                │
│     │  └─ Get result file URL                                                │
│     │                                                                        │
│     │  Stats Phase:                                                          │
│     │  ├─ Download result file                                               │
│     │  ├─ Parse error rows                                                   │
│     │  └─ Return success/failure stats                                       │
│     ▼                                                                        │
│  5. MICROSOFT ADVERTISING                                                    │
│                                                                              │
│     - Processes Customer List updates                                        │
│     - Matches users for targeting                                            │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 File Processing Detail

```
Input: uploadData.txt (JSON-L)
─────────────────────────────────────────────────────────────
{"message":{"action":"Add","list":[{"email":"a@b.com","hashedEmail":"hash1"}]},"metadata":{"job_id":1}}
{"message":{"action":"Add","list":[{"email":"c@d.com","hashedEmail":"hash2"}]},"metadata":{"job_id":2}}
{"message":{"action":"Remove","list":[{"email":"e@f.com","hashedEmail":"hash3"}]},"metadata":{"job_id":3}}

                            │
                            ▼
                    Group by Action
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
   Add Events          Remove Events      Replace Events
   (jobs 1,2)            (job 3)            (none)
        │                   │                   │
        ▼                   ▼                   ▼
   add.csv              remove.csv         (skipped)
        │                   │
        ▼                   ▼
   add.zip              remove.zip
        │                   │
        ▼                   ▼
   Upload #1            Upload #2
   ImportId: abc        ImportId: def
        │                   │
        └───────┬───────────┘
                ▼
        Combined ImportIds
        "abc,def"
```

---

## 4. Action Types and Behavior

### 4.1 Add Action

**Purpose:** Add new users to the Customer List audience

**Behavior:**
- Users are added to the existing audience
- Duplicate emails are ignored (idempotent)
- Processing is asynchronous

**Use Cases:**
- Onboarding new customers
- Adding users who completed a conversion
- Growing remarketing audiences

### 4.2 Remove Action

**Purpose:** Remove users from the Customer List audience

**Behavior:**
- Specified users are removed from the audience
- Non-existent users are silently ignored
- Processing is asynchronous

**Use Cases:**
- Honoring opt-out requests
- Removing churned customers
- GDPR/CCPA compliance

### 4.3 Replace Action

**Purpose:** Replace entire audience with new user list

**Behavior:**
- All existing users are removed
- Only specified users remain in audience
- Atomic operation (all-or-nothing)

**Use Cases:**
- Complete audience refresh
- Mirror sync from source of truth
- Periodic full sync operations

### 4.4 Action Type Precedence

When multiple actions exist in the same batch:
1. Each action type creates a separate file
2. Files are uploaded independently
3. Processing order is not guaranteed

**Recommendation:** Avoid having the same email in multiple action types within the same sync to prevent race conditions.

---

## 5. Email Hashing Requirements

### 5.1 Microsoft Requirements

Microsoft Advertising requires emails to be:
1. **Lowercase** before hashing
2. **SHA256 hashed** (64 character hex string)
3. **No prefixes** (unlike some platforms that require `sha256::` prefix)

### 5.2 Hashing Process

```
Original Email: User@Example.COM
        │
        ▼
Lowercase: user@example.com
        │
        ▼
SHA256 Hash: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
```

### 5.3 RudderStack Configuration

| Config Option | Value | Behavior |
|---------------|-------|----------|
| `hashEmail: true` (default) | RudderStack hashes | Send plaintext emails |
| `hashEmail: false` | User pre-hashes | Send SHA256 hashed emails |

### 5.4 Validation Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `EmailMustBeHashed` | Plaintext email sent | Enable `hashEmail` or pre-hash |
| Invalid hash format | Wrong hash algorithm or format | Use SHA256, 64 char hex |

---

## 6. Error Scenarios and Handling

### 6.1 Configuration Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| `audienceId is empty` | Missing config | Configure Audience ID in destination settings |
| `error in unmarshalling destination config` | Invalid config JSON | Check destination configuration |
| `failed to generate oauth token` | OAuth issues | Re-authorize destination |

### 6.2 Upload Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| `unable to get bulk upload url` | Auth failure or API issue | Check credentials, verify permissions |
| `unable to upload bulk file` | Network or auth error | Retry (automatic), check connectivity |
| File size exceeded | Event too large | Event will retry in smaller batch |

### 6.3 Processing Errors (from Result File)

| Error Code | Description | Resolution |
|------------|-------------|------------|
| `InvalidCustomerListId` | Audience doesn't exist | Verify Audience ID in config |
| `EmailMustBeHashed` | Unhashed email | Enable `hashEmail` setting |
| `InvalidAudienceId` | Malformed audience ID | Check format (numeric) |

### 6.4 Error Flow

```
Upload Error
    │
    ├─► File creation error
    │       │
    │       └─► All jobs → FailedJobIDs (retry)
    │
    ├─► GetBulkUploadUrl error
    │       │
    │       └─► Action jobs → FailedJobIDs (retry)
    │
    └─► UploadBulkFile error
            │
            └─► Action jobs → FailedJobIDs (retry)

Processing Error (from Result)
    │
    └─► Individual job error
            │
            └─► Job → AbortedJobIDs (no retry)
```

---

## 7. Common Use Cases

### 7.1 New Customer Onboarding

**Scenario:** Add new customers to remarketing audience

**Flow:**
1. Customer signs up on website
2. Identify event triggers audience sync
3. audienceList event with `add` action
4. User added to Customer List
5. User eligible for remarketing campaigns

**Event:**
```json
{
    "type": "audienceList",
    "properties": {
        "listData": {
            "add": [{"email": "newcustomer@example.com"}]
        }
    }
}
```

### 7.2 GDPR Right to Erasure

**Scenario:** Remove user data per GDPR request

**Flow:**
1. User requests data deletion
2. Trigger audienceList event with `remove` action
3. User removed from all synced audiences
4. Compliance documented

**Event:**
```json
{
    "type": "audienceList",
    "properties": {
        "listData": {
            "remove": [{"email": "gdpr-request@example.com"}]
        }
    }
}
```

### 7.3 Daily Audience Sync (Mirror)

**Scenario:** Maintain exact copy of warehouse segment

**Flow:**
1. Reverse ETL queries warehouse daily
2. Complete user list sent with `replace` action
3. Audience perfectly mirrors warehouse segment

**Event:**
```json
{
    "type": "audienceList",
    "properties": {
        "listData": {
            "add": [
                {"email": "active1@example.com"},
                {"email": "active2@example.com"}
            ]
        }
    }
}
```

### 7.4 High-Value Customer Targeting

**Scenario:** Target customers with high LTV

**Flow:**
1. Analytics identifies high-value customers
2. Segment synced to Bing Ads audience
3. Higher bids applied for these users
4. Improved ROAS

**Event:**
```json
{
    "type": "audienceList",
    "properties": {
        "listData": {
            "add": [
                {"email": "vip1@example.com"},
                {"email": "vip2@example.com"}
            ]
        }
    }
}
```

---

## 8. Flow Diagrams

### 8.1 Upload Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          UPLOAD FLOW                                     │
└─────────────────────────────────────────────────────────────────────────┘

START
  │
  ▼
┌─────────────────────┐
│ Get audienceId from │
│ destination config  │
└─────────────────────┘
  │
  │ audienceId empty?
  │     │
  │     YES ─────────► FAIL: "audienceId is empty"
  │     │
  │     NO
  ▼
┌─────────────────────┐
│ Open input JSON-L   │
│ file                │
└─────────────────────┘
  │
  ▼
┌─────────────────────┐
│ Create ActionFiles  │
│ for Add, Remove,    │
│ Replace             │
└─────────────────────┘
  │
  ▼
┌─────────────────────┐     ┌─────────────────────┐
│ FOR each line in    │────►│ Parse JSON          │
│ input file          │     │ Get action type     │
└─────────────────────┘     │ Get email list      │
  │                         └─────────────────────┘
  │                                   │
  │                                   ▼
  │                         ┌─────────────────────┐
  │                         │ Check size limits   │
  │                         │ fileSizeLimit: 100MB│
  │                         │ eventsLimit: 4M     │
  │                         └─────────────────────┘
  │                                   │
  │                         Exceeds? ─┼─ YES ──► Add to FailedJobIDs
  │                                   │
  │                                   NO
  │                                   │
  │                                   ▼
  │                         ┌─────────────────────┐
  │                         │ Write CSV rows      │
  │                         │ Add to SuccessJobs  │
  │                         └─────────────────────┘
  │                                   │
  │◄──────────────────────────────────┘
  │
  ▼
┌─────────────────────┐
│ Convert each CSV    │
│ to ZIP              │
└─────────────────────┘
  │
  ▼
┌─────────────────────┐
│ FOR each non-empty  │
│ ActionFile          │
└─────────────────────┘
  │
  │     ┌─────────────────────────────────────────┐
  │     │                                         │
  │     ▼                                         │
  │   ┌─────────────────────┐                     │
  │   │ GetBulkUploadUrl()  │                     │
  │   └─────────────────────┘                     │
  │     │                                         │
  │     │ Error? ── YES ──► Add to FailedJobs ────┤
  │     │                                         │
  │     NO                                        │
  │     │                                         │
  │     ▼                                         │
  │   ┌─────────────────────┐                     │
  │   │ UploadBulkFile()    │                     │
  │   └─────────────────────┘                     │
  │     │                                         │
  │     │ Error? ── YES ──► Add to FailedJobs ────┤
  │     │                                         │
  │     NO                                        │
  │     │                                         │
  │     ▼                                         │
  │   ┌─────────────────────┐                     │
  │   │ Store ImportId      │                     │
  │   │ Add to SuccessJobs  │                     │
  │   └─────────────────────┘                     │
  │     │                                         │
  │◄────┴─────────────────────────────────────────┘
  │
  ▼
┌─────────────────────┐
│ Cleanup ZIP files   │
│ Return results      │
└─────────────────────┘
  │
  ▼
END
```

### 8.2 Poll Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           POLL FLOW                                      │
└─────────────────────────────────────────────────────────────────────────┘

START
  │
  │ Input: ImportId = "abc,def,ghi"
  │
  ▼
┌─────────────────────┐
│ Split ImportIds     │
│ by comma            │
└─────────────────────┘
  │
  │ requestIds = ["abc", "def", "ghi"]
  │
  ▼
┌─────────────────────┐
│ FOR each requestId  │
└─────────────────────┘
  │
  │     ┌─────────────────────────────────────────┐
  │     │                                         │
  │     ▼                                         │
  │   ┌─────────────────────┐                     │
  │   │ GetBulkUploadStatus │                     │
  │   │ (requestId)         │                     │
  │   └─────────────────────┘                     │
  │     │                                         │
  │     │ Error? ── YES ──► StatusCode: 500       │
  │     │                   HasFailed: true       │
  │     NO                                        │
  │     │                                         │
  │     ▼                                         │
  │   ┌─────────────────────┐                     │
  │   │ Check RequestStatus │                     │
  │   └─────────────────────┘                     │
  │     │                                         │
  │     ├─ "Completed" ────► Complete: true       │
  │     │                                         │
  │     ├─ "CompletedWithErrors"                  │
  │     │       │                                 │
  │     │       └──► Complete: true               │
  │     │            HasFailed: true              │
  │     │            FailedJobParams: ResultURL   │
  │     │                                         │
  │     ├─ "InProgress"/"FileUploaded"            │
  │     │       │                                 │
  │     │       └──► InProgress: true             │
  │     │                                         │
  │     └─ "Failed"/other                         │
  │             │                                 │
  │             └──► HasFailed: true              │
  │                  StatusCode: 500              │
  │                                               │
  │◄──────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────┐
│ Aggregate results:  │
│                     │
│ If any 500 status   │
│   → StatusCode: 500 │
│                     │
│ If any InProgress   │
│   → InProgress: true│
│                     │
│ If any HasFailed    │
│   → HasFailed: true │
│                     │
│ If all Complete     │
│   → Complete: true  │
└─────────────────────┘
  │
  ▼
END
```

### 8.3 GetUploadStats Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      GET UPLOAD STATS FLOW                               │
└─────────────────────────────────────────────────────────────────────────┘

START
  │
  │ Input: FailedJobParameters = "url1,url2"
  │        ImportingList = [job1, job2, job3]
  │
  ▼
┌─────────────────────┐
│ Build initial list  │
│ [job1.ID, job2.ID,  │
│  job3.ID]           │
└─────────────────────┘
  │
  ▼
┌─────────────────────┐
│ Split result URLs   │
│ by comma            │
└─────────────────────┘
  │
  ▼
┌─────────────────────┐
│ FOR each resultURL  │
└─────────────────────┘
  │
  │     ┌─────────────────────────────────────────┐
  │     │                                         │
  │     ▼                                         │
  │   ┌─────────────────────┐                     │
  │   │ Download ZIP from   │                     │
  │   │ resultURL           │                     │
  │   └─────────────────────┘                     │
  │     │                                         │
  │     │ Error? ── YES ──► Return StatusCode 500 │
  │     │                                         │
  │     NO                                        │
  │     │                                         │
  │     ▼                                         │
  │   ┌─────────────────────┐                     │
  │   │ Extract ZIP         │                     │
  │   │ Read CSV            │                     │
  │   └─────────────────────┘                     │
  │     │                                         │
  │     ▼                                         │
  │   ┌─────────────────────┐                     │
  │   │ Parse error rows    │                     │
  │   │ (Type contains      │                     │
  │   │  "Customer List     │                     │
  │   │   Item Error")      │                     │
  │   └─────────────────────┘                     │
  │     │                                         │
  │     ▼                                         │
  │   ┌─────────────────────┐                     │
  │   │ Extract Client ID   │                     │
  │   │ Parse: jobId<<>>hash│                     │
  │   │ Get Error message   │                     │
  │   └─────────────────────┘                     │
  │     │                                         │
  │     ▼                                         │
  │   ┌─────────────────────┐                     │
  │   │ Add to abortedJobs  │                     │
  │   │ Map jobId → error   │                     │
  │   └─────────────────────┘                     │
  │                                               │
  │◄──────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────┐
│ Calculate success:  │
│                     │
│ succeededJobs =     │
│   initialList -     │
│   abortedJobs       │
└─────────────────────┘
  │
  ▼
┌─────────────────────┐
│ Return:             │
│ - AbortedKeys       │
│ - AbortedReasons    │
│ - SucceededKeys     │
└─────────────────────┘
  │
  ▼
END
```

---

## Related Documentation

- [README.md](../README.md) - Main integration documentation
- [Implementation Details](implementation.md) - Code walkthrough
