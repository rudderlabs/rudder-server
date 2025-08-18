# Snowpipe Streaming in RudderStack

## Overview

Snowpipe Streaming is RudderStack's mechanism for efficiently uploading and tracking data batches to Snowflake using the Snowpipe Streaming API. It handles event grouping, channel management, error handling, and polling for job completion.

---

## Main Flow

### 1. Uploading Events
- **Event File Created:** Events are written to a file for batch upload.
- **Manager.Upload:** The upload process is initiated, reading events from the file.
- **Grouping:** Events are grouped by their target table.
- **Channel Preparation:** For each table, a channel is created (or reused) using `initializeChannelWithSchema` (which internally calls `createChannel`).
- **Event Insertion:** Events are inserted into the channel via the Snowpipe Streaming API.
- **Discards:** Invalid or unprocessable events are sent to a discards table.
- **Result:** The upload returns lists of importing and failed job IDs, along with import parameters for polling.

### 2. Polling for Status
- **Poll:** The system periodically checks the status of each import using the import ID.
- **getImportStatus:** For each channel, the status is fetched via the API. If the channel is missing (e.g., due to a Snowpipe restart), it is recreated (without schema changes) and polling continues.
- **Completion:** Once all jobs reach a terminal state (success or failure), results are aggregated and returned.

### 3. Error Handling
- **Auth/Backoff:** If authentication or authorization errors occur, backoff logic is triggered to avoid repeatedly waking up the warehouse.
- **Channel Recreation:** If a channel is not found, it is recreated as needed for the operation to continue.
- **Cleanup:** Failed imports trigger channel cleanup and statistics updates.

---

## Key Components

- **Manager.Upload:** Orchestrates the upload process, event grouping, and channel management.
- **initializeChannelWithSchema:** Ensures the channel exists and schema is up to date (not used during polling recreation).
- **createChannel:** Handles channel creation and cache updates.
- **getImportStatus:** Polls for job status, recreates channels if needed, and updates cache.
- **Backoff Logic:** Prevents repeatedly waking up the warehouse when persistent errors occur.

---

## References
- See `router/batchrouter/asyncdestinationmanager/snowpipestreaming/snowpipestreaming.go` for main logic.
- See `router/batchrouter/asyncdestinationmanager/snowpipestreaming/channel.go` for channel management details. 