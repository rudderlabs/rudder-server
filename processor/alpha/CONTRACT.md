# Alpha Service — HTTP Contract

This is the contract the rudder-server side implements when forwarding post-user-transformation events to the alpha service. Hackathon scope, one-time demo — please implement just enough to satisfy this.

## Endpoint

- **Method:** `POST`
- **URL:** the full URL you give us. We use it verbatim — no path is appended. Example: `http://alpha.local:8080/events`. Configure on our side via the `alphaServiceUrl` config key (env var: `RSERVER_ALPHA_SERVICE_URL`).
- **Content-Type:** `application/json`
- **Auth:** none. No headers, no tokens.

## Request body — batched

Each request carries a batch of events sharing one workspace. Mirrors the Go structs you defined:

```go
type IngestEvent struct {
    UserID    string `json:"userID"`
    MessageID string `json:"messageID"`
    EventName string `json:"eventName"`
}

type EventsRequest struct {
    WorkspaceID string         `json:"workspaceID"`
    Events      []IngestEvent  `json:"events"`
}
```

Wire example:

```json
{
  "workspaceID": "2gN8z5Vc3mL4kPq6sTxR1yWb9hF",
  "events": [
    {
      "userID":    "user_42",
      "messageID": "1c5e8f2a-3d2b-4d6e-9a17-2b1e0c8c0a4d",
      "eventName": "Order Completed"
    },
    {
      "userID":    "user_77",
      "messageID": "8a1b3c5d-7e9f-4011-a223-3344556677aa",
      "eventName": "Page Viewed"
    }
  ]
}
```

| Field | Type | Notes |
|---|---|---|
| `workspaceID` | string | RudderStack workspace identifier. Always non-empty. All events in a single request share this value. |
| `events`      | array  | One or more `IngestEvent`. Always non-empty (we skip the POST if there's nothing to send). |
| `events[].userID`    | string | The end-user identifier from the original event. **Always non-empty** — we filter out events without a `userId` before batching. |
| `events[].messageID` | string | RudderStack's per-event UUID. Stable across retries of the same batch. Treat as the natural primary key. |
| `events[].eventName` | string | RudderStack event name (e.g. `"Order Completed"`). **Always non-empty** — we filter out events with an empty `eventName` before batching. |

We do **not** send the optional `properties` field. It's never present in the JSON; alpha can rely on the `omitempty` semantics.

## Response

- **`200 OK`** — we treat as success and stop retrying. Body content is ignored; an empty body is fine.
- **Anything else** (non-200 status, connection refused, timeout, TCP reset, etc.) — we treat the **entire batch** as failed and retry the whole batch.

## Retry behavior

- Up to **10 attempts** per batch.
- **30 seconds** between attempts (constant delay, no backoff).
- **60 second** per-request timeout.
- If alpha is down (connection refused / fails fast): worst case ~5 minutes per batch before we give up.
- If alpha is up but hangs every request: worst case ~14.5 minutes per batch (10×60s + 9×30s).
- After 10 failed attempts we drop the entire batch and log a warning on our side. We do **not** persist or queue past that.

This means: if you respond 5xx, we'll keep hammering you with the same batch on a 30-second cadence. If you crash mid-write and only persisted some events, the next retry will redeliver the full batch — see Idempotency below.

## Idempotency

We do **not** dedupe on our side. The same `messageID` can arrive at your service multiple times because:

1. We retry the whole batch on any non-200 response, so a request that succeeded at your end but failed to deliver the response (network blip) will be retried.
2. If a single source event is fanned out to multiple destinations in RudderStack, each post-user-transformation pass for each destination produces its own batch. The `messageID` will be the same; `eventName` *may* differ if a destination's user transformation renamed the event.

If your service needs to count each event once, dedupe on `messageID` (e.g. a Redis `SET` with TTL).

## Concurrency & volume

- Up to **4 concurrent in-flight batches** from us (we run a 4-worker pool draining a 200-batch buffer).
- Demo throughput: **trickle, 1–10 events/sec source rate**. Batches typically hold 1–20 events at peak; small batches are normal.
- One POST per source-destination pair invocation in the processor pipeline — we do **not** time-window or cross-pair accumulate.

## What we do NOT send

- No authentication headers.
- No custom headers beyond `Content-Type`.
- No `anonymousId`, no `properties`, no traits, no timestamps, no original payload — only the fields listed above.
- Empty batches: skipped entirely (no POST).

## Suggested minimal handler

A 200 OK to every well-formed POST is enough to pass the contract end-to-end:

```go
// Go example
http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
    var req EventsRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }
    log.Printf("workspace=%s events=%d", req.WorkspaceID, len(req.Events))
    w.WriteHeader(http.StatusOK)
})
```

```python
# Flask example
@app.post("/events")
def events():
    payload = request.get_json(force=True)
    # payload is {"workspaceID": "...", "events": [...]}
    print(payload["workspaceID"], len(payload["events"]))
    return ("", 200)
```

That's the entire contract. Ping me if anything is ambiguous.
