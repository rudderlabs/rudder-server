# Alpha Service — HTTP Contract

This is the contract the rudder-server side implements when forwarding post-user-transformation events to the alpha service. Hackathon scope, one-time demo — please implement just enough to satisfy this.

## Endpoint

- **Method:** `POST`
- **URL:** the full URL you give us. We use it verbatim — no path is appended. Example: `http://alpha.local:8080/events`. Configure on our side via env var `ALPHA_SERVICE_URL`.
- **Content-Type:** `application/json`
- **Auth:** none. No headers, no tokens.

## Request body

A single JSON object per event:

```json
{
  "eventName":   "Order Completed",
  "messageId":   "1c5e8f2a-3d2b-4d6e-9a17-2b1e0c8c0a4d",
  "workspaceId": "2gN8z5Vc3mL4kPq6sTxR1yWb9hF",
  "userId":      "user_42"
}
```

| Field | Type | Notes |
|---|---|---|
| `eventName`   | string | RudderStack event name (e.g. `"Order Completed"`, `"Page Viewed"`). May be empty for some event types. |
| `messageId`   | string | RudderStack's per-event UUID. Stable across retries of the same event. Treat as the natural primary key. |
| `workspaceId` | string | RudderStack workspace identifier. Always non-empty. |
| `userId`      | string | The end-user identifier from the original event. **Always non-empty** — we filter out events without a `userId` before sending. |

No other fields. No nesting. No metadata wrapper.

## Response

- **`200 OK`** — we treat as success and stop retrying. Body content is ignored; an empty body is fine.
- **Anything else** (non-200 status, connection refused, timeout, TCP reset, etc.) — we treat as failure and retry.

## Retry behavior (so you know what to expect from us)

- Up to **10 attempts** per event.
- **30 seconds** between attempts (constant delay, no backoff).
- **5 second** per-request timeout.
- Worst case: ~5 minutes per event before we give up. Designed to survive an alpha-service restart.
- After 10 failed attempts we drop the event and log a warning on our side. We do **not** persist or queue past that.

This means: if you restart your service, expect duplicate deliveries of in-flight events. If you respond 5xx, we'll keep hammering you on a 30-second cadence until either you return 200 or we hit 10 attempts.

## Idempotency

We do **not** dedupe on our side. The same `messageId` can arrive at your service multiple times because:

1. We retry on non-200 responses, so a request that succeeds at your end but fails to deliver the response (network blip) will be retried.
2. If a single source event is fanned out to multiple destinations in RudderStack, each post-user-transformation pass will produce its own POST. The `messageId` will be the same; `eventName` *may* differ if a destination's user transformation renamed the event.

If your service needs to count each event once, dedupe on `messageId` (e.g. a Redis `SET` with TTL).

## Concurrency & volume

- Up to **4 concurrent in-flight requests** from us.
- Demo throughput: **trickle, 1–10 events/sec**.
- One request per event (no batching).

## What we do NOT send

- No authentication headers.
- No custom headers beyond `Content-Type`.
- No `anonymousId`, no event properties, no traits, no timestamps, no original payload — only the four fields listed above.
- No batch endpoint — every event is its own POST.

## Suggested minimal handler

A 200 OK to every well-formed POST is enough to pass the contract end-to-end:

```python
# Flask example
@app.post("/events")
def events():
    payload = request.get_json(force=True)
    # payload is {"eventName", "messageId", "workspaceId", "userId"}
    print(payload)
    return ("", 200)
```

```javascript
// Express example
app.post("/events", express.json(), (req, res) => {
  console.log(req.body);
  res.sendStatus(200);
});
```

That's the entire contract. Ping me if anything is ambiguous.
