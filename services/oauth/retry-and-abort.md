## OAuth: when a job is retried and when it is aborted

This document describes how the OAuth layer (`services/oauth/v2`) decides the outcome of a request for an OAuth
destination, and how that outcome becomes a *retry* or an *abort* for the job. For the module overview see
[README.md](README.md).

### TL;DR

| Situation                                                              | Status | Outcome   |
|------------------------------------------------------------------------|--------|-----------|
| Token fetched, destination accepted it                                 | dest's | delivered |
| Destination said `REFRESH_TOKEN` and the refresh **succeeded**         | 500    | 🔁 retry  |
| Token fetch / refresh failed with `ref_token_invalid_grant` (`body.code`) | 400 | ⛔ abort  |
| Destination said `AUTH_STATUS_INACTIVE`                                | 400    | ⛔ abort  |
| Any other OAuth / Control Plane / internal failure                     | 500    | 🔁 retry  |

In the router a status is terminal when `200 <= status < 500` (except 408 and 429), see `isJobTerminated` in
[router/misc.go](../../router/misc.go). So **5xx = retry, 4xx = abort**.

"Retry" never means an immediate resend inside the OAuth layer: the job is marked failed and goes through the full
pipeline again on its next attempt (see [What "retry" means](#what-retry-means-in-the-router)).

### Building blocks

- **`NewOAuthHttpClient`** ([v2/http/client.go](v2/http/client.go)) takes a normal `*http.Client` and replaces its
  `Transport` with an `OAuthTransport`, keeping the original transport underneath for the actual network call. Callers
  just call `client.Do(req)`.
- **`OAuthTransport.RoundTrip`** ([v2/http/transport.go](v2/http/transport.go)) wraps that network call with two
  hooks, `preRoundTrip` and `postRoundTrip`. It **always returns `(resp, nil)`**: every OAuth failure is expressed as
  an HTTP status on a (possibly synthetic) response, never as a Go error.
- **`OAuthHandler`** ([v2/oauth_handler.go](v2/oauth_handler.go)) implements `FetchToken` / `RefreshToken` against
  the Control Plane (`POST {ConfigBEURL}/destination/workspaces/{ws}/accounts/{accountId}/token`), with an in-memory
  token cache and a per-account lock.
- **`oauthBreaker`** ([v2/oauth_breaker.go](v2/oauth_breaker.go)) wraps the handler with per-account circuit breakers.
- **Caller-supplied pieces**:
  - an `Augmenter` ([v2/extensions](v2/extensions)) that puts the token on the request; when it is `nil`,
    `preRoundTrip` is skipped entirely;
  - a `getAuthErrorCategory func([]byte) (string, error)` that extracts `REFRESH_TOKEN` / `AUTH_STATUS_INACTIVE` from
    the response the caller receives.

### Request lifecycle

```
caller ── client.Do(req) ──► OAuthTransport.RoundTrip
                               ├─ destination not OAuth for this flow → original transport, untouched
                               ├─ preRoundTrip   get a token, attach it to the request
                               │                 (on failure: return a synthetic response, STOP here)
                               ├─ original transport → the real HTTP call (e.g. to the transformer)
                               └─ postRoundTrip  read authErrorCategory from the response and react to it
```

#### preRoundTrip — before the call

Gets the access token via `FetchToken` and attaches it to the request with the `Augmenter`.

`FetchToken` returns the cached token unless:
- there is no cached token, or
- it expires within `refreshBeforeExpiry` (router: `Router.oauth.expirationTimeDiff`, default 5m; handler default
  1m) — a **proactive refresh**, or
- the destination config carries a newer `secretVersion` than the cached token (e.g. the user re-authorized).

In those cases it calls the Control Plane token endpoint.

**If `preRoundTrip` fails, the real HTTP call is never made in this attempt** (for the router this means
`/routerTransform` is not called and nothing is delivered). The synthetic response carries a
`TransportResponse.InterceptorResponse` with the status, message and — for `FetchToken` failures — the
Control Plane `ErrorType`.

| Case                                                     | Status    | Outcome  |
|----------------------------------------------------------|-----------|----------|
| Request body cannot be read                              | 500       | 🔁 retry |
| `FetchToken` fails with `ref_token_invalid_grant`        | 400 + `ErrorType` | ⛔ abort |
| `FetchToken` fails any other way (CP error, empty secret, unparsable response, breaker open with a non-terminal last error, …) | 500 | 🔁 retry |
| `Augment` fails                                          | 500       | 🔁 retry |

`AUTH_STATUS_INACTIVE` can **never** come out of `preRoundTrip`: it is produced by the destination integration in the
response, which does not exist yet at this point.

#### postRoundTrip — after the call

Extracts `authErrorCategory` from the response using the caller's `getAuthErrorCategory`, then rewrites the response
body as

```json
{ "originalResponse": "<raw body>", "interceptorResponse": { "statusCode": 0, "response": "" } }
```

and fills `interceptorResponse` according to the category:

| Case                                                            | `interceptorResponse.statusCode` | Outcome  |
|-----------------------------------------------------------------|---------------------------------|----------|
| Response body cannot be read                                    | 500 (synthetic response)        | 🔁 retry |
| `getAuthErrorCategory` fails (malformed response)               | 500                             | 🔁 retry |
| No category                                                     | not set — caller keeps the destination's status | per destination |
| `REFRESH_TOKEN`, secret used for the call missing from context  | 500                             | 🔁 retry |
| `REFRESH_TOKEN`, `RefreshToken` **succeeds**                    | 500                             | 🔁 retry with the new token |
| `REFRESH_TOKEN`, `RefreshToken` fails with `ref_token_invalid_grant` in `body.code` | 400       | ⛔ abort |
| `REFRESH_TOKEN`, `RefreshToken` fails any other way             | 500                             | 🔁 retry |
| `AUTH_STATUS_INACTIVE`                                          | 400                             | ⛔ abort |

A successful refresh deliberately returns 500: the request that just failed was sent with the old token, so it has to
be sent again. The new token is in the cache, so the next attempt's `preRoundTrip` picks it up.

`RefreshToken` passes the secret that was used for the call (`previousSecret`). If another worker has already rotated
the token (cached secret differs from `previousSecret` and is not expired), the cached token is returned without
calling the Control Plane, so concurrent failures for one account trigger a single refresh.

### Control Plane token errors

`getRefreshTokenFromResponse` in [v2/oauth_handler.go](v2/oauth_handler.go) maps the token endpoint's response to a
status:

| Control Plane response                                       | Status | Outcome  |
|--------------------------------------------------------------|--------|----------|
| `body.code = "ref_token_invalid_grant"` (token/user revoked) | 400    | ⛔ abort |
| `body.code = "INVALID_REFRESH_RESPONSE"` or any other code   | 500    | 🔁 retry |
| top-level `errorType` set                                    | 500    | 🔁 retry (see [known gaps](#known-gaps)) |
| empty response, unparsable response, empty secret            | 500    | 🔁 retry |

### How callers apply the outcome

#### Router — `/routerTransform` (`clientOAuthV2`, `RouterHeaderAugmenter`)

`Transform` in [router/transformer/transformer.go](../../router/transformer/transformer.go):

- **Transformer returned 200**: the interceptor status/message override the status/error **only of the jobs whose
  `authErrorCategory` is `REFRESH_TOKEN` or `AUTH_STATUS_INACTIVE`**; other jobs in the batch keep their own status.
- **Non-200** (this includes every synthetic `preRoundTrip` failure): all jobs get **500**, except 404 (kept) and
  `InterceptorResponse.ErrorType == ref_token_invalid_grant` (→ **400**, using the interceptor message). The error
  type, not the status, decides this, so an invalid grant is aborted even if the Control Plane reported it with a
  5xx-mapped shape.
- `Transform` has its own connection-retry loop, but it only fires when `client.Do` returns a Go error. Because
  `OAuthTransport` never does, OAuth failures are not retried in-process.

#### Router — transformer proxy delivery (`proxyClientOAuthV2`, no augmenter)

No augmenter, so **no `preRoundTrip`**: the transformer resolves the token itself. `ProxyRequest` uses
`InterceptorResponse.StatusCode`, when set, for the proxy status **and** for every job in the request, and
`InterceptorResponse.Response` for every job's error.

#### Regulation worker — delete flow (`HeaderAugmenter`)

[regulation-worker/internal/delete/api/api.go](../../regulation-worker/internal/delete/api/api.go) uses the
interceptor status for the job and additionally:
- `REFRESH_TOKEN` → resends the delete request **in-process** up to
  `RegulationWorker.oauth.maxRefreshRetryAttempts` times (default 1);
- `AUTH_STATUS_INACTIVE` → the regulation job is aborted.

#### Async destinations (batch router)

Salesforce Bulk Upload and Yandex Metrica build their own client with their own augmenter, `getAuthErrorCategory` and
breaker options (`BatchRouter.<DEST>.OauthBreaker.*`); how the rewritten response maps to job states is up to each
uploader. Marketo Bulk Upload does not use this module and handles token refresh in its own code.

### What "retry" means in the router

1. The job gets a 5xx status and is written back to jobsdb as `failed`.
2. The router picks it up again after an exponential backoff between `Router.minRetryBackoff` (default 10s) and
   `Router.maxRetryBackoff` (default 300s).
3. The **whole** flow runs again: `Transform` → `RoundTrip` → `preRoundTrip` → (call) → `postRoundTrip`.
4. A job keeps being retried until it succeeds or **both** `AttemptNum >= Router.maxFailedCountForJob` (default 3) **and**
   more than `Router.retryTimeWindow` (default 180m) has passed since its first attempt — then it is aborted.

All router keys can be overridden per destination type as `Router.<DEST_TYPE>.<key>`.

### Circuit breaker

Every `FetchToken` / `RefreshToken` runs inside two per-account breakers
([v2/oauth_breaker.go](v2/oauth_breaker.go)):

| Breaker | Trips when                                                     | While open                                         | Defaults |
|---------|----------------------------------------------------------------|----------------------------------------------------|----------|
| error   | `consecutiveErrorsThreshold` consecutive failures              | no CP call; returns the **last error** (same status → same retry/abort decision) | 5 errors, open for 5m |
| success | `successesThreshold` *new* tokens minted within `successesInterval` | no CP call; returns the **last token**             | 5 tokens / 1m, open for 1m |

The success breaker stops refresh storms when a destination keeps answering `REFRESH_TOKEN` for a token that is in fact
valid. Keys: `<prefix>.OauthBreaker.{enabled,consecutiveErrorsThreshold,errorsTimeout,successesThreshold,successesInterval,successesTimeout}`
with `<prefix>` = `Router.<DEST_TYPE>`, `RegulationWorker` or `BatchRouter.<DEST_TYPE>`, falling back to
`OauthBreaker.*`.

### Where `AUTH_STATUS_INACTIVE` and `REFRESH_TOKEN` come from

rudder-server never produces these categories. The destination integration in
[rudder-transformer](https://github.com/rudderlabs/rudder-transformer) sets `authErrorCategory` from the destination
API's response (async destinations do the same in their own `getAuthErrorCategory`). Common rules there:

- `401` → `REFRESH_TOKEN`; `403` → `AUTH_STATUS_INACTIVE` (shared helpers in `src/v0/util`).
- Destination-specific terminal 401s → `AUTH_STATUS_INACTIVE`, e.g. LinkedIn `REVOKED_ACCESS_TOKEN`, Google Ads
  `NOT_ADS_USER` / `CUSTOMER_NOT_FOUND`, Intercom (no refresh tokens).

### `AUTH_STATUS_INACTIVE` no longer deactivates the destination

Until [#6492](https://github.com/rudderlabs/rudder-server/pull/6492) (Nov 2025), on `AUTH_STATUS_INACTIVE` or an
`invalid_grant` the server also called
`PUT {ConfigBEURL}/workspaces/{ws}/destinations/{id}/authStatus/toggle` with `{"authStatus":"inactive"}` and evicted the
account's token from the cache. That call was removed. Today the server only:

- aborts the job (400), and
- for `AUTH_STATUS_INACTIVE`, increments `oauth_action_request{action="auth_status_inactive"}` (`ReportAuthStatusInactive`).

The `authStatus/toggle` endpoint still exists in the Control Plane but is not called by rudder-server; comments in
rudder-transformer that describe the category as "de-activate authStatus" predate this change. A re-authorized secret
is picked up through `secretVersion` instead of cache eviction.

### Known gaps

- **Top-level `errorType` invalid grant during a refresh is retried.** If the Control Plane reports
  `ref_token_invalid_grant` as a top-level `errorType` (instead of `body.code`), `getRefreshTokenFromResponse` maps it
  to 500. In `preRoundTrip` this is still aborted, because the router checks `InterceptorResponse.ErrorType`. In
  `postRoundTrip` the `ErrorType` is not propagated, so the job is retried until the router's retry limits are hit.

### Observability

- `oauth_action_request`, `oauth_action_request_sent`, `oauth_action_request_latency`, `oauth_action_total_latency`
  — tagged with `action` (`fetch_token`, `refresh_token`, `auth_status_inactive`), `isCallToCpApi`, `errorMessage`,
  destination/workspace tags and flow.
- `oauth_action_proactive_token_refresh` — proactive refresh decisions.
- `oauth_action_error_breaker_tripped`, `oauth_action_success_breaker_tripped` — breaker short-circuits per account.
- `oauth_v2_http_total_roundtrip_latency`, `oauth_v2_http_pre_roundtrip_latency`, `oauth_v2_http_roundtrip_latency`,
  `oauth_v2_http_post_roundtrip_latency` — transport phase timings.
