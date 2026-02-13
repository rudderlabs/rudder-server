## OAuth Module (services/oauth)

This module provides RudderStack's unified OAuth implementation and shared HTTP client used by Router Transformer, the Regulation Service and the Async destinations.

Important notes:
- Only one version of OAuth implementation exists now.
- There’s a single HTTP client handling both OAuth and non‑OAuth integrations from Router Transformer, Regulation Service and Async destinations.

### Overview

The active implementation lives under `services/oauth/v2`. It exposes:
- A shared HTTP transport that transparently augments requests for OAuth destinations.
- An `OAuthHandler` that fetches and refreshes access tokens via Control Plane, with in‑memory caching and metrics.
- Lightweight context helpers to pass destination info and secrets across the HTTP pipeline.

Primary consumers: Router Transformer and the Regulation Service.

### Architecture and Components

- `v2/http/client.go`:
  - `NewOAuthHttpClient`: wraps a given `*http.Client` with `OAuthTransport`, enabling both OAuth and non‑OAuth requests.
- `v2/http/transport.go`:
  - `OAuthTransport (http.RoundTripper)`: request/response interceptor.
    - Detects if the destination is OAuth‑enabled for the current flow.
    - Pre‑roundtrip: fetches token (or uses cache) and augments the request via `extensions.Augmenter`.
    - Post‑roundtrip: interprets destination responses, handles refresh‑token and auth‑inactive cases and emits metrics.
- `v2/oauth.go`:
  - `OAuthHandler`: orchestration for token fetch/refresh, CP calls, cache, locking, and metrics.
- `v2/controlplane/cp_connector.go`:
  - `Connector`: thin client to Control Plane; adds basic auth, handles timeouts/retries metadata, normalizes responses.
- `v2/context/context.go`:
  - Helpers to embed and retrieve `backendconfig.DestinationT` and token `secret` on `context.Context`.
- `v2/utils.go`, `v2/types.go`, `v2/common/constants.go`, `v2/cache.go`, `v2/stats.go`:
  - Shared types, constants, cache interface, and small utilities.

### Request Lifecycle

1. Caller constructs an `*http.Request` and attaches `backendconfig.DestinationT` to the context.
2. The shared client’s `OAuthTransport` inspects whether the destination is OAuth for the given flow.
3. If non‑OAuth: the request is passed through unchanged.
4. If OAuth:
   - Pre‑roundtrip: `OAuthHandler.FetchToken` returns an access token (from cache or CP). The request is augmented via `extensions.Augmenter`.
   - Roundtrip: underlying transport executes the request.
   - Post‑roundtrip: response is parsed to detect `REFRESH_TOKEN` or `AUTH_STATUS_INACTIVE` categories and react accordingly (refresh token, return actionable status codes to caller). A structured interceptor payload is set on the response body to communicate outcomes.

### Control Plane Contracts

- Token fetch/refresh: `POST {ConfigBEURL}/destination/workspaces/{workspaceId}/accounts/{accountId}/token`
  - Request body may include `{ hasExpired, expiredSecret }` when refreshing.
  - Response includes `secret` with `expirationDate` and provider‑specific token JSON.

### Configuration

- `HttpClient.oauth.timeout`: Control Plane connector timeout (default 30s).
- `ConfigBEURL`: derived via `backendconfig.GetConfigBackendURL()` for Control Plane endpoints.
- Expiration skew: optional override via `WithExpirationTimeDiff` on `OAuthHandler` (defaults to 1m).

### Public APIs and Extension Points

- Shared client:
  - `v2/http.NewOAuthHttpClient(client, flow, tokenCache, backendConfig, getAuthErrorCategory, args)` returns a single client usable for both OAuth and non‑OAuth destinations.
- Transport:
  - `OAuthTransport` is injected into `client.Transport` and handles all OAuth logic.
- OAuth orchestration:
  - `OAuthHandler` methods: `FetchToken`, `RefreshToken`.
- Extensions:
  - `extensions.Augmenter` augments requests with provider‑specific auth material from the retrieved `secret`.

### Usage Example (Sketch)

```go
// Build a single client for both OAuth and non‑OAuth destinations
baseClient := &http.Client{ Transport: http.DefaultTransport }
client := v2http.NewOAuthHttpClient(
    baseClient,
    common.RudderFlowDelivery,
    tokenCache,
    backendConfig,
    detectAuthCategory, // func([]byte)(string, error) parsing destination response
    &v2http.HttpClientOptionalArgs{ Logger: logger, Locker: locker },
)

// Attach destination info to context
ctx := oauthctx.CtxWithDestInfo(context.Background(), &backendconfig.DestinationT{ /* ... */ })
req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, body)

// Perform request; transport transparently handles OAuth
resp, err := client.Do(req)
```

### Observability

- The module emits timers and counters for CP requests, total roundtrip latency, pre/post roundtrip phases, and outcome categories. Labels include flow, destination ID/type, workspace, and status codes.

### Troubleshooting

- Non‑OAuth destination: ensure `definition.auth.type` is absent or not `OAuth` for pass‑through.
- Missing account ID: `GetAccountID` validates expected keys per flow.
- Token refresh loops: verify `expirationDate` and augmentation; check Control Plane response and interceptor payload.

### Maintenance

- Single version policy: only `v2` is active; new functionality should extend `v2`.
- Shared client: do not fork per consumer; extend transport/augmenters when needed.
