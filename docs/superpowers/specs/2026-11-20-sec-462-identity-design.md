# SEC-462: verify the dataplane identity token on cp-router's NAMESPACE path

## Why
cp-router accepts a namespace name as a hosted dataplane's identity. In `configbe/db.go` `GetConnectionIDFromToken`, when `tokenType == NAMESPACE` it returns the peer's own string after an existence check. Namespace names aren't secret (`free-us-1` is hardcoded in two repos). Anyone who reaches the tunnel can join the routing pool for a namespace and, once the real connection drops, receive its control-plane traffic, including data-retention bucket credentials. P1 (cp-router#390, live in 0.26.1) stopped the "most labels wins" takeover, but impersonation stays open until this lands.

The IP allowlist (SEC-585 Phase B) is the interim mitigation. This fix is what lets the allowlist be removed, so self-hosted users stop being blocked. It also covers peers inside allowed networks, which the allowlist can't.

## Goals
- A peer on the NAMESPACE path proves which control-plane namespace it belongs to with the SEC-471 dataplane identity token, and cp-router binds the connection to *that* namespace only.
- Zero dropped hosted connections during rollout: a `log` mode that verifies and counts but never rejects, then `enforce`.
- Visibility: a metric of verification outcomes per service and reason.

## Non-goals
- Any change to the WORKSPACE_TOKEN path (self-hosted dataplanes).
- Changes to the identity system itself (issuers, registry, token minting). SEC-471 owns those; cp-router consumes them.
- Removing `HOSTED_SERVICE_SECRET` from cp-router (follow-up once enforce is on).

## Anti-goals
- A hosted dataplane loses its tunnel because enforcement came on before it sends a valid token.
- Self-hosted users see any change.
- cp-router's identity checks drift from config-backend's (a token one accepts and the other rejects).

## Approaches considered
1. **Shared `HOSTED_SERVICE_SECRET` alongside the namespace.** Small, but the secret lets any holder claim *any* namespace. cp-router and the dataplanes read it from different Vault entries that nothing keeps in sync. SEC-471/SEC-966 are moving routes off it. Rejected.
2. **New per-namespace secrets (original P2–P5).** New config-backend table, issuance, rotation, Vault seeding. Duplicates what SEC-471 already built. Rejected.
3. **SEC-471 identity token (chosen).** Per-dataplane, short-lived, Kubernetes-issued tokens already reach every hosted dataplane. config-backend and dataplane-gateway already verify them, and the registry already maps (issuer, k8s namespace) → control-plane namespace. cp-router reuses the same config and checks.

## Design

### Protocol (`proto/common/dpauth.proto` in cp-router, rudder-server and rudder-sources)
Add an optional field to `GetConnectionTokenSuccessResponse`:
```proto
string identityToken = 6;
```
Backward compatible: old peers omit it, old cp-routers ignore it.

### Dataplanes
- **rudder-server**: when `tokenType == NAMESPACE` and `DATAPLANE_IDENTITY_TOKEN_FILE` is set, read the token through `rudder-cp-sdk` `tokenfile` (as `backend-config/namespace_config.go` already does) and put it in `identityToken`. Read it fresh on **every** handshake, since tokens live ≤600 s. Applies to both callers: `processor/cpservice/connection.go` and `warehouse/api/grpc.go`. `controlplane.AuthInfo` gets a token *source* (a function), not a fixed string, so reconnects re-read the file.
- **rudder-sources**: the same, in `grpc/deployment.go` / `controlplane/auth.go`. #3661 already reads the token for dataplane-gateway; reuse that reader.
- If the file is unset or unreadable, send the handshake without a token. cp-router counts that as `missing` and, in `log` mode, still accepts.

### cp-router
- **Config:** read the same `DATAPLANE_IDENTITY_CONFIG_FILE` format config-backend parses (`audience`, `issuers` with `jwksUri`, `registry`), mounted from the same rendered source in rudder-devops. Mode from `DATAPLANE_IDENTITY_MODE` = `off` | `log` | `enforce` (default `off`, then `log`).
- **Verification** (Go port of `rudder-config-backend` `src/authentication/dataplaneIdentityVerifier.ts`, same checks and failure reasons):
  - issuer must be configured, with keys only from that issuer's configured JWKS URL
  - signature valid, `aud` equals the configured audience
  - lifetime ≤600 s, no future `iat`, 30 s clock skew allowed
  - `kubernetes.io` pod binding present
  - registry lookup of (issuer, k8s namespace) gives the control-plane namespace, and the service account matches
  - **cp-router-specific:** the registry's namespace must equal the namespace the peer claims in `connectionToken`, otherwise `wrong_namespace`
  - JWKS caching as config-backend does: 10 min max age, 30 s cooldown, 5 s timeout, last good key set kept for up to 24 h while the endpoint fails
- **Decision on the NAMESPACE path:**

| Token | `log` | `enforce` |
| --- | --- | --- |
| valid, namespace matches | accept, `result="ok"` | accept |
| missing | accept, `result="missing"` | reject |
| invalid (any reason) | accept, `result="rejected", reason=…` + warn log | reject |

  The free-tier form (connection token equal to `HOSTED_SERVICE_SECRET`, mapped to `free-us-1`) goes through the same check.
- **Metric:** `cp_router_dataplane_identity_total{service, result, reason}`. `service` is checked against the fixed list first, and `reason` comes from the fixed set above, so peers can't add label values.
- **Logs:** a warning per failure with connection ID, service, reason, issuer and `sub`. Never the token.
- Verification runs once per handshake. Long-lived tunnels aren't re-checked.

### rudder-devops
- Mount the identity config into cp-router and set `DATAPLANE_IDENTITY_MODE`, reusing the render config-backend uses (`helm-charts/control-plane/charts/config-backend/templates/deployment.yaml`, `scripts/dataplane-identity/`).
- Network: cp-router must reach each issuer's JWKS URL. Check the egress and Cilium policy.

### Rollout
1. cp-router release with the verifier, `off` by default. Then `log` in staging, then prod.
2. rudder-server and rudder-sources releases that send the token.
3. Those releases reach the hosted dataplanes. This rides SEC-471's rollout, which already needs the same upgrades and the toggle in each dataplane's CR.
4. Watch `result` per service and region until `missing` and `rejected` stay at 0 for 7 days.
5. `DATAPLANE_IDENTITY_MODE=enforce`, staging then prod. Rollback: back to `log`.
6. Then remove the SEC-585 IP allowlist.

### Tests
- cp-router: unit tests for each verification reason and each row of the decision table, using a local JWKS and minted test tokens. An integration test (extend `integration_test/labels_test.go`): an impostor sends a namespace with no token, and a token for a different namespace; both rejected in `enforce`, both counted in `log`.
- Parity: run config-backend's identity test cases against the Go verifier, so the two can't drift.
- rudder-server and rudder-sources: NAMESPACE handshakes carry a freshly read token; WORKSPACE_TOKEN handshakes carry none.

## Open, to check while building
- Whether any self-hosted dataplane uses the NAMESPACE token type (expected none; the `log` metric will show it).
- Which hosted renders don't set `DATAPLANE_IDENTITY_TOKEN_FILE` (SEC-471's rollout list covers this).

## Left to the builder
- Go JWT/JWKS library choice (cp-router has none today), file layout, log wording.
- Whether the verifier lives in cp-router or in a small shared Go package for reuse.
- A Grafana panel for the new metric next to the duplicate-connections metric (recommended).
