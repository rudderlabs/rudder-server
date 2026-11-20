# SEC-462 rudder-server: send the identity token on the cp-router handshake — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** On NAMESPACE handshakes with cp-router, the processor and warehouse send the SEC-471 identity token, read fresh from `DATAPLANE_IDENTITY_TOKEN_FILE` on every handshake.

**Architecture:** `controlplane.AuthInfo` gains a token *source* (`func() string`) that `authService.GetConnectionToken` calls per handshake and puts in the new proto field. `deployment` builds that source once from the env (reusing `rudder-cp-sdk/tokenfile`, as `backend-config/namespace_config.go` does) and both callers use it.

**Tech Stack:** Go, `github.com/rudderlabs/rudder-cp-sdk/tokenfile` (already a dependency, v1.6.0), protoc via `make proto`.

**Spec:** `docs/superpowers/specs/2026-11-20-sec-462-identity-design.md` (approved 2026-11-20), section "Dataplanes". cp-router side: rudderlabs/cp-router#409.

**Repo / branch:** `~/Documents/Code/rudderstack/rudder-server`, new worktree on `feature/sec-462-identity-token` from `origin/master` (local checkout is stale).

## Global Constraints
- Proto: `GetConnectionTokenSuccessResponse` gains `string identityToken = 6;` — same name and number as cp-router#409.
- Send the token only when `tokenType == "NAMESPACE"` and `DATAPLANE_IDENTITY_TOKEN_FILE` is set. WORKSPACE_TOKEN (self-hosted/dedicated) handshakes carry no token.
- Read through `tokenfile.Token.Get()` on every handshake. It re-reads the file once the cached token is 10 s old and never returns empty after one successful `Load`.
- An unset env, or a file that can't be read: the handshake still succeeds without a token (cp-router counts it as `missing`). Never fail the handshake or the process because of the token.
- Never log the token.
- No change to `GetWorkspaceToken` (the legacy fallback RPC).

## Review Focus
1. **Token rotated between two reconnects** → the second handshake carries the new token, not a value captured at startup. Test in Task 2.
2. **File missing at startup, appears later** (projected volume mounted late) → first handshakes send no token; once the file is readable, later handshakes carry it. Test in Task 2.
3. **WORKSPACE_TOKEN deployment with the env set by mistake** → no token sent. Test in Task 2.
4. **Token value in logs** (e.g. `%+v` of `AuthInfo`) → never printed. Test in Task 3 by asserting the connector's logged fields.
5. **Warehouse and processor both connect from one pod** → both send a token; they may share one `tokenfile.Token`. Test in Task 3.

---

### Task 1: Proto field

**Files:**
- Modify: `proto/common/dpauth.proto`
- Regenerate: `proto/common/dpauth.pb.go` (`make proto`)
- Create: `docs/superpowers/specs/2026-11-20-sec-462-identity-design.md` (copied from cp-router#409's branch)

**Interfaces:**
- Produces: `GetConnectionTokenSuccessResponse.IdentityToken` / `GetIdentityToken() string`.

- [ ] **Step 1:** Add `string identityToken = 6;` to `GetConnectionTokenSuccessResponse`; run `make proto`. Keep only the dpauth changes (revert header-only churn in other generated files; note the protoc header line if it changes).
- [ ] **Step 2:** `go build ./... && go test ./controlplane/...` → PASS.
- [ ] **Step 3:** Commit: `feat(proto): add identityToken to the cp-router handshake (SEC-462)`

### Task 2: Token source in the handshake

**Files:**
- Modify: `controlplane/auth.go` (`AuthInfo`, `GetConnectionToken`)
- Modify: `utils/types/deployment/deployment.go` (new function)
- Test: `controlplane/auth_test.go` (create if absent), `utils/types/deployment/deployment_test.go`

**Interfaces:**
- Produces:
  - `AuthInfo.IdentityToken func() string` — nil means "send none".
  - `func IdentityTokenSource(tokenType string) func() string` in `deployment`: returns nil unless `tokenType == "NAMESPACE"` and `DATAPLANE_IDENTITY_TOKEN_FILE` is set; otherwise returns `tok.Get` for a `tokenfile.New(path)` it calls `Load()` on once (a `Load` error is logged without the path's content and ignored, so a late-mounted file is picked up by `Get`).
  - `deployment.NamespaceTokenType` exported constant (= `"NAMESPACE"`) if callers need it.

- [ ] **Step 1: Failing tests**
  - `TestGetConnectionToken_SendsIdentityToken`: `AuthInfo{TokenType: "NAMESPACE", IdentityToken: func() string { return "t1" }}` → response `GetIdentityToken() == "t1"`.
  - `TestGetConnectionToken_ReadsTokenPerHandshake`: source returns `"t1"` then `"t2"` → two calls give `"t1"`, `"t2"`.
  - `TestGetConnectionToken_NoSource`: nil source → `GetIdentityToken() == ""`, other fields unchanged.
  - `TestIdentityTokenSource`: table — WORKSPACE_TOKEN + env set → nil; NAMESPACE + env unset → nil; NAMESPACE + file `"abc\n"` → source returns `"abc"`; NAMESPACE + missing file → source returns `""`, then after writing the file and advancing past 10 s (or using a fresh read) returns the token.
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement. `GetConnectionToken` sets `IdentityToken` only when the source is non-nil.
- [ ] **Step 4:** `go test ./controlplane/... ./utils/types/deployment/...` → PASS.
- [ ] **Step 5:** Commit: `feat(controlplane): send the identity token on NAMESPACE handshakes (SEC-462)`

### Task 3: Wire processor and warehouse

**Files:**
- Modify: `processor/cpservice/connection.go` (`NewConnector`)
- Modify: `warehouse/api/grpc.go` (connection manager setup, ~line 150)
- Test: `processor/cpservice/connection_test.go`, `warehouse/api/grpc_test.go` (extend existing)

**Interfaces:**
- Consumes: `deployment.IdentityTokenSource(tokenType)`, `AuthInfo.IdentityToken`.

- [ ] **Step 1: Failing tests**
  - Processor: with `DEPLOYMENT_TYPE=MULTITENANT`, `WORKSPACE_NAMESPACE=ns`, `DATAPLANE_IDENTITY_TOKEN_FILE=<tmp file>`, `NewConnector` produces an `AuthInfo` whose `IdentityToken()` returns the file's token; with `DEPLOYMENT_TYPE=DEDICATED` it is nil.
  - Warehouse: the same two cases on the gRPC setup's connection manager.
  - Neither component logs the token: build with a capturing logger and assert no logged field equals the token.
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Set `IdentityToken: deployment.IdentityTokenSource(tokenType)` in both `AuthInfo` literals.
- [ ] **Step 4:** `go test ./processor/cpservice/... ./warehouse/api/...` and `make lint` (or `golangci-lint run ./controlplane/... ./processor/cpservice/... ./warehouse/api/... ./utils/types/deployment/...`) → PASS.
- [ ] **Step 5:** Commit: `feat: processor and warehouse send the identity token to cp-router (SEC-462)`

---

## Not in this plan
- rudder-sources (same change in its own repo).
- rudder-devops: cp-router identity config and `DATAPLANE_IDENTITY_MODE=log`; the dataplane CRs already set `DATAPLANE_IDENTITY_TOKEN_FILE` through SEC-471's operator toggle.
