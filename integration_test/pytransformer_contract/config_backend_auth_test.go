package pytransformer_contract

import (
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
)

// This file pins what CONFIG_BACKEND_HOSTED_SECRET does to the wire, both variants, against a
// mock config backend ported from ../rudder-config-backend rather than invented here (see
// configBackendAuthMock below for the file-by-file provenance).
//
// The question it answers first is the boring one: a deployment that never sets the variable
// must be indistinguishable from the build that did not know about it. "We return {} when the
// secret is unset" is an implementation detail; what a deployment cares about is that the
// request reaching the config backend is unchanged, which is what
// TestConfigBackendAuthWithoutHostedSecret asserts directly — no Authorization header at all,
// same path, same query, events transformed.
//
// The rest is the other variant: with the secret set, the header is exactly
// Basic base64("<secret>:") — the empty password is load-bearing, rudder-config-backend rejects
// any decoded token that does not hold exactly one colon — and a wrong secret surfaces as a
// retryable 503 rather than a per-event failure rudder-server would abort.

const (
	// cbAuthSecret is the secret both pytransformer and the mock config backend are given in
	// the happy path. Restricted to [A-Za-z0-9_-] on purpose: a ":" breaks the basic-auth token
	// and a "," is split apart by the config backend's comma-separated secret list.
	cbAuthSecret = "py-contract-hosted-secret"

	// cbAuthOtherSecret is a second, valid secret configured on the config backend alongside
	// cbAuthSecret. rudder-config-backend accepts a comma-separated list so a secret can be
	// rotated without a flag day; carrying two here keeps that path exercised.
	cbAuthOtherSecret = "py-contract-hosted-secret-next"

	// cbAuthTransformationRoute / cbAuthLibraryRoute are the two routes pytransformer fetches
	// from. Both are authenticated on the internal gateway, and the library one is easy to
	// forget — it is a second call site for the header.
	cbAuthTransformationRoute = "/transformation/getByVersionId"
	cbAuthLibraryRoute        = "/transformationLibrary/getByVersionId"
)

// cbAuthCode sets event["foo"] = "bar", the marker every happy-path assertion looks for.
const cbAuthCode = `
def transformEvent(event, metadata):
    event['foo'] = 'bar'
    return event
`

// cbAuthCodeUsingLibrary imports cbAuthLibraryCode, so a request using it only comes back
// transformed if the *library* fetch authenticated too — a second call site for the header.
const (
	cbAuthLibraryImportName = "cbauthlib"
	cbAuthLibraryCode       = `
def marker():
    return 'bar-from-library'
`
	cbAuthCodeUsingLibrary = `
import cbauthlib

def transformEvent(event, metadata):
    event['foo'] = cbauthlib.marker()
    return event
`
)

// TestConfigBackendAuthWithoutHostedSecret is the regression guard for the change being
// additive: with CONFIG_BACKEND_HOSTED_SECRET absent from the environment, pytransformer must
// talk to the config backend exactly as the build before it did.
//
// The assertion that carries the claim is not "it still works" — it is that every request the
// config backend received arrived with no Authorization header whatsoever. A build that sent
// `Authorization: Basic Og==` (base64 of ":") would still transform events fine against a
// permissive backend and quietly break against a strict one; only inspecting the received
// header separates the two.
func TestConfigBackendAuthWithoutHostedSecret(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	cb := newConfigBackendAuthMock(t, cbAuthSecret, cbAuthOtherSecret)

	// No CONFIG_BACKEND_HOSTED_SECRET passed at all — not empty, absent. This is the
	// pre-change deployment, and every self-hosted one.
	pyURL := startRudderPytransformer(t, pool, cb.server.URL)

	t.Run("public unauthenticated routes are reached with no Authorization header", func(t *testing.T) {
		const versionID = "cbauth-nosecret-public-v1"
		cb.setMode(cbModePublic)

		status, headers, items := sendRawTransform(t, pyURL, makeEvents(versionID, 2))

		require.Equal(t, http.StatusOK, status)
		require.Empty(t, headers.Get("X-Rudder-Should-Retry"))
		require.Len(t, items, 2)
		for _, item := range items {
			require.Equal(t, http.StatusOK, item.StatusCode, "error: %s", item.Error)
			require.Equal(t, "bar", item.Output["foo"])
		}

		// The load-bearing part. Not "the header was wrong" — the header was not there.
		seen := cb.requestsFor(cbAuthTransformationRoute, versionID)
		require.NotEmpty(t, seen, "config backend was never asked for the transformation code")
		for _, r := range seen {
			require.Empty(t, r.authorization,
				"an unset CONFIG_BACKEND_HOSTED_SECRET must send no Authorization header at all")
			require.Equal(t, versionID, r.versionID)
		}
	})

	t.Run("hosted-secret routes reject the anonymous fetch as retryable", func(t *testing.T) {
		// Pointing CONFIG_BACKEND_URL at the internal gateway and forgetting the secret. The
		// interesting question is not that it fails but *how*: a per-event 401 is terminal, and
		// rudder-server aborts every per-event status that is not 200/298, so the events would
		// be destroyed by a configuration mistake.
		const versionID = "cbauth-nosecret-authenticated-v1"
		cb.setMode(cbModeHostedSecret)

		status, headers, items := sendRawTransform(t, pyURL, makeEvents(versionID, 2))

		require.Equal(t, http.StatusServiceUnavailable, status)
		require.Equal(t, "true", headers.Get("X-Rudder-Should-Retry"))
		require.Equal(t, "config_backend_auth_failed", headers.Get("X-Rudder-Error-Reason"))
		require.Len(t, items, 2)
		for _, item := range items {
			require.Equal(t, http.StatusServiceUnavailable, item.StatusCode)
			require.Contains(t, item.Error, "401")
		}

		seen := cb.requestsFor(cbAuthTransformationRoute, versionID)
		require.NotEmpty(t, seen)
		for _, r := range seen {
			require.Empty(t, r.authorization)
			require.Equal(t, http.StatusUnauthorized, r.status)
		}
	})

	t.Run("a blocked public route surfaces as forbidden, not as auth failed", func(t *testing.T) {
		// rudder-config-backend's blockHostedPublicAccess answers 403 for a paid workspace once
		// BLOCK_PUBLIC_TRANSFORMATION_ROUTES is on. Same retryable treatment, different reason,
		// because "the secret is wrong" and "this pod never moved to the internal gateway" page
		// different people.
		const versionID = "cbauth-nosecret-blocked-v1"
		cb.setMode(cbModePublicBlocked)

		status, headers, items := sendRawTransform(t, pyURL, makeEvents(versionID, 1))

		require.Equal(t, http.StatusServiceUnavailable, status)
		require.Equal(t, "true", headers.Get("X-Rudder-Should-Retry"))
		require.Equal(t, "config_backend_forbidden", headers.Get("X-Rudder-Error-Reason"))
		require.Len(t, items, 1)
		require.Equal(t, http.StatusServiceUnavailable, items[0].StatusCode)
		require.Contains(t, items[0].Error, "403")
	})
}

// TestConfigBackendAuthWithHostedSecret is the other variant: the secret is set, so every fetch
// must carry Basic base64("<secret>:") and authenticate against a config backend that enforces
// the real check.
func TestConfigBackendAuthWithHostedSecret(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	cb := newConfigBackendAuthMock(t, cbAuthSecret, cbAuthOtherSecret)
	pyURL := startRudderPytransformer(t, pool, cb.server.URL,
		"CONFIG_BACKEND_HOSTED_SECRET="+cbAuthSecret)

	// What the config backend must receive, spelled out rather than recomputed from the same
	// helper the assertion is checking: the secret is the username and the password is empty.
	wantHeader := "Basic " + base64.StdEncoding.EncodeToString([]byte(cbAuthSecret+":"))

	t.Run("the transformation fetch authenticates", func(t *testing.T) {
		const versionID = "cbauth-secret-transformation-v1"
		cb.setMode(cbModeHostedSecret)

		status, headers, items := sendRawTransform(t, pyURL, makeEvents(versionID, 2))

		require.Equal(t, http.StatusOK, status, "items: %+v", items)
		require.Empty(t, headers.Get("X-Rudder-Should-Retry"))
		require.Len(t, items, 2)
		for _, item := range items {
			require.Equal(t, http.StatusOK, item.StatusCode, "error: %s", item.Error)
			require.Equal(t, "bar", item.Output["foo"])
		}

		seen := cb.requestsFor(cbAuthTransformationRoute, versionID)
		require.NotEmpty(t, seen)
		for _, r := range seen {
			require.Equal(t, wantHeader, r.authorization)
			require.Equal(t, http.StatusOK, r.status)
		}
	})

	t.Run("the library fetch carries the same header", func(t *testing.T) {
		// A second call site that is easy to miss. The transformation imports the library, so
		// the event only comes back transformed if the library fetch authenticated too — the
		// header assertion below is corroboration, not the only evidence.
		const (
			versionID    = "cbauth-secret-library-v1"
			libVersionID = "cbauth-secret-library-lib-v1"
		)
		cb.setMode(cbModeHostedSecret)
		cb.addTransformation(versionID, cbAuthCodeUsingLibrary)
		cb.addLibrary(libVersionID, cbAuthLibraryImportName, cbAuthLibraryCode)

		status, _, items := sendRawTransform(t, pyURL,
			makeEvents(versionID, 1, libVersionID))

		require.Equal(t, http.StatusOK, status, "items: %+v", items)
		require.Len(t, items, 1)
		require.Equal(t, http.StatusOK, items[0].StatusCode, "error: %s", items[0].Error)
		require.Equal(t, "bar-from-library", items[0].Output["foo"])

		seen := cb.requestsFor(cbAuthLibraryRoute, libVersionID)
		require.NotEmpty(t, seen, "config backend was never asked for the library")
		for _, r := range seen {
			require.Equal(t, wantHeader, r.authorization)
			require.Equal(t, http.StatusOK, r.status)
		}
	})

	t.Run("a public config backend ignores the header and still serves", func(t *testing.T) {
		// Setting the secret while still pointed at the public routes is a harmless
		// intermediate state during a repoint, and must stay harmless: the header is sent, the
		// unauthenticated route does not look at it.
		const versionID = "cbauth-secret-public-v1"
		cb.setMode(cbModePublic)

		status, _, items := sendRawTransform(t, pyURL, makeEvents(versionID, 1))

		require.Equal(t, http.StatusOK, status, "items: %+v", items)
		require.Len(t, items, 1)
		require.Equal(t, http.StatusOK, items[0].StatusCode, "error: %s", items[0].Error)
		require.Equal(t, "bar", items[0].Output["foo"])

		seen := cb.requestsFor(cbAuthTransformationRoute, versionID)
		require.NotEmpty(t, seen)
		require.Equal(t, wantHeader, seen[0].authorization)
	})
}

// TestConfigBackendAuthWithWrongHostedSecret is the unhappy path that matters operationally: a
// stale secret after a rotation. It must be retryable, so the events survive until someone
// updates the secret, exactly like the no-secret-against-a-strict-backend case.
func TestConfigBackendAuthWithWrongHostedSecret(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	cb := newConfigBackendAuthMock(t, cbAuthSecret, cbAuthOtherSecret)
	cb.setMode(cbModeHostedSecret)
	pyURL := startRudderPytransformer(t, pool, cb.server.URL,
		"CONFIG_BACKEND_HOSTED_SECRET=py-contract-hosted-secret-rotated-away")

	const versionID = "cbauth-wrongsecret-v1"
	status, headers, items := sendRawTransform(t, pyURL, makeEvents(versionID, 3))

	require.Equal(t, http.StatusServiceUnavailable, status)
	require.Equal(t, "true", headers.Get("X-Rudder-Should-Retry"))
	require.Equal(t, "config_backend_auth_failed", headers.Get("X-Rudder-Error-Reason"))
	require.Len(t, items, 3)
	for _, item := range items {
		require.Equal(t, http.StatusServiceUnavailable, item.StatusCode)
	}

	// The header was sent and was well-formed — it simply is not one of the configured
	// secrets. Without this the test could pass on a request that never carried a header.
	seen := cb.requestsFor(cbAuthTransformationRoute, versionID)
	require.NotEmpty(t, seen)
	for _, r := range seen {
		require.Equal(t,
			"Basic "+base64.StdEncoding.EncodeToString([]byte("py-contract-hosted-secret-rotated-away:")),
			r.authorization)
		require.Equal(t, http.StatusUnauthorized, r.status)
	}
}

// TestConfigBackendHostedSecretTrailingNewlineIsTrimmed covers the shape the secret actually
// arrives in on Kubernetes: mounted from a file, which commonly ends in a newline. Untrimmed it
// would 401 every fetch, and the failure would look like a wrong secret rather than a stray byte.
func TestConfigBackendHostedSecretTrailingNewlineIsTrimmed(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	cb := newConfigBackendAuthMock(t, cbAuthSecret)
	cb.setMode(cbModeHostedSecret)
	pyURL := startRudderPytransformer(t, pool, cb.server.URL,
		"CONFIG_BACKEND_HOSTED_SECRET="+cbAuthSecret+"\n")

	const versionID = "cbauth-trailing-newline-v1"
	status, _, items := sendRawTransform(t, pyURL, makeEvents(versionID, 1))

	require.Equal(t, http.StatusOK, status, "items: %+v", items)
	require.Len(t, items, 1)
	require.Equal(t, http.StatusOK, items[0].StatusCode, "error: %s", items[0].Error)
	require.Equal(t, "bar", items[0].Output["foo"])

	seen := cb.requestsFor(cbAuthTransformationRoute, versionID)
	require.NotEmpty(t, seen)
	require.Equal(t,
		"Basic "+base64.StdEncoding.EncodeToString([]byte(cbAuthSecret+":")),
		seen[0].authorization,
		"the newline must be stripped before the token is built, not encoded into it")
}

// TestConfigBackendAuthMockMatchesConfigBackend pins the port itself. The container tests above
// are only worth their runtime if the thing they authenticate against rejects and accepts the
// same inputs rudder-config-backend does, and the rules are not obvious — the single-colon check
// and the trim-and-drop-blanks list parsing are both easy to approximate wrongly.
//
// Cases and expected outcomes are taken from rudder-config-backend's own suites:
// src/__tests__/config.test.ts and src/modules/rudder-basic-auth/__tests__.
func TestConfigBackendAuthMockMatchesConfigBackend(t *testing.T) {
	basic := func(token string) string {
		return "Basic " + base64.StdEncoding.EncodeToString([]byte(token))
	}

	t.Run("hostedSecretCredentials", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			raw      string
			expected []string
		}{
			{"single secret", "s3cr3t", []string{"s3cr3t"}},
			{"comma separated list", "rotating-old,rotating-new", []string{"rotating-old", "rotating-new"}},
			{"entries are trimmed", " padded , also-padded ", []string{"padded", "also-padded"}},
			{"blank entries are dropped", "kept,,  ,also-kept", []string{"kept", "also-kept"}},
			{"unset keeps the development default", "", []string{"password"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				creds := hostedSecretCredentials(tc.raw)
				usernames := make([]string, len(creds))
				for i, c := range creds {
					usernames[i] = c.username
					require.Empty(t, c.password,
						"every hosted secret is a username with an empty password")
				}
				require.Equal(t, tc.expected, usernames)
			})
		}
	})

	t.Run("rudderKoaBasicAuth", func(t *testing.T) {
		creds := hostedSecretCredentials("s3cr3t,rotating-new")
		for _, tc := range []struct {
			name       string
			header     string
			wantReason string
		}{
			{"configured secret with an empty password", basic("s3cr3t:"), ""},
			{"any secret in the list authenticates", basic("rotating-new:"), ""},
			{"missing header", "", cbAuthMissingHeader},
			{"bearer instead of basic", "Bearer s3cr3t", cbAuthMissingHeader},
			// The single-colon rule. This is why pytransformer must send "<secret>:" and not
			// a bare "<secret>": no colon at all is rejected before any comparison happens.
			{"no colon in the decoded token", basic("s3cr3t"), cbAuthInvalidHeader},
			{"more than one colon", basic("s3cr3t:extra:colon"), cbAuthInvalidHeader},
			{"empty token", basic(""), cbAuthInvalidHeader},
			{"colon only", basic(":"), cbAuthInvalidCredentials},
			{"unknown secret", basic("not-the-secret:"), cbAuthInvalidCredentials},
			// Same length as "s3cr3t:", so it gets past the length filter and must still be
			// rejected by the constant-time comparison.
			{"same-length wrong secret", basic("s3cr3u:"), cbAuthInvalidCredentials},
			{"right secret, non-empty password", basic("s3cr3t:pw"), cbAuthInvalidCredentials},
			{"untrimmed secret in the token", basic(" s3cr3t :"), cbAuthInvalidCredentials},
		} {
			t.Run(tc.name, func(t *testing.T) {
				require.Equal(t, tc.wantReason, rudderKoaBasicAuth(creds, tc.header))
			})
		}
	})
}

// ---------------------------------------------------------------------------
// Mock config backend
// ---------------------------------------------------------------------------

// cbAuthMode selects which of rudder-config-backend's route families the mock imitates for the
// two fetch paths. The same handler serves all three because in production it is the same two
// URLs — only the router they were registered on differs, and that is a deploy-time choice a
// pytransformer pod cannot see.
type cbAuthMode int32

const (
	// cbModePublic is the deprecated public router in
	// ../rudder-config-backend/src/modules/transformations/routes.ts (unAuthenticatedRouter):
	// no authentication of any kind. This is what api.rudderlabs.com serves today and what
	// every self-hosted config backend serves.
	cbModePublic cbAuthMode = iota

	// cbModeHostedSecret is the internal gateway in
	// ../rudder-config-backend/src/modules/transformations/internalRoutes.ts, registered on
	// getDataplaneGatewayRouter, which attaches DataPlaneController.verifyHostedDataPlaneSecret
	// to every route. Same paths, so a caller only swaps the base URL — and must start sending
	// the hosted secret.
	cbModeHostedSecret

	// cbModePublicBlocked is the public router with BLOCK_PUBLIC_TRANSFORMATION_ROUTES on and
	// the versionId's workspace on a paid plan: DataPlaneController.blockHostedPublicAccess
	// throws ForbiddenError(Message.Unauthorized). The 403 is how a pod that never moved to the
	// internal gateway finds out.
	cbModePublicBlocked
)

// cbCredential is one username/password pair, mirroring the Credential type in
// ../rudder-config-backend/src/modules/rudder-basic-auth/index.ts.
type cbCredential struct {
	username string
	password string
}

// cbAuthRequest is one request the mock received, kept so a test can assert on what was
// actually on the wire rather than on what the code under test says it sends.
type cbAuthRequest struct {
	path          string
	versionID     string
	authorization string
	status        int
}

// configBackendAuthMock is a mock rudder-config-backend for the two routes pytransformer
// fetches from, ported from ../rudder-config-backend so the authentication it enforces is the
// authentication pytransformer meets in production. Ported pieces, in the order a request meets
// them:
//
//   - src/modules/transformations/internalRoutes.ts — the internal-gateway mirror of
//     /transformation/getByVersionId and /transformationLibrary/getByVersionId, registered on
//     getDataplaneGatewayRouter.
//   - src/modules/internal-gateway/router.ts — getDataplaneGatewayRouter attaches
//     DataPlaneController.verifyHostedDataPlaneSecret to every route on it.
//   - src/controllers/dataPlane.controller.ts — verifyHostedDataPlaneSecret swallows the
//     specific failure and throws UnauthenticatedError(Message.IncorrectHostedServiceSecret),
//     so every rejection reads the same regardless of which rule was broken.
//   - src/modules/rudder-basic-auth/index.ts — RudderKoaBasicAuth, the actual check (see
//     rudderKoaBasicAuth).
//   - src/config.ts — hostedSecretConfig (see hostedSecretCredentials).
//   - src/serverUtils/middlewares.ts — a thrown CustomError becomes {"message": ...} with the
//     error's own status code, and no WWW-Authenticate header.
//   - src/controllers/transformation.controller.ts and transformationLibrary.controller.ts —
//     the success bodies and the 400 for a versionId that resolves to nothing.
//
// Deliberately not ported: the workspace-token alternative on /data-plane/v1 (pytransformer
// never holds a workspace token) and the leniency of Node's base64 decoder (see
// rudderKoaBasicAuth).
type configBackendAuthMock struct {
	server *httptest.Server

	// hostedSecrets is the parsed HOSTED_SERVICE_SECRETS of the config backend being imitated.
	hostedSecrets []cbCredential

	mode atomic.Int32

	mu              sync.Mutex
	transformations map[string]string    // versionId -> code
	libraries       map[string]cbLibrary // versionId -> library
	received        []cbAuthRequest
}

type cbLibrary struct {
	importName string
	code       string
}

// newConfigBackendAuthMock starts a mock config backend whose HOSTED_SERVICE_SECRETS is the
// given list. It starts in cbModePublic; a test picks the mode it needs with setMode.
//
// Every versionId resolves — an unregistered one is served cbAuthCode — so a test only registers
// a transformation when the code itself matters (the library case). Subtests still need distinct
// versionIds, but for a different reason: pytransformer caches fetched code in its L2 cache and
// would not re-fetch, so a reused id would be answered without the config backend being asked.
func newConfigBackendAuthMock(t *testing.T, hostedServiceSecrets ...string) *configBackendAuthMock {
	t.Helper()

	cb := &configBackendAuthMock{
		hostedSecrets:   hostedSecretCredentials(strings.Join(hostedServiceSecrets, ",")),
		transformations: map[string]string{},
		libraries:       map[string]cbLibrary{},
	}

	cb.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		versionID := r.URL.Query().Get("versionId")
		authorization := r.Header.Get("Authorization")

		status, body := cb.handle(r.URL.Path, versionID, authorization)

		cb.mu.Lock()
		cb.received = append(cb.received, cbAuthRequest{
			path:          r.URL.Path,
			versionID:     versionID,
			authorization: authorization,
			status:        status,
		})
		cb.mu.Unlock()

		t.Logf("ConfigBackend: %s versionId=%q authorization=%q -> %d",
			r.URL.Path, versionID, authorization, status)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if err := jsonrs.NewEncoder(w).Encode(body); err != nil {
			t.Errorf("ConfigBackend: failed to encode response: %v", err)
		}
	}))
	t.Cleanup(cb.server.Close)

	return cb
}

// handle is the request pipeline: route match, then the mode's authentication, then the
// controller. The order matters and is the config backend's — authentication runs as middleware,
// so a request with a bad secret gets 401 even when the versionId is also unknown.
func (cb *configBackendAuthMock) handle(path, versionID, authorization string) (int, any) {
	if path != cbAuthTransformationRoute && path != cbAuthLibraryRoute {
		return http.StatusNotFound, map[string]any{"message": "Not Found"}
	}

	switch cbAuthMode(cb.mode.Load()) {
	case cbModePublic:
		// unAuthenticatedRouter: the Authorization header, present or not, is never read.
	case cbModeHostedSecret:
		if rudderKoaBasicAuth(cb.hostedSecrets, authorization) != "" {
			// verifyHostedDataPlaneSecret discards the specific reason and throws
			// UnauthenticatedError(Message.IncorrectHostedServiceSecret) -> 401.
			return http.StatusUnauthorized, map[string]any{
				"message": "Incorrect hosted workspace secret",
			}
		}
	case cbModePublicBlocked:
		// blockHostedPublicAccess -> ForbiddenError(Message.Unauthorized) -> 403. The real one
		// blocks only if the versionId's workspace resolves and is on a paid plan; here every
		// versionId resolves and the mode is the paid-workspace posture, so it always blocks.
		return http.StatusForbidden, map[string]any{"message": "Unauthorised"}
	}

	if versionID == "" {
		// TransformationController.getByVersionId: BadRequestError('versionId is required').
		return http.StatusBadRequest, map[string]any{"message": "versionId is required"}
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if path == cbAuthLibraryRoute {
		lib, ok := cb.libraries[versionID]
		if !ok {
			return http.StatusBadRequest, map[string]any{
				"message": fmt.Sprintf(
					"Transformation library not found for given version id: %s", versionID),
			}
		}
		// TransformationLibraryController.getByVersionId: the library plus importName from
		// getHandleName(). pytransformer reads importName and code.
		return http.StatusOK, map[string]any{
			"versionId":  versionID,
			"name":       lib.importName,
			"handleName": lib.importName,
			"importName": lib.importName,
			"code":       lib.code,
			"language":   "pythonfaas",
		}
	}

	code, ok := cb.transformations[versionID]
	if !ok {
		code = cbAuthCode
	}
	// TransformationController.getByVersionId: the revision DTO with secrets attached.
	// pytransformer reads code; rudder-transformer also reads language and codeVersion.
	return http.StatusOK, map[string]any{
		"versionId":      versionID,
		"name":           "Config backend auth contract test",
		"description":    "",
		"code":           code,
		"language":       "pythonfaas",
		"codeVersion":    "1",
		"secretsVersion": nil,
		"imports":        []any{},
		"secrets":        map[string]any{},
	}
}

// setMode switches which route family the mock imitates. Safe to call between subtests sharing
// one container; each subtest must use its own versionId, since pytransformer caches fetched
// code in its L2 cache and would not re-fetch.
func (cb *configBackendAuthMock) setMode(mode cbAuthMode) {
	cb.mode.Store(int32(mode))
}

func (cb *configBackendAuthMock) addTransformation(versionID, code string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.transformations[versionID] = code
}

func (cb *configBackendAuthMock) addLibrary(versionID, importName, code string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.libraries[versionID] = cbLibrary{importName: importName, code: code}
}

// requestsFor returns every request the mock received for a path and versionId. Scoped by
// versionId because subtests share a mock, and an assertion that swept in a sibling subtest's
// requests would be reporting on the wrong configuration.
func (cb *configBackendAuthMock) requestsFor(path, versionID string) []cbAuthRequest {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	var out []cbAuthRequest
	for _, r := range cb.received {
		if r.path == path && r.versionID == versionID {
			out = append(out, r)
		}
	}
	return out
}

// hostedSecretCredentials ports parseHostedServiceSecrets and hostedSecretConfig from
// ../rudder-config-backend/src/config.ts: HOSTED_SERVICE_SECRETS is a comma-separated list, each
// entry trimmed and blanks dropped, and every password is the empty string. That last detail is
// the whole reason pytransformer sends base64("<secret>:") rather than base64("<secret>").
//
// The unset case keeps the config backend's development default of a single "password" secret.
// Its startup-time throw for a set-but-all-blank value is not ported: it is a config backend
// startup concern with nothing for pytransformer to observe.
func hostedSecretCredentials(hostedServiceSecrets string) []cbCredential {
	var usernames []string
	for secret := range strings.SplitSeq(hostedServiceSecrets, ",") {
		if trimmed := strings.TrimSpace(secret); trimmed != "" {
			usernames = append(usernames, trimmed)
		}
	}
	if len(usernames) == 0 {
		usernames = []string{"password"}
	}

	credentials := make([]cbCredential, len(usernames))
	for i, username := range usernames {
		credentials[i] = cbCredential{username: username, password: ""}
	}
	return credentials
}

// The three rejection messages RudderKoaBasicAuth attaches to its UnauthenticatedError, verbatim
// from ../rudder-config-backend/src/modules/rudder-basic-auth/index.ts. They are quotations, not
// Go error strings, hence the capitalisation.
const (
	cbAuthMissingHeader      = "Authorization header is missing"
	cbAuthInvalidHeader      = "Invalid Authorization header"
	cbAuthInvalidCredentials = "Invalid credentials"
)

// rudderKoaBasicAuth ports RudderKoaBasicAuth from
// ../rudder-config-backend/src/modules/rudder-basic-auth/index.ts. It returns "" when the header
// authenticates, otherwise the message that rule would have thrown — a string rather than an
// error because it is never wrapped or compared, and because verifyHostedDataPlaneSecret
// collapses all three into one 401 body on the wire. Keeping them distinct here is what lets a
// test say *which* rule rejected a header.
//
// The rule worth naming is the colon count: the decoded token must contain exactly one ":".
// A bare "<secret>" is rejected before any secret comparison happens, which is why the empty
// password in "<secret>:" is not cosmetic.
//
// One deliberate divergence: Node's Buffer.from(x, 'base64') silently ignores characters outside
// the base64 alphabet, Go's decoder errors. Malformed base64 therefore takes the same branch here
// that garbage bytes would take there — rejected either way, and no caller sends malformed base64.
func rudderKoaBasicAuth(credentials []cbCredential, header string) string {
	if header == "" {
		return cbAuthMissingHeader
	}

	// JS: const [type, base64token] = header.split(' ') — extra segments are dropped, and a
	// header with no space leaves base64token undefined, which throws downstream. Either way
	// the request is rejected.
	parts := strings.Split(header, " ")
	if parts[0] != "Basic" || len(parts) < 2 {
		return cbAuthMissingHeader
	}

	raw, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return cbAuthInvalidHeader
	}

	token := string(raw)
	if !strings.Contains(token, ":") || strings.Index(token, ":") != strings.LastIndex(token, ":") {
		return cbAuthInvalidHeader
	}

	// Comparing every candidate of matching length without short-circuiting, as the original
	// does: length is compared first because it leaks nothing, and the survivors go through a
	// constant-time comparison whose results are summed rather than any-ed.
	claim := []byte(token)
	lengthMatches, credentialMatches := 0, 0
	for _, c := range credentials {
		want := []byte(c.username + ":" + c.password)
		if len(want) != len(claim) {
			continue
		}
		lengthMatches++
		credentialMatches += subtle.ConstantTimeCompare(want, claim)
	}
	if lengthMatches == 0 || credentialMatches < 1 {
		return cbAuthInvalidCredentials
	}
	return ""
}
