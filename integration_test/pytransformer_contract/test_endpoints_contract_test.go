package pytransformer_contract

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/registry"

	"github.com/rudderlabs/rudder-server/processor/usertransformer"
)

// testFlowResponse is the success envelope shared by /test and /testRun.
type testFlowResponse struct {
	TransformedEvents []map[string]any `json:"transformedEvents"`
	Logs              []string         `json:"logs"`
}

// testFlowError is the whole-execution failure body shared by all four endpoints.
type testFlowError struct {
	Error string `json:"error"`
}

// faasDeployRequest is the subset of the OpenFaaS deployment payload
// (buildOpenfaasFn in rudder-transformer) the mock gateway needs.
type faasDeployRequest struct {
	Service    string `json:"service"`
	Name       string `json:"name"`
	EnvProcess string `json:"envProcess"`
}

// dynamicFaasGateway mocks the OpenFaaS gateway for the control-plane test flow.
// Unlike newMockOpenFaaSGateway (fixed proxy target), it honours function
// deployments: POST /system/functions starts a real openfaas-flask-base
// container whose fprocess is the deployed envProcess — which is how test-mode
// inline code reaches the old architecture (`python index.py --code "..."`).
// Health checks and invocations are routed to the per-function container, and
// DELETE purges it (rudder-transformer deletes test functions after each run).
type dynamicFaasGateway struct {
	t          *testing.T
	pool       *dockertest.Pool
	server     *httptest.Server
	mu         sync.Mutex
	fnURLs     map[string]string
	containers map[string]*dockertest.Resource
}

func newDynamicFaasGateway(t *testing.T, pool *dockertest.Pool) *dynamicFaasGateway {
	t.Helper()
	g := &dynamicFaasGateway{
		t:          t,
		pool:       pool,
		fnURLs:     map[string]string{},
		containers: map[string]*dockertest.Resource{},
	}
	g.server = httptest.NewServer(http.HandlerFunc(g.handle))
	t.Cleanup(g.server.Close)
	return g
}

// register makes the gateway route /function/{name} traffic to url. The
// container may be nil for externally-managed functions (e.g. fn-ast, whose
// lifecycle is owned by the test body).
func (g *dynamicFaasGateway) register(name, url string, container *dockertest.Resource) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.fnURLs[name] = url
	if container != nil {
		g.containers[name] = container
	}
}

func (g *dynamicFaasGateway) lookup(name string) (string, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	url, ok := g.fnURLs[name]
	return url, ok
}

func (g *dynamicFaasGateway) handle(w http.ResponseWriter, r *http.Request) {
	switch {
	// Deploy function: start a flask-base container with the deployed envProcess.
	case r.Method == http.MethodPost && r.URL.Path == "/system/functions":
		var req faasDeployRequest
		if err := jsonrs.NewDecoder(r.Body).Decode(&req); err != nil {
			g.t.Errorf("DynamicFaasGateway: decoding deploy request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		name := req.Service
		if name == "" {
			name = req.Name
		}
		g.t.Logf("DynamicFaasGateway: deploying %q with fprocess %q", name, req.EnvProcess)
		url, container, err := startOpenFaasFlaskFprocess(g.t, g.pool, req.EnvProcess)
		if err != nil {
			g.t.Errorf("DynamicFaasGateway: starting flask container for %q: %v", name, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		g.register(name, url, container)
		w.WriteHeader(http.StatusOK)

	// Update function
	case r.Method == http.MethodPut && r.URL.Path == "/system/functions":
		w.WriteHeader(http.StatusOK)

	// Delete function: purge its container.
	case r.Method == http.MethodDelete && r.URL.Path == "/system/functions":
		var req struct {
			FunctionName string `json:"functionName"`
		}
		if err := jsonrs.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		g.mu.Lock()
		container := g.containers[req.FunctionName]
		delete(g.containers, req.FunctionName)
		delete(g.fnURLs, req.FunctionName)
		g.mu.Unlock()
		if container != nil {
			if err := g.pool.Purge(container); err != nil {
				g.t.Logf("DynamicFaasGateway: purging %q: %v", req.FunctionName, err)
			}
		}
		w.WriteHeader(http.StatusOK)

	// List functions
	case r.Method == http.MethodGet && r.URL.Path == "/system/functions":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))

	// Get function info
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/system/function/"):
		name := strings.TrimPrefix(r.URL.Path, "/system/function/")
		if _, ok := g.lookup(name); !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = fmt.Fprintf(w, "error finding function %s", name)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = jsonrs.NewEncoder(w).Encode(map[string]any{"name": name, "replicas": 1})

	// Health check (GET) or invoke (POST) — proxied to the function's container.
	case strings.HasPrefix(r.URL.Path, "/function/"):
		name := strings.TrimPrefix(r.URL.Path, "/function/")
		target, ok := g.lookup(name)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = fmt.Fprintf(w, "error finding function %s", name)
			return
		}
		switch r.Method {
		case http.MethodGet:
			// fwatchdog health check; 503 until the container is up.
			req, err := http.NewRequest(http.MethodGet, target+"/", nil)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			req.Header.Set("X-REQUEST-TYPE", "HEALTH-CHECK")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			defer func() { _ = resp.Body.Close() }()
			w.WriteHeader(resp.StatusCode)
			_, _ = io.Copy(w, resp.Body)
		case http.MethodPost:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			resp, err := http.Post(target+"/", "application/json", bytes.NewReader(body))
			if err != nil {
				g.t.Logf("DynamicFaasGateway: invoking %q: %v", name, err)
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			defer func() { _ = resp.Body.Close() }()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(resp.StatusCode)
			_, _ = io.Copy(w, resp.Body)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}

	default:
		g.t.Logf("DynamicFaasGateway: unhandled %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}
}

// startOpenFaasFlaskFprocess starts an openfaas-flask-base container running
// the given fprocess verbatim (as rudder-transformer deploys it). It does not
// wait for readiness — awaitFunctionReadiness polls through the gateway's
// health endpoint. Returns an error instead of failing the test because it is
// called from the gateway's HTTP handler goroutine.
func startOpenFaasFlaskFprocess(
	t *testing.T, pool *dockertest.Pool, fprocess string,
) (string, *dockertest.Resource, error) {
	t.Helper()
	const containerPort = "8080"
	cfg := newContainerConfig(t, containerPort)
	container, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "422074288268.dkr.ecr.us-east-1.amazonaws.com/rudderstack/openfaas-flask",
		// Pinned to match the production version
		Tag:  "1.13.2",
		Auth: registry.AuthConfiguration(),
		Env: []string{
			"fprocess=" + fprocess,
			"port=" + cfg.portStr(containerPort),
		},
		// The image does not EXPOSE any port, and Docker ignores port bindings
		// for unexposed ports — required for bridge networking (macOS).
		ExposedPorts: []string{containerPort + "/tcp"},
		ExtraHosts:   cfg.ExtraHosts,
		PortBindings: cfg.PortBindings,
	}, cfg.hostConfigFn)
	if err != nil {
		return "", nil, err
	}
	t.Cleanup(func() {
		// Best effort: functions deleted by rudder-transformer are already gone.
		_ = pool.Purge(container)
	})
	return cfg.url(container, containerPort), container, nil
}

// stripJSErrorPrefix removes the "Error: " prefix rudder-transformer's test
// routes surface for validation errors: they respond with the first line of
// error.stack ("Error: <message>"), while pyt returns the bare message.
func stripJSErrorPrefix(s string) string {
	return strings.TrimPrefix(s, "Error: ")
}

// TestPyTransformerTestEndpoints pins the wire contract of the four
// control-plane endpoints introduced for the Python transformation-test flow,
// comparing the new architecture against the old one:
//
//	pyt POST /test          ~ rudder-transformer POST /transformation/test
//	pyt POST /testRun       ~ rudder-transformer POST /transformation/testRun
//	pyt POST /test-library  ~ rudder-transformer POST /transformationLibrary/test
//	pyt POST /extract-libs  ~ rudder-transformer POST /extractLibs
//
// Old architecture: rudder-transformer (TRANSFORMER_TEST_MODE=true) deploys a
// per-request OpenFaaS function whose fprocess carries the inline code, invokes
// it, and deletes it; the AST routes invoke the long-lived fn-ast function. The
// dynamic mock gateway backs each deployment with a real openfaas-flask-base
// container so the inline code actually runs.
//
// New architecture: requests go through usertransformer.Client.Test/TestRun/
// TestLibrary/ExtractLibs — the same methods cpservice.Forward uses in
// production — so the test covers the exact client → pyt path.
//
// Responses are compared field-by-field except where rudder-pytransformer
// deliberately (and documentedly, see its README) diverges:
//   - /test error elements: old is {error}, pyt adds the input event's metadata.
//   - /testRun elements: old uses the transformedEvent key and no statusCode;
//     pyt uses output and adds statusCode.
//   - AST import-violation messages: pyt surfaces the runtime's wording
//     ("Import of 'os' is not allowed. ...") instead of fn-ast's
//     ("Unpermitted import(s). ...") — the runtime is the source of truth.
func TestPyTransformerTestEndpoints(t *testing.T) {
	const (
		workspaceID  = "ws-test-endpoints"
		libVersionID = "lib-mathhelper-v1"
	)

	libraryCode := "def double(x):\n    return x * 2\n"

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	// Pool.Retry lazily initializes MaxWait on first use, which is a data race
	// under the concurrent container startups below — set it up front.
	pool.MaxWait = time.Minute

	// The inline test endpoints never fetch transformation code (it arrives in
	// the request body). Libraries are fetched by rudder-transformer
	// (getLibraryCodeV1: name/handleName), openfaas-flask-base (--lvids at
	// startup: importName/code) and pyt (fetch_library: importName/code) — one
	// response body serves all three.
	configBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/transformationLibrary/getByVersionId" {
			t.Logf("ConfigBackend: unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		versionID := r.URL.Query().Get("versionId")
		if versionID != libVersionID {
			t.Logf("ConfigBackend: unknown library versionId %q", versionID)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = jsonrs.NewEncoder(w).Encode(map[string]any{
			"versionId":  versionID,
			"name":       "mathhelper",
			"handleName": "mathhelper",
			"importName": "mathhelper",
			"code":       libraryCode,
			"language":   "pythonfaas",
		})
	}))
	defer configBackend.Close()

	gateway := newDynamicFaasGateway(t, pool)

	var (
		wg                               sync.WaitGroup
		transformerURL, pyTransformerURL string
	)
	wg.Go(func() {
		transformerURL = startRudderTransformer(t, pool, configBackend.URL, gateway.server.URL,
			"TRANSFORMER_TEST_MODE=true")
	})
	wg.Go(func() {
		pyTransformerURL = startRudderPytransformer(t, pool, configBackend.URL)
	})
	wg.Go(func() {
		// The AST routes invoke the long-lived fn-ast function (versionId
		// "ast" makes flask-base load its built-in AST parser instead of
		// fetching code).
		astURL, astContainer, err := startOpenFaasFlaskFprocess(t, pool, "python index.py --vid ast")
		require.NoError(t, err, "failed to start fn-ast container")
		waitForOpenFaasFlask(t, pool, astURL)
		gateway.register("fn-ast", astURL, astContainer)
	})
	wg.Wait()

	conf := config.New()
	conf.Set("PYTHON_TRANSFORM_URL", pyTransformerURL)
	client := usertransformer.New(conf, logger.NOP, stats.NOP)
	ctx := context.Background()

	// callOld POSTs payload to a rudder-transformer test route and returns the
	// HTTP status and body. These routes were only ever called by the control
	// plane (never rudder-server), so a plain HTTP call is the faithful client.
	callOld := func(t *testing.T, path string, payload map[string]any) (int, []byte) {
		t.Helper()
		body, err := jsonrs.Marshal(payload)
		require.NoError(t, err)
		resp, err := http.Post(transformerURL+path, "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp.StatusCode, respBody
	}

	// callNew marshals payload and sends it through the given client method,
	// returning the pyt HTTP status code and response body unchanged.
	callNew := func(
		t *testing.T,
		method func(context.Context, string, []byte) (int, []byte, error),
		payload map[string]any,
	) (int, []byte) {
		t.Helper()
		body, err := jsonrs.Marshal(payload)
		require.NoError(t, err)
		statusCode, respBody, err := method(ctx, workspaceID, body)
		require.NoError(t, err)
		return statusCode, respBody
	}

	decodeFlow := func(t *testing.T, body []byte) testFlowResponse {
		t.Helper()
		var resp testFlowResponse
		require.NoError(t, jsonrs.Unmarshal(body, &resp), "body: %s", body)
		return resp
	}

	decodeError := func(t *testing.T, body []byte) string {
		t.Helper()
		var resp testFlowError
		require.NoError(t, jsonrs.Unmarshal(body, &resp), "body: %s", body)
		return resp.Error
	}

	decodeImportMap := func(t *testing.T, body []byte) map[string]any {
		t.Helper()
		var resp map[string]any
		require.NoError(t, jsonrs.Unmarshal(body, &resp), "body: %s", body)
		return resp
	}

	// compareTestBodies compares /transformation/test and pyt /test success
	// bodies. Success elements are bare transformed events on both sides. Error
	// elements are {error} on the old side and {error, metadata} on the new —
	// the error text must match, the metadata addition is pyt's documented
	// improvement.
	compareTestBodies := func(t *testing.T, oldBody, newBody []byte) {
		t.Helper()
		oldResp, newResp := decodeFlow(t, oldBody), decodeFlow(t, newBody)
		require.Len(t, newResp.TransformedEvents, len(oldResp.TransformedEvents),
			"old and new arch must return the same number of transformed events\nold: %s\nnew: %s", oldBody, newBody)
		for i, oldEl := range oldResp.TransformedEvents {
			newEl := newResp.TransformedEvents[i]
			if oldErr, isErr := oldEl["error"]; isErr && len(oldEl) == 1 {
				require.Equal(t, oldErr, newEl["error"], "error text for element %d", i)
				continue
			}
			require.Equal(t, oldEl, newEl, "transformed event %d", i)
		}
		require.Equal(t, oldResp.Logs, newResp.Logs, "logs must match")
	}

	// compareTestRunBodies compares /transformation/testRun and pyt /testRun
	// bodies. Old elements are {transformedEvent|error, metadata}; new elements
	// are {output|error, metadata, statusCode} (documented key rename +
	// statusCode addition).
	compareTestRunBodies := func(t *testing.T, oldBody, newBody []byte) {
		t.Helper()
		oldResp, newResp := decodeFlow(t, oldBody), decodeFlow(t, newBody)
		require.Len(t, newResp.TransformedEvents, len(oldResp.TransformedEvents),
			"old and new arch must return the same number of transformed events\nold: %s\nnew: %s", oldBody, newBody)
		for i, oldEl := range oldResp.TransformedEvents {
			newEl := newResp.TransformedEvents[i]
			require.Equal(t, oldEl["metadata"], newEl["metadata"], "metadata for element %d", i)
			if oldErr, isErr := oldEl["error"]; isErr {
				require.Equal(t, oldErr, newEl["error"], "error text for element %d", i)
				require.EqualValues(t, 400, newEl["statusCode"], "pyt stamps statusCode 400 on errored elements")
				continue
			}
			require.Equal(t, oldEl["transformedEvent"], newEl["output"], "transformed event %d", i)
			require.EqualValues(t, 200, newEl["statusCode"], "pyt stamps statusCode 200 on successful elements")
		}
		require.Equal(t, oldResp.Logs, newResp.Logs, "logs must match")
	}

	t.Run("test endpoint", func(t *testing.T) {
		t.Run("should run inline code and return transformed events with logs", func(t *testing.T) {
			payload := map[string]any{
				"trRevCode": map[string]any{
					"code":        "def transformEvent(event, metadata):\n    log('hello from test')\n    event['foo'] = 'bar'\n    return event",
					"codeVersion": "1",
					"language":    "pythonfaas",
				},
				"events": []map[string]any{
					{"message": map[string]any{"messageId": "m1"}, "metadata": map[string]any{"messageId": "m1"}},
					{"message": map[string]any{"messageId": "m2"}, "metadata": map[string]any{"messageId": "m2"}},
				},
			}
			oldStatus, oldBody := callOld(t, "/transformation/test", payload)
			newStatus, newBody := callNew(t, client.Test, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus)
			resp := decodeFlow(t, newBody)
			require.Len(t, resp.TransformedEvents, 2)
			for _, ev := range resp.TransformedEvents {
				require.Equal(t, "bar", ev["foo"])
			}
			compareTestBodies(t, oldBody, newBody)
		})

		t.Run("should fetch libraries from the config backend", func(t *testing.T) {
			payload := map[string]any{
				"trRevCode": map[string]any{
					"code":        "import mathhelper\ndef transformEvent(event, metadata):\n    event['doubled'] = mathhelper.double(event['value'])\n    return event",
					"codeVersion": "1",
					"language":    "pythonfaas",
				},
				"events": []map[string]any{
					{"message": map[string]any{"messageId": "m1", "value": 21}, "metadata": map[string]any{"messageId": "m1"}},
				},
				"libraryVersionIDs": []string{libVersionID},
			}
			oldStatus, oldBody := callOld(t, "/transformation/test", payload)
			newStatus, newBody := callNew(t, client.Test, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			resp := decodeFlow(t, newBody)
			require.Len(t, resp.TransformedEvents, 1)
			require.EqualValues(t, 42, resp.TransformedEvents[0]["doubled"])
			compareTestBodies(t, oldBody, newBody)
		})

		t.Run("should expose request credentials via getCredential", func(t *testing.T) {
			payload := map[string]any{
				"trRevCode": map[string]any{
					"code":        "def transformEvent(event, metadata):\n    event['secret'] = getCredential('API_KEY')\n    return event",
					"codeVersion": "1",
					"language":    "pythonfaas",
				},
				"events": []map[string]any{
					{"message": map[string]any{"messageId": "m1"}, "metadata": map[string]any{"messageId": "m1"}},
				},
				"credentials": []map[string]any{{"key": "API_KEY", "value": "secret123"}},
			}
			oldStatus, oldBody := callOld(t, "/transformation/test", payload)
			newStatus, newBody := callNew(t, client.Test, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			resp := decodeFlow(t, newBody)
			require.Len(t, resp.TransformedEvents, 1)
			require.Equal(t, "secret123", resp.TransformedEvents[0]["secret"])
			compareTestBodies(t, oldBody, newBody)
		})

		t.Run("should keep a single event's error inline with HTTP 200", func(t *testing.T) {
			payload := map[string]any{
				"trRevCode": map[string]any{
					"code":        "def transformEvent(event, metadata):\n    if event['n'] == 1:\n        raise ValueError('boom')\n    return event",
					"codeVersion": "1",
					"language":    "pythonfaas",
				},
				"events": []map[string]any{
					{"message": map[string]any{"messageId": "m0", "n": 0}, "metadata": map[string]any{"messageId": "m0"}},
					{"message": map[string]any{"messageId": "m1", "n": 1}, "metadata": map[string]any{"messageId": "m1"}},
				},
			}
			oldStatus, oldBody := callOld(t, "/transformation/test", payload)
			newStatus, newBody := callNew(t, client.Test, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)

			resp := decodeFlow(t, newBody)
			require.Len(t, resp.TransformedEvents, 2)
			var errored []map[string]any
			for _, ev := range resp.TransformedEvents {
				if errMsg, ok := ev["error"].(string); ok && errMsg != "" {
					errored = append(errored, ev)
				}
			}
			require.Len(t, errored, 1)
			require.Contains(t, errored[0]["error"], "boom")
			meta, ok := errored[0]["metadata"].(map[string]any)
			require.True(t, ok, "pyt errored elements carry the input event's metadata")
			require.Equal(t, "m1", meta["messageId"])

			compareTestBodies(t, oldBody, newBody)
		})

		t.Run("should return HTTP 400 with a top-level error for a compile error", func(t *testing.T) {
			payload := map[string]any{
				"trRevCode": map[string]any{
					// Missing colon: whole-execution compile failure.
					"code":        "def transformEvent(event, metadata)\n    return event",
					"codeVersion": "1",
					"language":    "pythonfaas",
				},
				"events": []map[string]any{
					{"message": map[string]any{"messageId": "m1"}, "metadata": map[string]any{"messageId": "m1"}},
				},
			}
			oldStatus, oldBody := callOld(t, "/transformation/test", payload)
			newStatus, newBody := callNew(t, client.Test, payload)

			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			// The wording deliberately differs: the old arch surfaces fn-ast's
			// BadCodeError (its import extraction parses the code before the
			// function is even deployed), pyt surfaces its runtime compiler's
			// message. Both must carry Python's syntax diagnosis.
			oldErr := stripJSErrorPrefix(decodeError(t, oldBody))
			newErr := decodeError(t, newBody)
			t.Logf("compile error — old: %q new: %q", oldErr, newErr)
			require.Contains(t, oldErr, "expected ':'")
			require.Contains(t, newErr, "expected ':'")
		})

		t.Run("should return the verbatim missing-code error", func(t *testing.T) {
			payload := map[string]any{
				"trRevCode": map[string]any{"codeVersion": "1", "language": "pythonfaas"},
				"events": []map[string]any{
					{"message": map[string]any{"messageId": "m1"}, "metadata": map[string]any{"messageId": "m1"}},
				},
			}
			oldStatus, oldBody := callOld(t, "/transformation/test", payload)
			newStatus, newBody := callNew(t, client.Test, payload)

			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus)
			require.Equal(t, "Invalid Request. Missing parameters in transformation code block", decodeError(t, newBody))
			require.Equal(t, stripJSErrorPrefix(decodeError(t, oldBody)), decodeError(t, newBody))
		})

		t.Run("should return the verbatim missing-events error", func(t *testing.T) {
			payload := map[string]any{
				"trRevCode": map[string]any{
					"code":        "def transformEvent(event, metadata):\n    return event",
					"codeVersion": "1",
					"language":    "pythonfaas",
				},
				"events": []map[string]any{},
			}
			oldStatus, oldBody := callOld(t, "/transformation/test", payload)
			newStatus, newBody := callNew(t, client.Test, payload)

			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus)
			require.Equal(t, "Invalid request. Missing events", decodeError(t, newBody))
			require.Equal(t, stripJSErrorPrefix(decodeError(t, oldBody)), decodeError(t, newBody))
		})

		// Known strictness divergence: pyt decodes each event's metadata into a
		// typed struct (MetadataPartial) when parsing the request body, so a
		// type violation there fails the whole request with 400 "Invalid JSON".
		// The old architecture treats metadata as opaque JSON and happily runs
		// the transformation. Production rudder-server always sends well-typed
		// metadata; this only surfaces for hand-crafted control-plane payloads.
		t.Run("should reject typed-metadata violations the old architecture accepts", func(t *testing.T) {
			code := "def transformEvent(event, metadata):\n    event['foo'] = 'bar'\n    return event"
			cases := []struct {
				name     string
				metadata any
			}{
				{"messageId as number", map[string]any{"messageId": 123}},
				{"jobId as string", map[string]any{"messageId": "m1", "jobId": "not-a-number"}},
				{"metadata as non-object", "not-an-object"},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					payload := map[string]any{
						"trRevCode": map[string]any{"code": code, "codeVersion": "1", "language": "pythonfaas"},
						"events": []map[string]any{
							{"message": map[string]any{"messageId": "m1"}, "metadata": tc.metadata},
						},
					}
					oldStatus, oldBody := callOld(t, "/transformation/test", payload)
					newStatus, newBody := callNew(t, client.Test, payload)

					require.Equal(t, http.StatusOK, oldStatus, "old architecture accepts, body: %s", oldBody)
					oldResp := decodeFlow(t, oldBody)
					require.Len(t, oldResp.TransformedEvents, 1)
					require.Equal(t, "bar", oldResp.TransformedEvents[0]["foo"],
						"old architecture runs the transformation despite the metadata type violation")

					require.Equal(t, http.StatusBadRequest, newStatus, "pyt rejects, body: %s", newBody)
					require.Contains(t, decodeError(t, newBody), "Invalid JSON")
				})
			}
		})

		// pyt-only: in the old architecture an unknown library version crashes
		// the deployed flask container at startup (its --lvids fetch fails), so
		// the request degenerates into a readiness-timeout/retry loop rather
		// than a comparable 400.
		t.Run("should return HTTP 400 when a library cannot be fetched", func(t *testing.T) {
			newStatus, newBody := callNew(t, client.Test, map[string]any{
				"trRevCode": map[string]any{
					"code":        "def transformEvent(event, metadata):\n    return event",
					"codeVersion": "1",
					"language":    "pythonfaas",
				},
				"events": []map[string]any{
					{"message": map[string]any{"messageId": "m1"}, "metadata": map[string]any{"messageId": "m1"}},
				},
				"libraryVersionIDs": []string{"unknown-library-version"},
			})
			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.NotEmpty(t, decodeError(t, newBody))
		})
	})

	t.Run("testRun endpoint", func(t *testing.T) {
		t.Run("should echo each input event's metadata with output and statusCode", func(t *testing.T) {
			payload := map[string]any{
				"codeRevision": map[string]any{
					"code":        "def transformEvent(event, metadata):\n    event['foo'] = 'bar'\n    return event",
					"language":    "pythonfaas",
					"versionId":   "v1",
					"codeVersion": "1",
				},
				"input": []map[string]any{
					{"message": map[string]any{"messageId": "m1"}, "metadata": map[string]any{"messageId": "m1", "sourceId": "s1"}},
				},
			}
			oldStatus, oldBody := callOld(t, "/transformation/testRun", payload)
			newStatus, newBody := callNew(t, client.TestRun, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			resp := decodeFlow(t, newBody)
			require.Len(t, resp.TransformedEvents, 1)
			el := resp.TransformedEvents[0]
			output, ok := el["output"].(map[string]any)
			require.True(t, ok, "pyt elements carry the transformed event under the output key")
			require.Equal(t, "bar", output["foo"])
			compareTestRunBodies(t, oldBody, newBody)
		})

		t.Run("should resolve dependencies libraries and credentials", func(t *testing.T) {
			payload := map[string]any{
				"codeRevision": map[string]any{
					"code":        "import mathhelper\ndef transformEvent(event, metadata):\n    event['doubled'] = mathhelper.double(event['value'])\n    event['secret'] = getCredential('API_KEY')\n    return event",
					"language":    "pythonfaas",
					"versionId":   "v1",
					"codeVersion": "1",
				},
				"input": []map[string]any{
					{"message": map[string]any{"messageId": "m1", "value": 21}, "metadata": map[string]any{"messageId": "m1"}},
				},
				"dependencies": map[string]any{
					"libraries":   []map[string]any{{"versionId": libVersionID}},
					"credentials": []map[string]any{{"key": "API_KEY", "value": "secret123"}},
				},
			}
			oldStatus, oldBody := callOld(t, "/transformation/testRun", payload)
			newStatus, newBody := callNew(t, client.TestRun, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			resp := decodeFlow(t, newBody)
			require.Len(t, resp.TransformedEvents, 1)
			output, ok := resp.TransformedEvents[0]["output"].(map[string]any)
			require.True(t, ok)
			require.EqualValues(t, 42, output["doubled"])
			require.Equal(t, "secret123", output["secret"])
			compareTestRunBodies(t, oldBody, newBody)
		})

		t.Run("should keep a per-event error inline with element statusCode 400", func(t *testing.T) {
			payload := map[string]any{
				"codeRevision": map[string]any{
					"code":        "def transformEvent(event, metadata):\n    raise ValueError('boom')",
					"language":    "pythonfaas",
					"versionId":   "v1",
					"codeVersion": "1",
				},
				"input": []map[string]any{
					{"message": map[string]any{"messageId": "m1"}, "metadata": map[string]any{"messageId": "m1"}},
				},
			}
			oldStatus, oldBody := callOld(t, "/transformation/testRun", payload)
			newStatus, newBody := callNew(t, client.TestRun, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			resp := decodeFlow(t, newBody)
			require.Len(t, resp.TransformedEvents, 1)
			el := resp.TransformedEvents[0]
			require.EqualValues(t, 400, el["statusCode"])
			require.Contains(t, el["error"], "boom")
			compareTestRunBodies(t, oldBody, newBody)
		})

		t.Run("should return the verbatim missing-code error", func(t *testing.T) {
			payload := map[string]any{
				"codeRevision": map[string]any{"codeVersion": "1", "language": "pythonfaas"},
				"input": []map[string]any{
					{"message": map[string]any{"messageId": "m1"}, "metadata": map[string]any{"messageId": "m1"}},
				},
			}
			oldStatus, oldBody := callOld(t, "/transformation/testRun", payload)
			newStatus, newBody := callNew(t, client.TestRun, payload)

			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus)
			require.Equal(t, "Invalid Request. Missing parameters in transformation code block", decodeError(t, newBody))
			require.Equal(t, stripJSErrorPrefix(decodeError(t, oldBody)), decodeError(t, newBody))
		})

		t.Run("should return the verbatim missing-events error", func(t *testing.T) {
			payload := map[string]any{
				"codeRevision": map[string]any{
					"code":        "def transformEvent(event, metadata):\n    return event",
					"language":    "pythonfaas",
					"versionId":   "v1",
					"codeVersion": "1",
				},
				"input": []map[string]any{},
			}
			oldStatus, oldBody := callOld(t, "/transformation/testRun", payload)
			newStatus, newBody := callNew(t, client.TestRun, payload)

			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus)
			require.Equal(t, "Invalid request. Missing events", decodeError(t, newBody))
			require.Equal(t, stripJSErrorPrefix(decodeError(t, oldBody)), decodeError(t, newBody))
		})
	})

	t.Run("test-library endpoint", func(t *testing.T) {
		t.Run("should return the import map for valid library code", func(t *testing.T) {
			payload := map[string]any{
				"code":     "import json\nimport datetime\ndef double(x):\n    return x * 2",
				"language": "pythonfaas",
			}
			oldStatus, oldBody := callOld(t, "/transformationLibrary/test", payload)
			newStatus, newBody := callNew(t, client.TestLibrary, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			require.Equal(t, map[string]any{"json": []any{}, "datetime": []any{}}, decodeImportMap(t, newBody))
			require.Equal(t, decodeImportMap(t, oldBody), decodeImportMap(t, newBody))
		})

		t.Run("should reject a non-whitelisted import with the runtime's message", func(t *testing.T) {
			payload := map[string]any{
				"code":     "import os\ndef double(x):\n    return x * 2",
				"language": "pythonfaas",
			}
			oldStatus, oldBody := callOld(t, "/transformationLibrary/test", payload)
			newStatus, newBody := callNew(t, client.TestLibrary, payload)

			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			// The wording deliberately differs: fn-ast says "Unpermitted
			// import(s). ...", pyt surfaces the runtime's message so static
			// validation and runtime can't drift.
			require.NotEmpty(t, decodeError(t, oldBody))
			require.Contains(t, decodeError(t, newBody), "Import of 'os' is not allowed.")
		})

		t.Run("should return HTTP 400 for a syntax error", func(t *testing.T) {
			payload := map[string]any{
				"code":     "def double(x)\n    return x * 2",
				"language": "pythonfaas",
			}
			oldStatus, oldBody := callOld(t, "/transformationLibrary/test", payload)
			newStatus, newBody := callNew(t, client.TestLibrary, payload)

			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			// Wording deliberately differs (fn-ast BadCodeError vs pyt's runtime
			// compiler); both must carry Python's syntax diagnosis.
			require.Contains(t, decodeError(t, oldBody), "expected ':'")
			require.Contains(t, decodeError(t, newBody), "expected ':'")
		})

		t.Run("should return the verbatim missing-code error", func(t *testing.T) {
			payload := map[string]any{"language": "pythonfaas"}
			oldStatus, oldBody := callOld(t, "/transformationLibrary/test", payload)
			newStatus, newBody := callNew(t, client.TestLibrary, payload)

			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus)
			require.Equal(t, "Invalid request. Missing code", decodeError(t, newBody))
			require.Equal(t, decodeError(t, oldBody), decodeError(t, newBody))
		})
	})

	t.Run("extract-libs endpoint", func(t *testing.T) {
		t.Run("should extract non-whitelisted imports when validation is off", func(t *testing.T) {
			payload := map[string]any{
				"code":            "import os\nimport json",
				"language":        "pythonfaas",
				"validateImports": false,
			}
			oldStatus, oldBody := callOld(t, "/extractLibs", payload)
			newStatus, newBody := callNew(t, client.ExtractLibs, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			require.Equal(t, map[string]any{"os": []any{}, "json": []any{}}, decodeImportMap(t, newBody))
			require.Equal(t, decodeImportMap(t, oldBody), decodeImportMap(t, newBody))
		})

		t.Run("should allow additional libraries under validation", func(t *testing.T) {
			payload := map[string]any{
				"code":                "import mylib\nimport json",
				"language":            "pythonfaas",
				"validateImports":     true,
				"additionalLibraries": []string{"mylib"},
			}
			oldStatus, oldBody := callOld(t, "/extractLibs", payload)
			newStatus, newBody := callNew(t, client.ExtractLibs, payload)

			require.Equal(t, http.StatusOK, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			require.Equal(t, map[string]any{"mylib": []any{}, "json": []any{}}, decodeImportMap(t, newBody))
			require.Equal(t, decodeImportMap(t, oldBody), decodeImportMap(t, newBody))
		})

		t.Run("should reject a non-whitelisted import when validation is on", func(t *testing.T) {
			payload := map[string]any{
				"code":            "import os",
				"language":        "pythonfaas",
				"validateImports": true,
			}
			oldStatus, oldBody := callOld(t, "/extractLibs", payload)
			newStatus, newBody := callNew(t, client.ExtractLibs, payload)

			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus, "old body: %s", oldBody)
			// Deliberate wording difference — see the test-library subtest above.
			require.NotEmpty(t, decodeError(t, oldBody))
			require.Contains(t, decodeError(t, newBody), "Import of 'os' is not allowed.")
		})

		t.Run("should return the verbatim missing-code error", func(t *testing.T) {
			payload := map[string]any{
				"language":        "pythonfaas",
				"validateImports": true,
			}
			oldStatus, oldBody := callOld(t, "/extractLibs", payload)
			newStatus, newBody := callNew(t, client.ExtractLibs, payload)

			require.Equal(t, http.StatusBadRequest, newStatus, "body: %s", newBody)
			require.Equal(t, oldStatus, newStatus)
			require.Equal(t, "Invalid request. Code is missing", decodeError(t, newBody))
			require.Equal(t, decodeError(t, oldBody), decodeError(t, newBody))
		})
	})
}
