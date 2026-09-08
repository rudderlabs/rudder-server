// Package rudderauth provides a rudder-go-kit-style docker resource helper for
// the rudder-auth container that async destination integration tests use to
// resolve OAuth tokens. It mirrors the rudder-go-kit testhelper/docker/resource
// convention: Setup(pool, cleaner, opts...) (*Resource, error).
package rudderauth

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
)

const (
	defaultImage = "422074288268.dkr.ecr.us-east-1.amazonaws.com/rudderstack/rudder-auth"
	defaultTag   = "develop"
	defaultPort  = "3033/tcp"

	clientTimeout = 30 * time.Second
)

// Resource is a running rudder-auth container.
type Resource struct {
	URL  string // base URL, e.g. http://localhost:3000
	Port string // mapped host port

	httpClient *http.Client // created once with the resource; reused by RefreshToken
}

type config struct {
	image string
	tag   string
	port  string
	env   map[string]string
}

// Opt configures Setup.
type Opt func(*config)

// WithImage overrides the container image (default: the rudder-auth ECR image).
func WithImage(image string) Opt { return func(c *config) { c.image = image } }

// WithTag overrides the image tag (default: "develop").
func WithTag(tag string) Opt { return func(c *config) { c.tag = tag } }

// WithPort overrides the container port in "port/tcp" form (default: "3033/tcp").
func WithPort(port string) Opt { return func(c *config) { c.port = port } }

// WithEnv sets the OAuth app credentials the container needs to perform a
// refresh, keyed by the container env var name the rudder-auth image expects
// (e.g. "BINGADS_OFFLINE_CONVERSIONS_CLIENT_ID"). Repeated calls merge.
func WithEnv(env map[string]string) Opt {
	return func(c *config) {
		c.env = lo.Assign(c.env, env)
	}
}

// Setup starts the rudder-auth container (assumed already present locally — it is
// NOT pulled here; the CI job pulls it beforehand), waits for /health, registers
// teardown with cln, and returns the running resource.
func Setup(pool *dockertest.Pool, cln resource.Cleaner, opts ...Opt) (*Resource, error) {
	conf := &config{image: defaultImage, tag: defaultTag, port: defaultPort}
	for _, opt := range opts {
		opt(conf)
	}

	env := make([]string, 0, len(conf.env))
	for key, value := range conf.env {
		if value == "" {
			return nil, fmt.Errorf("empty value for rudder-auth container env %q", key)
		}
		env = append(env, key+"="+value)
	}

	container, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   conf.image,
		Tag:          conf.tag,
		ExposedPorts: []string{conf.port},
		Env:          env,
	}, func(hc *docker.HostConfig) {
		hc.AutoRemove = true
		hc.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		return nil, fmt.Errorf("running rudder-auth container: %w", err)
	}
	cln.Cleanup(func() {
		if purgeErr := pool.Purge(container); purgeErr != nil {
			cln.Logf("purging rudder-auth container: %v", purgeErr)
		}
	})

	hostPort := container.GetPort(conf.port)
	res := &Resource{
		URL:        fmt.Sprintf("http://localhost:%s", hostPort),
		Port:       hostPort,
		httpClient: &http.Client{Timeout: clientTimeout},
	}

	if pool.MaxWait == 0 {
		pool.MaxWait = 2 * time.Minute
	}
	if err := pool.Retry(func() error {
		req, reqErr := http.NewRequestWithContext(context.Background(), http.MethodGet, res.URL+"/health", nil)
		if reqErr != nil {
			return reqErr
		}
		resp, doErr := res.httpClient.Do(req)
		if doErr != nil {
			return doErr
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode < 200 || resp.StatusCode >= 400 {
			return fmt.Errorf("rudder-auth health status %d", resp.StatusCode)
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("waiting for rudder-auth to be healthy: %w", err)
	}

	return res, nil
}

// AccountDefinition describes an OAuth account for a rudder-auth v1 refresh.
type AccountDefinition struct {
	Type     string `json:"type"`
	Category string `json:"category"`
	Name     string `json:"name"`
}

// RefreshRequest describes a token refresh against rudder-auth.
type RefreshRequest struct {
	// Version selects the rudder-auth refresh API: "v0" or "v1". When empty it
	// defaults to "v1" if AccountDefinition is set, otherwise "v0".
	Version string

	RefreshToken string

	// Destination is the v0 path segment, e.g. "bingads_offline_conversions".
	Destination string

	// AccountDefinition and ProviderFields are used by the v1 API.
	AccountDefinition *AccountDefinition
	ProviderFields    map[string]string
}

func (r RefreshRequest) version() string {
	if r.Version != "" {
		return r.Version
	}
	if r.AccountDefinition != nil {
		return "v1"
	}
	return "v0"
}

// Secret is the flat set of string fields rudder-auth returns from a refresh.
type Secret map[string]string

// AccessToken returns the resolved access token (accessToken, or access_token).
func (s Secret) AccessToken() string {
	if v := s["accessToken"]; v != "" {
		return v
	}
	return s["access_token"]
}

// DeveloperToken returns the developer token if rudder-auth included one.
func (s Secret) DeveloperToken() string { return s["developerToken"] }

// RefreshToken exchanges a refresh token for a fresh secret (access token, and
// optionally a developer token) via this rudder-auth instance. It mirrors the
// rudder-transformer live suite's OAuthTokenResolver. For an already-running
// instance, construct a Resource directly: (&Resource{URL: url}).RefreshToken(...).
func (r *Resource) RefreshToken(ctx context.Context, req RefreshRequest) (Secret, error) {
	baseURL := strings.TrimRight(r.URL, "/")
	if r.httpClient == nil {
		r.httpClient = &http.Client{Timeout: clientTimeout}
	}

	var url string
	var body any
	switch req.version() {
	case "v0":
		if req.Destination == "" {
			return nil, fmt.Errorf("destination is required for a v0 rudder-auth refresh")
		}
		url = fmt.Sprintf("%s/tokens/destination/%s/refresh", baseURL, req.Destination)
		body = map[string]string{"refreshToken": req.RefreshToken}
	case "v1":
		if req.AccountDefinition == nil {
			return nil, fmt.Errorf("accountDefinition is required for a v1 rudder-auth refresh")
		}
		secret := lo.Assign(map[string]string{"refreshToken": req.RefreshToken}, req.ProviderFields)
		url = baseURL + "/auth/v1/refresh"
		body = map[string]any{
			"accountDefinition": req.AccountDefinition,
			"account":           map[string]any{"secret": secret, "options": map[string]any{}},
		}
	default:
		return nil, fmt.Errorf("unknown rudder-auth oauth version %q (want v0 or v1)", req.version())
	}

	payload, err := jsonrs.Marshal(body)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("calling rudder-auth %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("rudder-auth refresh %s failed: status %d: %s", url, resp.StatusCode, string(respBody))
	}

	var raw map[string]any
	if err := jsonrs.Unmarshal(respBody, &raw); err != nil {
		return nil, fmt.Errorf("unmarshalling rudder-auth response: %w", err)
	}
	secret := make(Secret, len(raw))
	for k, v := range raw {
		if s, ok := v.(string); ok {
			secret[k] = s
		}
	}
	if secret.AccessToken() == "" {
		return nil, fmt.Errorf("rudder-auth %s returned no access token: %s", url, string(respBody))
	}
	return secret, nil
}
