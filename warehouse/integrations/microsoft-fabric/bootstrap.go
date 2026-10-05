package microsoftfabric

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
)

const (
	fabricAPIScope = "https://api.fabric.microsoft.com/.default"
	fabricAPIHost  = "https://api.fabric.microsoft.com"
	bootstrapTTL   = 24 * time.Hour
)

type bootstrapError struct {
	StatusCode int
	ErrorCode  string
	RequestID  string
	Retryable  bool
}

func (e *bootstrapError) Error() string {
	message := fmt.Sprintf("spn_token_bootstrap: Fabric API request failed with HTTP %d", e.StatusCode)
	if e.ErrorCode != "" {
		message += ", errorCode=" + e.ErrorCode
	}
	if e.RequestID != "" {
		message += ", requestId=" + e.RequestID
	}
	message += fmt.Sprintf(", retryable=%t", e.Retryable)
	if !e.Retryable {
		message += "; enable 'Service principals can use Fabric APIs' and grant the service principal the required workspace role"
	}
	return message
}

type bootstrapAPIError struct {
	ErrorCode   string `json:"errorCode"`
	RequestID   string `json:"requestId"`
	IsRetriable bool   `json:"isRetriable"`
	Error       *struct {
		ErrorCode   string `json:"errorCode"`
		RequestID   string `json:"requestId"`
		IsRetriable bool   `json:"isRetriable"`
	} `json:"error"`
}

type bootstrapCall struct {
	done chan struct{}
	err  error
}

type bootstrapper struct {
	newCredential func(tenantID, clientID, clientSecret string) (azcore.TokenCredential, error)
	client        *http.Client
	endpoint      string
	now           func() time.Time
	ttl           time.Duration

	mu       sync.Mutex
	success  map[string]time.Time
	inflight map[string]*bootstrapCall
}

func newBootstrapper(client *http.Client) *bootstrapper {
	if client == nil {
		client = http.DefaultClient
	}
	return &bootstrapper{
		newCredential: func(tenantID, clientID, clientSecret string) (azcore.TokenCredential, error) {
			return azidentity.NewClientSecretCredential(tenantID, clientID, clientSecret, nil)
		},
		client:   client,
		endpoint: fabricAPIHost,
		now:      time.Now,
		ttl:      bootstrapTTL,
		success:  make(map[string]time.Time),
		inflight: make(map[string]*bootstrapCall),
	}
}

func (b *bootstrapper) Bootstrap(ctx context.Context, tenantID, clientID, clientSecret, workspaceID string) error {
	key := tenantID + "\x00" + clientID

	b.mu.Lock()
	if at, ok := b.success[key]; ok && b.now().Sub(at) < b.ttl {
		b.mu.Unlock()
		return nil
	}
	if call, ok := b.inflight[key]; ok {
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-call.done:
			return call.err
		}
	}
	call := &bootstrapCall{done: make(chan struct{})}
	b.inflight[key] = call
	b.mu.Unlock()

	err := b.bootstrap(ctx, tenantID, clientID, clientSecret, workspaceID)
	b.mu.Lock()
	if err == nil {
		b.success[key] = b.now()
	}
	call.err = err
	delete(b.inflight, key)
	close(call.done)
	b.mu.Unlock()
	return err
}

func (b *bootstrapper) bootstrap(ctx context.Context, tenantID, clientID, clientSecret, workspaceID string) error {
	credential, err := b.newCredential(tenantID, clientID, clientSecret)
	if err != nil {
		return fmt.Errorf("spn_token_bootstrap: creating Fabric API credential: %w", err)
	}
	token, err := credential.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{fabricAPIScope}})
	if err != nil {
		return fmt.Errorf("spn_token_bootstrap: authenticating with Fabric API: %w", err)
	}

	baseURL, err := url.Parse(b.endpoint)
	if err != nil {
		return fmt.Errorf("spn_token_bootstrap: parsing Fabric API endpoint: %w", err)
	}
	baseURL.Path = path.Join(baseURL.Path, "v1", "workspaces", workspaceID, "items")
	query := baseURL.Query()
	query.Set("recursive", "false")
	baseURL.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL.String(), nil)
	if err != nil {
		return fmt.Errorf("spn_token_bootstrap: creating Fabric API request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token.Token)
	response, err := b.client.Do(req)
	if err != nil {
		return fmt.Errorf("spn_token_bootstrap: calling Fabric API: %w", err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode == http.StatusOK {
		// The items collection and any continuation token are intentionally ignored.
		return nil
	}

	apiError := bootstrapAPIError{}
	decoder := jsonrs.NewDecoder(io.LimitReader(response.Body, 64*1024))
	_ = decoder.Decode(&apiError)
	if apiError.Error != nil {
		if apiError.ErrorCode == "" {
			apiError.ErrorCode = apiError.Error.ErrorCode
		}
		if apiError.RequestID == "" {
			apiError.RequestID = apiError.Error.RequestID
		}
		apiError.IsRetriable = apiError.IsRetriable || apiError.Error.IsRetriable
	}
	return &bootstrapError{
		StatusCode: response.StatusCode,
		ErrorCode:  apiError.ErrorCode,
		RequestID:  apiError.RequestID,
		Retryable:  response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError || apiError.IsRetriable,
	}
}
