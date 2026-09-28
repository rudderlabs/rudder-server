package auth

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
)

const (
	fabricScope  = "https://api.fabric.microsoft.com/.default"
	storageScope = "https://storage.azure.com/.default"
	bootstrapURL = "https://api.fabric.microsoft.com/v1/workspaces?$top=1"
)

type bootstrapState struct {
	mu       sync.Mutex
	complete bool
}

var bootstrapStates sync.Map

// Credential owns the Entra service-principal credential shared by the Fabric
// SQL bootstrap and OneLake paths. It never includes tokens or secrets in errors.
type Credential struct {
	tokenCredential azcore.TokenCredential
	httpClient      *http.Client
	state           *bootstrapState
}

func New(tenantID, clientID, clientSecret string, httpClient *http.Client) (*Credential, error) {
	if tenantID == "" || clientID == "" || clientSecret == "" {
		return nil, errors.New("tenantId, clientId and clientSecret are required")
	}
	credential, err := azidentity.NewClientSecretCredential(tenantID, clientID, clientSecret, nil)
	if err != nil {
		return nil, fmt.Errorf("creating service principal credential: %w", err)
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	key := sha256.Sum256([]byte(tenantID + "\x00" + clientID + "\x00" + clientSecret))
	state, _ := bootstrapStates.LoadOrStore(key, &bootstrapState{})
	return &Credential{tokenCredential: credential, httpClient: httpClient, state: state.(*bootstrapState)}, nil
}

func NewWithTokenCredential(tokenCredential azcore.TokenCredential, httpClient *http.Client) *Credential {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Credential{tokenCredential: tokenCredential, httpClient: httpClient, state: &bootstrapState{}}
}

// Bootstrap makes the documented Fabric REST call required to initialize a
// service principal's Fabric security token. Successful calls are cached.
func (c *Credential) Bootstrap(ctx context.Context) error {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if c.state.complete {
		return nil
	}
	token, err := c.tokenCredential.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{fabricScope}})
	if err != nil {
		return fmt.Errorf("acquiring Fabric bootstrap token: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, bootstrapURL, nil)
	if err != nil {
		return fmt.Errorf("creating Fabric bootstrap request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token.Token)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("calling Fabric bootstrap API: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("fabric bootstrap API returned HTTP %d", resp.StatusCode)
	}
	c.state.complete = true
	return nil
}

func (c *Credential) StorageToken(ctx context.Context) (string, error) {
	token, err := c.tokenCredential.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{storageScope}})
	if err != nil {
		return "", fmt.Errorf("acquiring OneLake token: %w", err)
	}
	return token.Token, nil
}
