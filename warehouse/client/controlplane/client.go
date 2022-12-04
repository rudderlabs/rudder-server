package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type PublicPrivateKeyPair struct {
	PublicKey  string
	PrivateKey string
}

type BasicAuth struct {
	Username string
	Password string
}

type CPInternalAPI interface {
	GetDestinationSSHKeys(ctx context.Context, id string) (*PublicPrivateKeyPair, error)
}

type cachedControlPlaneAPIImpl struct {
	cpAPI CPInternalAPI
	cache map[string]*PublicPrivateKeyPair
}

func NewControlPlaneAPI(baseURI string, auth BasicAuth) CPInternalAPI {
	return &controlPlaneAPIImpl{
		baseURI: baseURI,
		auth:    auth,
	}
}

type controlPlaneAPIImpl struct {
	baseURI string
	auth    BasicAuth
}

func (api *controlPlaneAPIImpl) GetDestinationSSHKeys(ctx context.Context, id string) (*PublicPrivateKeyPair, error) {
	url := fmt.Sprintf("%s/dataplane/admin/destinations/%s/sshKeys", api.baseURI, id)

	req, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("creating new request with ctx: %w", err)
	}

	req.SetBasicAuth(
		api.auth.Username,
		api.auth.Password)

	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching response from http client: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, nil
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("invalid status code: %d", resp.StatusCode)
	}

	decoder := json.NewDecoder(resp.Body)

	keypair := PublicPrivateKeyPair{}
	if err := decoder.Decode(&keypair); err != nil {
		return nil, fmt.Errorf("decoding upstream response body: %w", err)
	}

	return &keypair, nil
}

func NewCachedControlPlaneAPI(baseURI string, auth BasicAuth) CPInternalAPI {
	apiClient := NewControlPlaneAPI(baseURI, auth)
	return &cachedControlPlaneAPIImpl{
		cpAPI: apiClient,
		cache: make(map[string]*PublicPrivateKeyPair),
	}
}

func (api *cachedControlPlaneAPIImpl) GetDestinationSSHKeys(ctx context.Context, id string) (*PublicPrivateKeyPair, error) {
	if val, ok := api.cache[id]; ok {
		return val, nil
	}

	keyPair, err := api.cpAPI.GetDestinationSSHKeys(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("fetching and caching destination ssh keys: %w", err)
	}

	api.cache[id] = keyPair
	return keyPair, nil
}
