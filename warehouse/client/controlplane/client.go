package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/rudderlabs/rudder-server/utils/httputil"
)

type PublicPrivateKeyPair struct {
	PublicKey  string
	PrivateKey string
}

type BasicAuth struct {
	Username string
	Password string
}

type InternalControlPlane interface {
	GetDestinationSSHKeys(ctx context.Context, id string) (*PublicPrivateKeyPair, error)
}

type internalClientWithCache struct {
	client InternalControlPlane
	cache  map[string]*PublicPrivateKeyPair
}

func NewInternalClient(baseURI string, auth BasicAuth) InternalControlPlane {
	return &internalClient{
		baseURI: baseURI,
		auth:    auth,
	}
}

type internalClient struct {
	baseURI string
	auth    BasicAuth
}

func (api *internalClient) GetDestinationSSHKeys(ctx context.Context, id string) (*PublicPrivateKeyPair, error) {
	url := fmt.Sprintf("%s/dataplane/admin/destinations/%s/sshKeys", api.baseURI, id)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("creating new request with ctx: %w", err)
	}

	req.SetBasicAuth(api.auth.Username, api.auth.Password)

	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching response from http client: %w", err)
	}

	defer func() { httputil.CloseResponse(resp) }()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("invalid status code: %d", resp.StatusCode)
	}

	decoder := json.NewDecoder(resp.Body)

	keypair := PublicPrivateKeyPair{}
	if err := decoder.Decode(&keypair); err != nil {
		return nil, fmt.Errorf("decoding upstream response body: %w", err)
	}

	return &keypair, nil
}

func NewInternalClientWithCache(baseURI string, auth BasicAuth) InternalControlPlane {
	return &internalClientWithCache{
		client: NewInternalClient(baseURI, auth),
		cache:  make(map[string]*PublicPrivateKeyPair),
	}
}

func (cc *internalClientWithCache) GetDestinationSSHKeys(ctx context.Context, id string) (*PublicPrivateKeyPair, error) {
	if val, ok := cc.cache[id]; ok {
		return val, nil
	}

	keyPair, err := cc.client.GetDestinationSSHKeys(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("fetching and caching destination ssh keys: %w", err)
	}

	cc.cache[id] = keyPair
	return keyPair, nil
}
