package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/rudderlabs/rudder-server/utils/httputil"
)

var ErrKeyNotFound = errors.New("request key not found")

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
	GetSSHKeys(ctx context.Context, id string) (*PublicPrivateKeyPair, error)
}

type internalClientWithCache struct {
	client InternalControlPlane
	cache  sync.Map
}

func NewInternalClient(baseURI string, auth BasicAuth) InternalControlPlane {
	return &internalClient{
		baseURI:    baseURI,
		auth:       auth,
		httpClient: &http.Client{},
	}
}

type internalClient struct {
	baseURI    string
	auth       BasicAuth
	httpClient *http.Client
}

func (api *internalClient) GetSSHKeys(ctx context.Context, id string) (*PublicPrivateKeyPair, error) {
	url := fmt.Sprintf("%s/dataplane/admin/sshKeys/%s", api.baseURI, id)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("creating new request with ctx: %w", err)
	}

	req.SetBasicAuth(api.auth.Username, api.auth.Password)
	req.Header.Set("Content-Type", "application/json")

	resp, err := api.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching response from http client: %w", err)
	}

	defer func() { httputil.CloseResponse(resp) }()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("%w: key requested: %s", ErrKeyNotFound, id)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("invalid status code: %d", resp.StatusCode)
	}

	decoder := json.NewDecoder(resp.Body)

	var keypair PublicPrivateKeyPair
	if err := decoder.Decode(&keypair); err != nil {
		return nil, fmt.Errorf("decoding upstream response body: %w", err)
	}

	return &keypair, nil
}

func (api *internalClient) GetDestinationSSHKeys(ctx context.Context, id string) (*PublicPrivateKeyPair, error) {
	url := fmt.Sprintf("%s/dataplane/admin/destinations/%s/sshKeys", api.baseURI, id)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("creating new request with ctx: %w", err)
	}

	req.SetBasicAuth(api.auth.Username, api.auth.Password)
	req.Header.Set("Content-Type", "application/json")

	resp, err := api.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching response from http client: %w", err)
	}

	defer func() { httputil.CloseResponse(resp) }()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("%w: key requested: %s", ErrKeyNotFound, id)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("invalid status code: %d", resp.StatusCode)
	}

	decoder := json.NewDecoder(resp.Body)

	var keypair PublicPrivateKeyPair
	if err := decoder.Decode(&keypair); err != nil {
		return nil, fmt.Errorf("decoding upstream response body: %w", err)
	}

	return &keypair, nil
}

func NewInternalClientWithCache(baseURI string, auth BasicAuth) InternalControlPlane {
	return &internalClientWithCache{
		client: NewInternalClient(baseURI, auth),
		cache:  sync.Map{},
	}
}

func (cc *internalClientWithCache) GetSSHKeys(ctx context.Context, id string) (*PublicPrivateKeyPair, error) {
	if val, ok := cc.cache.Load(id); ok {
		return val.(*PublicPrivateKeyPair), nil
	}

	keypair, err := cc.client.GetSSHKeys(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("fetching and caching destination ssh keys: %w", err)
	}

	cc.cache.Store(id, keypair)
	return keypair, nil
}

func (cc *internalClientWithCache) GetDestinationSSHKeys(ctx context.Context, id string) (*PublicPrivateKeyPair, error) {
	if val, ok := cc.cache.Load(id); ok {
		return val.(*PublicPrivateKeyPair), nil
	}

	keypair, err := cc.client.GetDestinationSSHKeys(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("fetching and caching destination ssh keys: %w", err)
	}

	cc.cache.Store(id, keypair)
	return keypair, nil
}
