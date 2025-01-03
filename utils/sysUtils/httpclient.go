//go:generate mockgen -destination=../../mocks/utils/sysUtils/mock_httpclient.go -package mock_sysUtils github.com/rudderlabs/rudder-server/utils/sysUtils HTTPClientI
package sysUtils

import (
	"net/http"
	"sync"
	"time"
)

// HTTPClient interface
type HTTPClientI interface {
	Do(req *http.Request) (*http.Response, error)
}

type RecycledHTTPClient struct {
	client          *http.Client
	lastRefreshTime time.Time
	ttl             time.Duration
	clientFunc      func() *http.Client
	lock            sync.Mutex
}

func NewRecycledHTTPClient(_clientFunc func() *http.Client, _ttl time.Duration) *RecycledHTTPClient {
	return &RecycledHTTPClient{
		client:          _clientFunc(),
		clientFunc:      _clientFunc,
		ttl:             _ttl,
		lastRefreshTime: time.Now(),
	}
}

func (r *RecycledHTTPClient) GetClient() *http.Client {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.ttl > 0 && time.Since(r.lastRefreshTime) > r.ttl {
		r.client.CloseIdleConnections()
		r.client = r.clientFunc()
		r.lastRefreshTime = time.Now()
	}
	return r.client
}

func (r *RecycledHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return r.GetClient().Do(req)
}
