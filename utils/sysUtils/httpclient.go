//go:generate mockgen -destination=../../mocks/utils/sysUtils/mock_httpclient.go -package mock_sysUtils github.com/rudderlabs/rudder-server/utils/sysUtils HTTPClientI
package sysUtils

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// HTTPClient interface
type HTTPClientI interface {
	Do(req *http.Request) (*http.Response, error)
}

type RecycledHTTPClient struct {
	client *http.Client
	lock   sync.Mutex
}

func NewRecycledHTTPClient(ctx context.Context, clientFunc func() *http.Client, ttl time.Duration) *RecycledHTTPClient {
	recycledHTTPClient := &RecycledHTTPClient{
		client: clientFunc(),
	}

	go func() {
		ticker := time.NewTicker(ttl)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				recycledHTTPClient.lock.Lock()
				recycledHTTPClient.client = clientFunc()
				recycledHTTPClient.lock.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}()

	return recycledHTTPClient
}

func (r *RecycledHTTPClient) GetClient() *http.Client {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.client
}
