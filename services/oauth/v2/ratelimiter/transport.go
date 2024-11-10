package ratelimiter

import (
	"net/http"
	"strings"

	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
)

//go:generate mockgen -destination=../../../../mocks/services/oauth/v2/http/mock_transport.go -package=mock_http_client net/http RoundTripper

type RateLimiterTransport struct {
	RateLimiter RateLimiter
	Transport   http.RoundTripper
}

func NewRateLimiterTransport(rateLimiter RateLimiter) *RateLimiterTransport {
	return &RateLimiterTransport{
		RateLimiter: rateLimiter,
		Transport:   http.DefaultTransport,
	}
}

func (t *RateLimiterTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	key, _ := common.ParseAccountIDFromTokenURL(req.URL.String())
	if !strings.HasSuffix(req.URL.Path, "/token") {
		return t.Transport.RoundTrip(req)
	}
	if allowed, res := t.RateLimiter.Intercept(req, key); !allowed {
		return res, nil
	}
	resp, err := t.Transport.RoundTrip(req)
	t.RateLimiter.TrackResponse(resp, key)
	return resp, err
}
