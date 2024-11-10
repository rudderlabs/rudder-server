package ratelimiter

import (
	"io"
	"net/http"
	"strings"
	"time"

	kitsync "github.com/rudderlabs/rudder-go-kit/sync"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
)

type RateLimiter interface {
	// Intercept should return true if the request is allowed, false otherwise
	Intercept(req *http.Request, key string) (bool, *http.Response)

	// TrackResponse should track the response for the given key
	TrackResponse(res *http.Response, key string)
}

type RefreshTokenRateLimiter struct {
	AllowedTokenRefreshInterval time.Duration
	AllowedInvalidGrantInterval time.Duration

	LastSuccessResponse *http.Response
	LastSuccessTime     time.Time

	LastInvalidGrantResponse *http.Response
	LastInvalidGrantTime     time.Time
	mu                       *kitsync.PartitionRWLocker
}

func NewRefreshTokenRateLimiter(allowedTokenRefreshInterval, allowedInvalidGrantInterval time.Duration) *RefreshTokenRateLimiter {
	return &RefreshTokenRateLimiter{
		AllowedTokenRefreshInterval: allowedTokenRefreshInterval,
		AllowedInvalidGrantInterval: allowedInvalidGrantInterval,
	}
}

func (r *RefreshTokenRateLimiter) Intercept(req *http.Request, key string) (bool, *http.Response) {
	r.mu.RLock(key)
	defer r.mu.RUnlock(key)
	if key == "" {
		return true, nil
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return true, nil
	}
	isBodyEmpty := string(body) == ""
	validRequest := strings.HasSuffix(req.URL.Path, "/token") && !isBodyEmpty

	if !validRequest {
		return true, nil
	}
	// Check if enough time has passed since the last successful refresh
	if !r.LastSuccessTime.IsZero() &&
		time.Since(r.LastSuccessTime) < r.AllowedTokenRefreshInterval {
		return false, r.LastSuccessResponse
	}

	// Check if enough time has passed since the last invalid grant
	if !r.LastInvalidGrantTime.IsZero() &&
		time.Since(r.LastInvalidGrantTime) < r.AllowedInvalidGrantInterval {
		return false, r.LastInvalidGrantResponse
	}
	return true, nil
}

func (r *RefreshTokenRateLimiter) TrackResponse(res *http.Response, key string) {
	r.mu.Lock(key)
	defer r.mu.Unlock(key)
	statusCode, resp := common.ProcessResponse(res)
	if statusCode == http.StatusOK {
		r.LastSuccessTime = time.Now()
		r.LastSuccessResponse = res
		return
	}
	if gjson.Get(resp, "body.code").String() == common.RefTokenInvalidGrant {
		r.LastInvalidGrantTime = time.Now()
		r.LastInvalidGrantResponse = res
	}
	return
}
