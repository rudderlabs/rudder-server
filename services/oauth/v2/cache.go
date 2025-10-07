package v2

import (
	"sync"
)

// NewOauthTokenCache returns a new cache for storing OAuth tokens.
func NewOauthTokenCache() OauthTokenCache {
	return &syncMapCache[OAuthToken]{}
}

// OauthTokenCache is an interface for a cache that stores OAuth tokens.
type OauthTokenCache interface {
	// Load retrieves the OAuth token associated with the given key.
	Load(key string) (OAuthToken, bool)
	// Store saves the OAuth token with the associated key.
	Store(key string, value OAuthToken)
	// Delete removes the OAuth token associated with the given key.
	Delete(key string)
}

type syncMapCache[T any] struct {
	m sync.Map
}

func (c *syncMapCache[T]) Load(key string) (T, bool) {
	value, ok := c.m.Load(key)
	if !ok {
		var zero T
		return zero, false
	}
	return value.(T), true
}

func (c *syncMapCache[T]) Store(key string, value T) {
	c.m.Store(key, value)
}

func (c *syncMapCache[T]) Delete(key string) {
	c.m.Delete(key)
}
