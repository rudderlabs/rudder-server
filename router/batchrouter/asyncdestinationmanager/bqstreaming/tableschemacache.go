package bqstreaming

import (
	"maps"
	"time"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func NewTableSchemaCache(ttl time.Duration) TableSchemaCache {
	return &tableSchemaCacheImpl{ttl: ttl, items: make(map[string]tableSchemaCacheItem)}
}

// Get gets the table schema from the cache.
func (c *tableSchemaCacheImpl) Get(key string, now time.Time) (whutils.ModelTableSchema, bool) {
	c.mu.RLock()
	item, ok := c.items[key]
	c.mu.RUnlock()
	if !ok || now.After(item.expiresAt) {
		return nil, false
	}
	return cloneSchema(item.schema), true
}

// Peek returns the cached table schema regardless of expiry.
func (c *tableSchemaCacheImpl) Peek(key string) (whutils.ModelTableSchema, bool) {
	c.mu.RLock()
	item, ok := c.items[key]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return cloneSchema(item.schema), true
}

// Set sets the table schema in the cache.
func (c *tableSchemaCacheImpl) Set(key string, schema whutils.ModelTableSchema, now time.Time) {
	c.mu.Lock()
	c.items[key] = tableSchemaCacheItem{schema: cloneSchema(schema), expiresAt: now.Add(c.ttl)}
	c.mu.Unlock()
}

// Invalidate invalidates the table schema in the cache.
func (c *tableSchemaCacheImpl) Invalidate(key string) {
	c.mu.Lock()
	delete(c.items, key)
	c.mu.Unlock()
}

// Len returns the number of items in the cache.
func (c *tableSchemaCacheImpl) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// cloneSchema clones the schema to avoid mutating the original schema.
func cloneSchema(in whutils.ModelTableSchema) whutils.ModelTableSchema {
	out := make(whutils.ModelTableSchema, len(in))
	maps.Copy(out, in)
	return out
}
