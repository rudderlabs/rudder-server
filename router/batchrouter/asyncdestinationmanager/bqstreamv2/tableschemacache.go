package bqstreamv2

import (
	"maps"
	"time"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// NewTableSchemaCache creates a TTL-based table schema cache; schemas are
// cloned on Set/Get/Peek so callers can mutate their copies safely.
func NewTableSchemaCache(ttl time.Duration) TableSchemaCache {
	return &tableSchemaCacheImpl{ttl: ttl, items: make(map[string]tableSchemaCacheItem)}
}

// Get gets the table schema from the cache.
func (c *tableSchemaCacheImpl) Get(tableName string, now time.Time) (whutils.ModelTableSchema, bool) {
	c.mu.RLock()
	item, ok := c.items[tableName]
	c.mu.RUnlock()
	if !ok || now.After(item.expiresAt) {
		return nil, false
	}
	return cloneSchema(item.schema), true
}

// Has reports whether a non-expired schema exists for the table, without
// cloning it.
func (c *tableSchemaCacheImpl) Has(tableName string, now time.Time) bool {
	c.mu.RLock()
	item, ok := c.items[tableName]
	c.mu.RUnlock()
	return ok && !now.After(item.expiresAt)
}

// Peek returns the cached table schema regardless of expiry.
func (c *tableSchemaCacheImpl) Peek(tableName string) (whutils.ModelTableSchema, bool) {
	c.mu.RLock()
	item, ok := c.items[tableName]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return cloneSchema(item.schema), true
}

// Set sets the table schema in the cache.
func (c *tableSchemaCacheImpl) Set(tableName string, schema whutils.ModelTableSchema, now time.Time) {
	c.mu.Lock()
	c.items[tableName] = tableSchemaCacheItem{schema: cloneSchema(schema), expiresAt: now.Add(c.ttl)}
	c.mu.Unlock()
}

// Invalidate invalidates the table schema in the cache.
func (c *tableSchemaCacheImpl) Invalidate(tableName string) {
	c.mu.Lock()
	delete(c.items, tableName)
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
