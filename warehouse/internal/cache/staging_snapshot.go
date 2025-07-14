// Package cache provides a memory-cached, pluggable-expiry schema snapshot lookup for staging files.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-go-kit/cachettl"
	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
)

// StagingFileSchemaSnapshotsDBRepo defines the interface for DB operations needed by the cache.
type StagingFileSchemaSnapshotsDBRepo interface {
	Insert(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (uuid.UUID, error)
	GetLatest(ctx context.Context, sourceID, destinationID, workspaceID string) (*model.StagingFileSchemaSnapshot, error)
}

// StagingFileSchemaSnapshots provides a memory-cached, pluggable-expiry schema snapshot lookup.
type StagingFileSchemaSnapshots struct {
	dbRepo               StagingFileSchemaSnapshotsDBRepo
	cache                *cachettl.Cache[string, *model.StagingFileSchemaSnapshot]
	cacheRefreshInterval config.ValueLoader[time.Duration]
	cacheJitter          func() time.Duration
	expiryStrategy       StagingFileSchemaSnapshotsExpiryStrategy
}

// NewStagingFileSchemaSnapshots creates a new cache with the given DB repo and expiration strategy.
func NewStagingFileSchemaSnapshots(conf *config.Config, dbRepo StagingFileSchemaSnapshotsDBRepo, expiryStrategy StagingFileSchemaSnapshotsExpiryStrategy) *StagingFileSchemaSnapshots {
	cache := cachettl.New[string, *model.StagingFileSchemaSnapshot](cachettl.WithNoRefreshTTL)
	cacheJitter := func() time.Duration {
		return time.Duration(rand.Int63n(5)) * time.Second
	}
	return &StagingFileSchemaSnapshots{
		dbRepo:               dbRepo,
		cache:                cache,
		cacheRefreshInterval: conf.GetReloadableDurationVar(3, time.Hour, "Warehouse.stagingSnapshotCacheRefreshInterval"),
		cacheJitter:          cacheJitter,
		expiryStrategy:       expiryStrategy,
	}
}

// Get returns the latest schema snapshot for the given IDs, using cache and DB as needed.
func (c *StagingFileSchemaSnapshots) Get(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (*model.StagingFileSchemaSnapshot, error) {
	key := cacheKey(sourceID, destinationID, workspaceID)

	// Check cache
	cachedSnapshot := c.cache.Get(key)
	if cachedSnapshot != nil {
		// Not expired: return the snapshot
		if !c.expiryStrategy.IsExpired(cachedSnapshot) {
			return cachedSnapshot, nil
		}

		// Expired: insert new and fetch
		return c.insertAndCache(ctx, key, sourceID, destinationID, workspaceID, schemaBytes)
	}

	// Cache miss: fetch from DB
	snap, err := c.dbRepo.GetLatest(ctx, sourceID, destinationID, workspaceID)
	if err == nil {
		if !c.expiryStrategy.IsExpired(snap) {
			c.cache.Put(key, snap, c.cacheRefreshTTL())
			return snap, nil
		}
		// Expired in DB: insert new and fetch
		return c.insertAndCache(ctx, key, sourceID, destinationID, workspaceID, schemaBytes)
	}

	// Only insert if the error is ErrNoLatestSchemaSnapshot (no entry)
	if errors.Is(err, repo.ErrNoLatestSchemaSnapshot) {
		return c.insertAndCache(ctx, key, sourceID, destinationID, workspaceID, schemaBytes)
	}

	return nil, err
}

// insertAndCache inserts a new snapshot into the DB, fetches it, and caches the result.
func (c *StagingFileSchemaSnapshots) insertAndCache(ctx context.Context, key, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (*model.StagingFileSchemaSnapshot, error) {
	_, err := c.dbRepo.Insert(ctx, sourceID, destinationID, workspaceID, schemaBytes)
	if err != nil {
		return nil, err
	}

	// Fetch the snapshot from the DB
	snap, err := c.dbRepo.GetLatest(ctx, sourceID, destinationID, workspaceID)
	if err != nil {
		return nil, err
	}

	// Insert the snapshot into the cache
	c.cache.Put(key, snap, c.cacheRefreshTTL())
	return snap, nil
}

func (c *StagingFileSchemaSnapshots) cacheRefreshTTL() time.Duration {
	return c.cacheRefreshInterval.Load() + c.cacheJitter()
}

// cacheKey generates a unique key for a snapshot based on sourceID, destinationID, and workspaceID.
func cacheKey(sourceID, destinationID, workspaceID string) string {
	return fmt.Sprintf("%s:%s:%s", sourceID, destinationID, workspaceID)
}
