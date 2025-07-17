// Package snapshots provides a memory-cached, pluggable-expiry schema snapshot lookup for staging files.
package snapshots

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

	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
)

// StagingFileSchemaDBRepo defines the interface for DB operations needed by the cache.
type StagingFileSchemaDBRepo interface {
	Insert(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (uuid.UUID, error)
	GetLatest(ctx context.Context, sourceID, destinationID string) (*model.StagingFileSchemaSnapshot, error)
}

// StagingFileSchema provides a memory-cached, pluggable-expiry schema snapshot lookup.
type StagingFileSchema struct {
	dbRepo               StagingFileSchemaDBRepo
	cache                *cachettl.Cache[string, *model.StagingFileSchemaSnapshot] // cache for the latest schema snapshot to avoid DB lookups
	cacheRefreshInterval config.ValueLoader[time.Duration]                         // interval at which to refresh the cache
	cacheRefreshJitter   func() time.Duration                                      // jitter to add to the cache refresh interval
	now                  func() time.Time                                          // function to get the current time
	expiryStrategy       StagingFileSchemaExpiryStrategy                           // strategy to determine if the cache is expired
}

// NewStagingFileSchema creates a new cache with the given DB repo and expiration strategy.
func NewStagingFileSchema(conf *config.Config, dbRepo StagingFileSchemaDBRepo, expiryStrategy StagingFileSchemaExpiryStrategy) *StagingFileSchema {
	cache := cachettl.New[string, *model.StagingFileSchemaSnapshot](cachettl.WithNoRefreshTTL)
	cacheRefreshInterval := conf.GetReloadableDurationVar(60, time.Minute, "Warehouse.stagingSnapshotCacheRefreshInterval")
	cacheRefreshJitter := func() time.Duration {
		return time.Duration(rand.Int63n(10)) * time.Minute
	}
	return &StagingFileSchema{
		dbRepo:               dbRepo,
		cache:                cache,
		now:                  timeutil.Now,
		cacheRefreshInterval: cacheRefreshInterval,
		cacheRefreshJitter:   cacheRefreshJitter,
		expiryStrategy:       expiryStrategy,
	}
}

// GetOrCreate returns the latest schema snapshot for the given IDs, using cache and DB as needed. If not found or expired, inserts a new snapshot.
func (c *StagingFileSchema) GetOrCreate(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (*model.StagingFileSchemaSnapshot, error) {
	key := cacheKey(sourceID, destinationID)

	// Check cache
	cachedSnapshot := c.cache.Get(key)
	if cachedSnapshot != nil {
		// Not expired: return the snapshot
		if !c.expiryStrategy.IsExpired(cachedSnapshot) {
			return cachedSnapshot, nil
		}

		// Expired: insert new and fetch
		return c.insertAndCache(ctx, sourceID, destinationID, workspaceID, schemaBytes)
	}

	// Cache miss: fetch from DB
	snap, err := c.dbRepo.GetLatest(ctx, sourceID, destinationID)
	if err == nil {
		if !c.expiryStrategy.IsExpired(snap) {
			c.cache.Put(key, snap, c.cacheRefreshTTL())
			return snap, nil
		}
		// Expired in DB: insert new and fetch
		return c.insertAndCache(ctx, sourceID, destinationID, workspaceID, schemaBytes)
	}

	// Only insert if the error is ErrNoSchemaSnapshot (no entry)
	if errors.Is(err, repo.ErrNoSchemaSnapshot) {
		return c.insertAndCache(ctx, sourceID, destinationID, workspaceID, schemaBytes)
	}

	return nil, err
}

// insertAndCache inserts a new snapshot into the DB, and caches the result.
func (c *StagingFileSchema) insertAndCache(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (*model.StagingFileSchemaSnapshot, error) {
	snapshotID, err := c.dbRepo.Insert(ctx, sourceID, destinationID, workspaceID, schemaBytes)
	if err != nil {
		return nil, err
	}

	// Create the snapshot and insert it into the cache
	snap := &model.StagingFileSchemaSnapshot{
		ID:            snapshotID,
		SourceID:      sourceID,
		DestinationID: destinationID,
		WorkspaceID:   workspaceID,
		Schema:        schemaBytes,
		CreatedAt:     c.now(),
	}
	c.cache.Put(cacheKey(sourceID, destinationID), snap, c.cacheRefreshTTL())
	return snap, nil
}

func (c *StagingFileSchema) cacheRefreshTTL() time.Duration {
	return c.cacheRefreshInterval.Load() + c.cacheRefreshJitter()
}

func cacheKey(sourceID, destinationID string) string {
	return fmt.Sprintf("%s:%s", sourceID, destinationID)
}
