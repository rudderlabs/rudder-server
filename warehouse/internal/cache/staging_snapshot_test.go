package cache

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
)

type mockStagingFileSchemaSnapshotsDBRepo struct {
	insertFunc     func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (uuid.UUID, error)
	getLatestFunc  func(ctx context.Context, sourceID, destinationID, workspaceID string) (*model.StagingFileSchemaSnapshot, error)
	insertCalls    int
	getLatestCalls int
}

func (m *mockStagingFileSchemaSnapshotsDBRepo) Insert(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (uuid.UUID, error) {
	m.insertCalls++
	return m.insertFunc(ctx, sourceID, destinationID, workspaceID, schemaBytes)
}

func (m *mockStagingFileSchemaSnapshotsDBRepo) GetLatest(ctx context.Context, sourceID, destinationID, workspaceID string) (*model.StagingFileSchemaSnapshot, error) {
	m.getLatestCalls++
	return m.getLatestFunc(ctx, sourceID, destinationID, workspaceID)
}

type fixedExpiryStrategy struct {
	expired bool
}

func (f *fixedExpiryStrategy) IsExpired(*model.StagingFileSchemaSnapshot) bool {
	return f.expired
}

func TestStagingFileSchemaSnapshots(t *testing.T) {
	schema := json.RawMessage(`{"foo":"bar"}`)
	snapshot := &model.StagingFileSchemaSnapshot{
		ID:            uuid.New(),
		Schema:        schema,
		SourceID:      "279L3gEKqwruBoKGsXZtSVX7vIy",
		DestinationID: "27CHciD6leAhurSyFAeN4dp14qZ",
		WorkspaceID:   "279L3V7FSpx43LaNJ0nIs9KRaNC",
		CreatedAt:     time.Now(),
	}

	t.Run("cache hit", func(t *testing.T) {
		db := &mockStagingFileSchemaSnapshotsDBRepo{
			getLatestFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string) (*model.StagingFileSchemaSnapshot, error) {
				return nil, errors.New("should not be called")
			},
			insertFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (uuid.UUID, error) {
				return uuid.Nil, errors.New("should not be called")
			},
		}
		cache := NewStagingFileSchemaSnapshots(config.New(), db, &fixedExpiryStrategy{expired: false})
		cache.cache.Put(cacheKey("src", "dst", "ws"), snapshot, cache.cacheRefreshTTL())

		got, err := cache.Get(context.Background(), "src", "dst", "ws", schema)
		require.NoError(t, err)
		require.Equal(t, snapshot, got, "expected cache hit to return cached snapshot")
		require.Zero(t, db.getLatestCalls, "expected no DB calls")
		require.Zero(t, db.insertCalls, "expected no DB insert calls")
	})

	t.Run("cache expired", func(t *testing.T) {
		fresh := &model.StagingFileSchemaSnapshot{
			ID:            uuid.New(),
			Schema:        schema,
			SourceID:      "279L3gEKqwruBoKGsXZtSVX7vIy",
			DestinationID: "27CHciD6leAhurSyFAeN4dp14qZ",
			WorkspaceID:   "279L3V7FSpx43LaNJ0nIs9KRaNC",
			CreatedAt:     time.Now(),
		}
		db := &mockStagingFileSchemaSnapshotsDBRepo{
			getLatestFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string) (*model.StagingFileSchemaSnapshot, error) {
				return fresh, nil
			},
			insertFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (uuid.UUID, error) {
				return fresh.ID, nil
			},
		}
		cache := NewStagingFileSchemaSnapshots(config.New(), db, &fixedExpiryStrategy{expired: true})
		cache.cache.Put(cacheKey("279L3gEKqwruBoKGsXZtSVX7vIy", "27CHciD6leAhurSyFAeN4dp14qZ", "279L3V7FSpx43LaNJ0nIs9KRaNC"), &model.StagingFileSchemaSnapshot{CreatedAt: time.Now().Add(-time.Hour)}, cache.cacheRefreshTTL())

		got, err := cache.Get(context.Background(), "279L3gEKqwruBoKGsXZtSVX7vIy", "27CHciD6leAhurSyFAeN4dp14qZ", "279L3V7FSpx43LaNJ0nIs9KRaNC", schema)
		require.NoError(t, err)
		require.Equal(t, fresh.ID, got.ID, "expected to fetch and cache new snapshot after expiry")
		require.Equal(t, 1, db.insertCalls, "expected 1 insert call")
		require.Equal(t, 1, db.getLatestCalls, "expected 1 DB getLatest call")
	})

	t.Run("cache miss, db hit", func(t *testing.T) {
		dbSnap := &model.StagingFileSchemaSnapshot{
			ID:            uuid.New(),
			Schema:        schema,
			SourceID:      "279L3gEKqwruBoKGsXZtSVX7vIy",
			DestinationID: "27CHciD6leAhurSyFAeN4dp14qZ",
			WorkspaceID:   "279L3V7FSpx43LaNJ0nIs9KRaNC",
			CreatedAt:     time.Now(),
		}
		db := &mockStagingFileSchemaSnapshotsDBRepo{
			getLatestFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string) (*model.StagingFileSchemaSnapshot, error) {
				return dbSnap, nil
			},
			insertFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (uuid.UUID, error) {
				return uuid.Nil, errors.New("should not be called")
			},
		}
		cache := NewStagingFileSchemaSnapshots(config.New(), db, &fixedExpiryStrategy{expired: false})
		got, err := cache.Get(context.Background(), "279L3gEKqwruBoKGsXZtSVX7vIy", "27CHciD6leAhurSyFAeN4dp14qZ", "279L3V7FSpx43LaNJ0nIs9KRaNC", schema)
		require.NoError(t, err)
		require.Equal(t, dbSnap.ID, got.ID, "expected to return DB snapshot on cache miss")
		require.Equal(t, 1, db.getLatestCalls, "expected 1 DB getLatest call")
		require.Equal(t, 0, db.insertCalls, "expected no DB insert calls")
	})

	t.Run("cache miss, db expired", func(t *testing.T) {
		fresh := &model.StagingFileSchemaSnapshot{
			ID:            uuid.New(),
			Schema:        schema,
			SourceID:      "279L3gEKqwruBoKGsXZtSVX7vIy",
			DestinationID: "27CHciD6leAhurSyFAeN4dp14qZ",
			WorkspaceID:   "279L3V7FSpx43LaNJ0nIs9KRaNC",
			CreatedAt:     time.Now(),
		}
		db := &mockStagingFileSchemaSnapshotsDBRepo{
			getLatestFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string) (*model.StagingFileSchemaSnapshot, error) {
				return fresh, nil
			},
			insertFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (uuid.UUID, error) {
				return fresh.ID, nil
			},
		}
		cache := NewStagingFileSchemaSnapshots(config.New(), db, &fixedExpiryStrategy{expired: true})
		got, err := cache.Get(context.Background(), "279L3gEKqwruBoKGsXZtSVX7vIy", "27CHciD6leAhurSyFAeN4dp14qZ", "279L3V7FSpx43LaNJ0nIs9KRaNC", schema)
		require.NoError(t, err)
		require.Equal(t, fresh.ID, got.ID, "expected to insert and return new snapshot when DB is expired")
		require.Equal(t, 1, db.insertCalls, "expected 1 insert call")
		require.Equal(t, 2, db.getLatestCalls, "expected 1 DB getLatest call")
	})

	t.Run("cache miss, db no entry", func(t *testing.T) {
		fresh := &model.StagingFileSchemaSnapshot{
			ID:            uuid.New(),
			Schema:        schema,
			SourceID:      "279L3gEKqwruBoKGsXZtSVX7vIy",
			DestinationID: "27CHciD6leAhurSyFAeN4dp14qZ",
			WorkspaceID:   "279L3V7FSpx43LaNJ0nIs9KRaNC",
			CreatedAt:     time.Now(),
		}
		calls := 0
		db := &mockStagingFileSchemaSnapshotsDBRepo{
			insertFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (uuid.UUID, error) {
				return fresh.ID, nil
			},
			getLatestFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string) (*model.StagingFileSchemaSnapshot, error) {
				calls++
				if calls == 1 {
					return nil, repo.ErrNoLatestSchemaSnapshot
				}
				return fresh, nil
			},
		}
		cache := NewStagingFileSchemaSnapshots(config.New(), db, &fixedExpiryStrategy{expired: false})
		got, err := cache.Get(context.Background(), "279L3gEKqwruBoKGsXZtSVX7vIy", "27CHciD6leAhurSyFAeN4dp14qZ", "279L3V7FSpx43LaNJ0nIs9KRaNC", schema)
		require.NoError(t, err)
		require.Equal(t, fresh.ID, got.ID, "expected to insert and return new snapshot when DB returns ErrNoLatestSchemaSnapshot")
		require.Equal(t, 1, db.insertCalls, "expected 1 insert call")
		require.Equal(t, 2, db.getLatestCalls, "expected 1 DB getLatest call")
	})

	t.Run("db error", func(t *testing.T) {
		db := &mockStagingFileSchemaSnapshotsDBRepo{
			getLatestFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string) (*model.StagingFileSchemaSnapshot, error) {
				return nil, errors.New("db error")
			},
			insertFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (uuid.UUID, error) {
				return uuid.Nil, nil
			},
		}
		cache := NewStagingFileSchemaSnapshots(config.New(), db, &fixedExpiryStrategy{expired: false})
		_, err := cache.Get(context.Background(), "279L3gEKqwruBoKGsXZtSVX7vIy", "27CHciD6leAhurSyFAeN4dp14qZ", "279L3V7FSpx43LaNJ0nIs9KRaNC", schema)
		require.Error(t, err)
		require.Equal(t, "db error", err.Error(), "expected db error")
		require.Equal(t, 1, db.getLatestCalls, "expected 1 DB getLatest call")
		require.Equal(t, 0, db.insertCalls, "expected no DB insert calls")
	})
}

func TestStagingFileSchemaSnapshots_ConcurrentAccess(t *testing.T) {
	r := &mockStagingFileSchemaSnapshotsDBRepo{
		getLatestFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string) (*model.StagingFileSchemaSnapshot, error) {
			return &model.StagingFileSchemaSnapshot{
				ID:            uuid.New(),
				Schema:        json.RawMessage(`{"foo":"bar"}`),
				SourceID:      "279L3gEKqwruBoKGsXZtSVX7vIy",
				DestinationID: "27CHciD6leAhurSyFAeN4dp14qZ",
				WorkspaceID:   "279L3V7FSpx43LaNJ0nIs9KRaNC",
				CreatedAt:     time.Now(),
			}, nil
		},
		insertFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (uuid.UUID, error) {
			return uuid.Nil, nil
		},
	}
	cache := NewStagingFileSchemaSnapshots(config.New(), r, NewStagingFileSchemaSnapshotsTimeBasedExpiryStrategy(time.Hour))

	var wg sync.WaitGroup
	numGoroutines := 1000
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(int) {
			defer wg.Done()
			_, err := cache.Get(context.Background(), "279L3gEKqwruBoKGsXZtSVX7vIy", "27CHciD6leAhurSyFAeN4dp14qZ", "279L3V7FSpx43LaNJ0nIs9KRaNC", json.RawMessage(`{"foo":"bar"}`))
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()
}
