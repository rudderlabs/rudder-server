package snapshots

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
)

type mockStagingFileSchemaDBRepo struct {
	insertCalls    atomic.Int64
	getLatestCalls atomic.Int64
	insertFunc     func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (uuid.UUID, error)
	getLatestFunc  func(ctx context.Context, sourceID, destinationID string) (*model.StagingFileSchemaSnapshot, error)
}

func (m *mockStagingFileSchemaDBRepo) Insert(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (uuid.UUID, error) {
	m.insertCalls.Add(1)
	return m.insertFunc(ctx, sourceID, destinationID, workspaceID, schemaBytes)
}

func (m *mockStagingFileSchemaDBRepo) GetLatest(ctx context.Context, sourceID, destinationID string) (*model.StagingFileSchemaSnapshot, error) {
	m.getLatestCalls.Add(1)
	return m.getLatestFunc(ctx, sourceID, destinationID)
}

type fixedExpiryStrategy struct {
	expired bool
}

func (f *fixedExpiryStrategy) IsExpired(*model.StagingFileSchemaSnapshot) bool {
	return f.expired
}

func TestStagingFileSchema(t *testing.T) {
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
		db := &mockStagingFileSchemaDBRepo{
			getLatestFunc: func(ctx context.Context, sourceID, destinationID string) (*model.StagingFileSchemaSnapshot, error) {
				return nil, errors.New("should not be called")
			},
			insertFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (uuid.UUID, error) {
				return uuid.Nil, errors.New("should not be called")
			},
		}
		schemaCache := NewStagingFileSchema(config.New(), db, &fixedExpiryStrategy{expired: false})
		schemaCache.cache.Put(cacheKey("279L3gEKqwruBoKGsXZtSVX7vIy", "27CHciD6leAhurSyFAeN4dp14qZ"), snapshot, schemaCache.cacheRefreshTTL())

		got, err := schemaCache.GetOrCreate(context.Background(), "279L3gEKqwruBoKGsXZtSVX7vIy", "27CHciD6leAhurSyFAeN4dp14qZ", "279L3V7FSpx43LaNJ0nIs9KRaNC", schema)
		require.NoError(t, err)
		require.Equal(t, snapshot, got, "expected cache hit to return cached snapshot")
		require.Zero(t, db.getLatestCalls.Load(), "expected no DB calls")
		require.Zero(t, db.insertCalls.Load(), "expected no DB insert calls")
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
		db := &mockStagingFileSchemaDBRepo{
			getLatestFunc: func(ctx context.Context, sourceID, destinationID string) (*model.StagingFileSchemaSnapshot, error) {
				return fresh, nil
			},
			insertFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (uuid.UUID, error) {
				return fresh.ID, nil
			},
		}
		schemaCache := NewStagingFileSchema(config.New(), db, &fixedExpiryStrategy{expired: true})
		schemaCache.cache.Put(cacheKey("279L3gEKqwruBoKGsXZtSVX7vIy", "27CHciD6leAhurSyFAeN4dp14qZ"), &model.StagingFileSchemaSnapshot{CreatedAt: time.Now().Add(-time.Hour)}, schemaCache.cacheRefreshTTL())

		got, err := schemaCache.GetOrCreate(context.Background(), "279L3gEKqwruBoKGsXZtSVX7vIy", "27CHciD6leAhurSyFAeN4dp14qZ", "279L3V7FSpx43LaNJ0nIs9KRaNC", schema)
		require.NoError(t, err)
		require.Equal(t, fresh.ID, got.ID, "expected to fetch and cache new snapshot after expiry")
		require.Equal(t, int64(1), db.insertCalls.Load(), "expected 1 insert call")
		require.Equal(t, int64(0), db.getLatestCalls.Load(), "expected 0 DB getLatest call")
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
		db := &mockStagingFileSchemaDBRepo{
			getLatestFunc: func(ctx context.Context, sourceID, destinationID string) (*model.StagingFileSchemaSnapshot, error) {
				return dbSnap, nil
			},
			insertFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (uuid.UUID, error) {
				return uuid.Nil, errors.New("should not be called")
			},
		}
		schemaCache := NewStagingFileSchema(config.New(), db, &fixedExpiryStrategy{expired: false})
		got, err := schemaCache.GetOrCreate(context.Background(), "279L3gEKqwruBoKGsXZtSVX7vIy", "27CHciD6leAhurSyFAeN4dp14qZ", "279L3V7FSpx43LaNJ0nIs9KRaNC", schema)
		require.NoError(t, err)
		require.Equal(t, dbSnap.ID, got.ID, "expected to return DB snapshot on cache miss")
		require.Equal(t, int64(1), db.getLatestCalls.Load(), "expected 1 DB getLatest call")
		require.Equal(t, int64(0), db.insertCalls.Load(), "expected no DB insert calls")
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
		db := &mockStagingFileSchemaDBRepo{
			getLatestFunc: func(ctx context.Context, sourceID, destinationID string) (*model.StagingFileSchemaSnapshot, error) {
				return fresh, nil
			},
			insertFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (uuid.UUID, error) {
				return fresh.ID, nil
			},
		}
		schemaCache := NewStagingFileSchema(config.New(), db, &fixedExpiryStrategy{expired: true})
		got, err := schemaCache.GetOrCreate(context.Background(), "279L3gEKqwruBoKGsXZtSVX7vIy", "27CHciD6leAhurSyFAeN4dp14qZ", "279L3V7FSpx43LaNJ0nIs9KRaNC", schema)
		require.NoError(t, err)
		require.Equal(t, fresh.ID, got.ID, "expected to insert and return new snapshot when DB is expired")
		require.Equal(t, int64(1), db.insertCalls.Load(), "expected 1 insert call")
		require.Equal(t, int64(1), db.getLatestCalls.Load(), "expected 1 DB getLatest call")
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
		db := &mockStagingFileSchemaDBRepo{
			insertFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (uuid.UUID, error) {
				return fresh.ID, nil
			},
			getLatestFunc: func(ctx context.Context, sourceID, destinationID string) (*model.StagingFileSchemaSnapshot, error) {
				calls++
				if calls == 1 {
					return nil, repo.ErrNoSchemaSnapshot
				}
				return fresh, nil
			},
		}
		schemaCache := NewStagingFileSchema(config.New(), db, &fixedExpiryStrategy{expired: false})
		got, err := schemaCache.GetOrCreate(context.Background(), "279L3gEKqwruBoKGsXZtSVX7vIy", "27CHciD6leAhurSyFAeN4dp14qZ", "279L3V7FSpx43LaNJ0nIs9KRaNC", schema)
		require.NoError(t, err)
		require.Equal(t, fresh.ID, got.ID, "expected to insert and return new snapshot when DB returns ErrNoSchemaSnapshot")
		require.Equal(t, int64(1), db.insertCalls.Load(), "expected 1 insert call")
		require.Equal(t, int64(1), db.getLatestCalls.Load(), "expected 1 DB getLatest call")
	})

	t.Run("db error: getLatest", func(t *testing.T) {
		db := &mockStagingFileSchemaDBRepo{
			getLatestFunc: func(ctx context.Context, sourceID, destinationID string) (*model.StagingFileSchemaSnapshot, error) {
				return nil, errors.New("db error")
			},
			insertFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (uuid.UUID, error) {
				return uuid.Nil, nil
			},
		}
		schemaCache := NewStagingFileSchema(config.New(), db, &fixedExpiryStrategy{expired: true})
		_, err := schemaCache.GetOrCreate(context.Background(), "279L3gEKqwruBoKGsXZtSVX7vIy", "27CHciD6leAhurSyFAeN4dp14qZ", "279L3V7FSpx43LaNJ0nIs9KRaNC", schema)
		require.Error(t, err)
		require.Equal(t, int64(1), db.getLatestCalls.Load(), "expected 1 DB getLatest call")
		require.Equal(t, int64(0), db.insertCalls.Load(), "expected no DB insert calls")
	})

	t.Run("db error: insert", func(t *testing.T) {
		latest := &model.StagingFileSchemaSnapshot{
			ID:            uuid.New(),
			Schema:        schema,
			SourceID:      "279L3gEKqwruBoKGsXZtSVX7vIy",
			DestinationID: "27CHciD6leAhurSyFAeN4dp14qZ",
			WorkspaceID:   "279L3V7FSpx43LaNJ0nIs9KRaNC",
			CreatedAt:     time.Now(),
		}
		db := &mockStagingFileSchemaDBRepo{
			getLatestFunc: func(ctx context.Context, sourceID, destinationID string) (*model.StagingFileSchemaSnapshot, error) {
				return latest, nil
			},
			insertFunc: func(ctx context.Context, sourceID, destinationID, workspaceID string, schemaBytes json.RawMessage) (uuid.UUID, error) {
				return uuid.Nil, errors.New("db error")
			},
		}
		schemaCache := NewStagingFileSchema(config.New(), db, &fixedExpiryStrategy{expired: true})
		_, err := schemaCache.GetOrCreate(context.Background(), "279L3gEKqwruBoKGsXZtSVX7vIy", "27CHciD6leAhurSyFAeN4dp14qZ", "279L3V7FSpx43LaNJ0nIs9KRaNC", schema)
		require.Error(t, err)
		require.Equal(t, int64(1), db.getLatestCalls.Load(), "expected 1 DB getLatest call")
		require.Equal(t, int64(1), db.insertCalls.Load(), "expected 1 DB insert call")
	})
}

func TestStagingFileSchema_ConcurrentAccess(t *testing.T) {
	db := &mockStagingFileSchemaDBRepo{
		getLatestFunc: func(ctx context.Context, sourceID, destinationID string) (*model.StagingFileSchemaSnapshot, error) {
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
	schemaCache := NewStagingFileSchema(config.New(), db, &fixedExpiryStrategy{expired: false})

	var wg sync.WaitGroup
	for range 1000 {
		wg.Go(func() {
			_, err := schemaCache.GetOrCreate(context.Background(), "279L3gEKqwruBoKGsXZtSVX7vIy", "27CHciD6leAhurSyFAeN4dp14qZ", "279L3V7FSpx43LaNJ0nIs9KRaNC", json.RawMessage(`{"foo":"bar"}`))
			require.NoError(t, err)
		})
	}
	wg.Wait()
}
