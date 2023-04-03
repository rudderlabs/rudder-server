package badgerdb

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/samber/lo"
)

// the key used in badgerdb to store the current token
const tokenKey = "__token__"

// Opt is a function that configures a badgerdb repository
type Opt func(*Repository)

// WithSeederSource sets the source of the seed data
func WithSeederSource(seederSource func() (io.ReadCloser, error)) Opt {
	return func(r *Repository) {
		r.seederSource = seederSource
	}
}

// WithMaxSeedWait sets the maximum time to wait for the seed to complete.
// If the seed takes longer than this, the repository will be started in restoring state and all
// repository methods will return [ErrRestoring] until the seed completes. The default wait time is 10 seconds.
func WithMaxSeedWait(maxSeedWait time.Duration) Opt {
	return func(r *Repository) {
		r.maxSeedWait = maxSeedWait
	}
}

// Repository is a repository backed by badgerdb
type Repository struct {
	// logger to use
	log logger.Logger
	// path to the badger db directory
	path string
	// max number of goroutines to use (badger config)
	maxGoroutines int

	maxSeedWait  time.Duration
	seederSource func() (io.ReadCloser, error)

	db *badger.DB

	// lock to prevent concurrent access to db during restore
	restoringLock sync.RWMutex
	restoring     bool
	stats         stats.Stats
}

// NewRepository returns a new repository backed by badgerdb.
func NewRepository(basePath string, log logger.Logger, stats stats.Stats, opts ...Opt) (*Repository, error) {
	b := &Repository{
		log:           log,
		path:          path.Join(basePath, "badgerdbv3"),
		maxGoroutines: 1,
		maxSeedWait:   10 * time.Second,
		stats:         stats,
	}
	for _, opt := range opts {
		opt(b)
	}
	err := b.start()
	return b, err
}

// GetToken returns the current token
func (b *Repository) GetToken() ([]byte, error) {
	b.restoringLock.RLock()
	defer b.restoringLock.RUnlock() // release the read lock at the end of the operation
	if b.restoring {
		return nil, model.ErrRestoring
	}
	if b.db.IsClosed() {
		return nil, badger.ErrDBClosed
	}

	var token []byte
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(tokenKey))
		if err != nil {
			return fmt.Errorf("could not get token: %w", err)
		}
		if err = item.Value(func(val []byte) error {
			token = append([]byte{}, val...)
			return nil
		}); err != nil {
			return fmt.Errorf("could not get token value: %w", err)
		}
		return nil
	})
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return nil, err
	}
	return token, nil
}

// Suppressed returns true if the given user is suppressed, false otherwise
func (b *Repository) Suppressed(workspaceID, userID, sourceID string) (bool, error) {
	b.restoringLock.RLock()
	defer b.restoringLock.RUnlock()
	if b.restoring {
		return false, model.ErrRestoring
	}
	if b.db.IsClosed() {
		return false, badger.ErrDBClosed
	}

	keyPrefix := keyPrefix(workspaceID, userID)
	err := b.db.View(func(txn *badger.Txn) error {
		wildcardKey := keyPrefix + model.Wildcard
		_, err := txn.Get([]byte(wildcardKey))
		if err == nil {
			return nil
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("could not get wildcard key %s: %w", wildcardKey, err)
		}
		sourceKey := keyPrefix + sourceID
		if _, err = txn.Get([]byte(sourceKey)); err != nil {
			return fmt.Errorf("could not get sourceID key %s: %w", sourceKey, err)
		}
		return err
	})
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Add adds the given suppressions to the repository
func (b *Repository) Add(suppressions []model.Suppression, token []byte) error {
	b.restoringLock.RLock()
	defer b.restoringLock.RUnlock()
	if b.restoring {
		return model.ErrRestoring
	}
	if b.db.IsClosed() {
		return badger.ErrDBClosed
	}
	wb := b.db.NewWriteBatch()
	defer wb.Cancel()

	for i := range suppressions {
		suppression := suppressions[i]
		keyPrefix := keyPrefix(suppression.WorkspaceID, suppression.UserID)
		var keys []string
		if len(suppression.SourceIDs) == 0 {
			keys = []string{keyPrefix + model.Wildcard}
		} else {
			keys = make([]string, len(suppression.SourceIDs))
			for i, sourceID := range suppression.SourceIDs {
				keys[i] = keyPrefix + sourceID
			}
		}
		for _, key := range keys {
			var err error
			if suppression.Canceled {
				err = wb.Delete([]byte(key))
			} else {
				err = wb.Set([]byte(key), []byte(""))
			}
			if err != nil {
				return fmt.Errorf("could not add key %s (canceled:%t) in write batch: %w", key, suppression.Canceled, err)
			}
		}

	}
	if err := wb.Set([]byte(tokenKey), token); err != nil {
		return fmt.Errorf("could not add token key %s in write batch: %w", tokenKey, err)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("could not flush write batch: %w", err)
	}
	return nil
}

// start the repository
func (b *Repository) start() (startErr error) {
	var seeder io.ReadCloser
	_, err := os.Stat(b.path)
	if os.IsNotExist(err) && b.seederSource != nil {
		seeder, err = b.seederSource()
		if err != nil {
			return fmt.Errorf("could not get seeder source: %w", err)
		}
		defer func() {
			if startErr != nil && seeder != nil {
				_ = seeder.Close()
			}
		}()
	}

	opts := badger.
		DefaultOptions(b.path).
		WithLogger(blogger{b.log}).
		WithCompression(options.None).
		WithIndexCacheSize(16 << 20). // 16mb
		WithNumGoroutines(b.maxGoroutines)

	b.db, startErr = badger.Open(opts)
	if startErr != nil {
		startErr = fmt.Errorf("could not open badgerdb: %w", startErr)
		return
	}

	if seeder != nil {
		restoreDone := lo.Async(func() error {
			defer func() { _ = seeder.Close() }()
			if err := b.Restore(seeder); err != nil {
				b.log.Error("Failed to restore badgerdb", "error", err)
				return err
			}
			return nil
		})
		var timeout <-chan time.Time
		if b.maxSeedWait > 0 {
			timeout = time.After(b.maxSeedWait)
		}

		select {
		case startErr = <-restoreDone:
			if startErr != nil {
				startErr = fmt.Errorf("failed to restore badgerdb: %w", err)
				return
			}
		case <-timeout:
			b.log.Warn("Badgerdb still restoring after %s, proceeding...", b.maxSeedWait)
		}
	}

	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			if b.db.IsClosed() {
				return
			} else {
				time.Sleep(5 * time.Minute)
			again: // see https://dgraph.io/docs/badger/get-started/#garbage-collection
				err := b.db.RunValueLogGC(0.7)
				if err == nil {
					goto again
				}
				lsmSize, vlogSize, totSize, err := misc.GetBadgerDBUsage(b.db.Opts().Dir)
				if err != nil {
					b.log.Errorf("Error while getting badgerDB usage: %v", err)
					continue
				}
				statName := "suppress-user"
				b.stats.NewTaggedStat("badger_db_size", stats.GaugeType, stats.Tags{"name": statName, "type": "lsm"}).Gauge((lsmSize))
				b.stats.NewTaggedStat("badger_db_size", stats.GaugeType, stats.Tags{"name": statName, "type": "vlog"}).Gauge((vlogSize))
				b.stats.NewTaggedStat("badger_db_size", stats.GaugeType, stats.Tags{"name": statName, "type": "total"}).Gauge((totSize))
			}
		}
	}()
	return nil
}

// Stop stops the repository
func (b *Repository) Stop() error {
	return b.db.Close()
}

// Backup writes a backup of the repository to the given writer
func (b *Repository) Backup(w io.Writer) error {
	b.restoringLock.RLock()
	defer b.restoringLock.RUnlock()
	if b.restoring {
		return model.ErrRestoring
	}
	if b.db.IsClosed() {
		return badger.ErrDBClosed
	}
	_, err := b.db.Backup(w, 0)
	return err
}

// Restore restores the repository from the given reader
func (b *Repository) Restore(r io.Reader) (err error) {
	if b.isRestoring() {
		return model.ErrRestoring
	}
	if b.db.IsClosed() {
		return badger.ErrDBClosed
	}
	b.setRestoring(true)
	defer b.setRestoring(false)
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic during restore: %v", r)
		}
	}()
	_, err = os.Create(filepath.Join(filepath.Dir(b.path), model.SyncInProgressMarker))
	if err != nil {
		return fmt.Errorf("could not create sync in progress marker: %w", err)
	}
	err = b.db.Load(r, b.maxGoroutines)
	if err == nil {
		err = os.Remove(filepath.Join(filepath.Dir(b.path), model.SyncInProgressMarker))
		if err != nil {
			b.log.Errorf("could not remove sync in progress marker: %v", err)
		}
		_, err = os.Create(filepath.Join(filepath.Dir(b.path), model.SyncDoneMarker))
		if err != nil {
			b.log.Errorf("could not create sync done marker: %v", err)
			return fmt.Errorf("could not create sync done marker: %w", err)
		}
		return nil
	}
	return fmt.Errorf("could not restore badgerdb: %w", err)
}

func (b *Repository) setRestoring(restoring bool) {
	b.restoringLock.Lock()
	b.restoring = restoring
	b.restoringLock.Unlock()
}

func (b *Repository) isRestoring() bool {
	b.restoringLock.RLock()
	defer b.restoringLock.RUnlock()
	return b.restoring
}

type blogger struct {
	logger.Logger
}

func (l blogger) Warningf(fmt string, args ...interface{}) {
	l.Warnf(fmt, args...)
}

func keyPrefix(workspaceID, userID string) string {
	return fmt.Sprintf("%s:%s:", workspaceID, userID)
}
