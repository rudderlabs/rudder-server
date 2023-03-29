package badgerdb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/rudderlabs/rudder-go-kit/config"
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
func WithSeederSource(seederSource func() (io.Reader, error)) Opt {
	return func(r *Repository) {
		r.repo.seederSource = seederSource
	}
}

// WithMaxSeedWait sets the maximum time to wait for the seed to complete.
// If the seed takes longer than this, the repository will be started in restoring state and all
// repository methods will return [ErrRestoring] until the seed completes. The default wait time is 10 seconds.
func WithMaxSeedWait(maxSeedWait time.Duration) Opt {
	return func(r *Repository) {
		r.repo.maxSeedWait = maxSeedWait
	}
}

// Repository is a repository backed by badgerdb
type Repository struct {
	repo     *repository
	tempRepo *repository
}

type repository struct {
	// logger to use
	log logger.Logger
	// path to the badger db directory
	path string
	// max number of goroutines to use (badger config)
	maxGoroutines int

	maxSeedWait  time.Duration
	seederSource func() (io.Reader, error)

	db *badger.DB

	// lock to prevent concurrent access to db during restore
	restoringLock sync.RWMutex
	restoring     bool
	closeOnce     sync.Once
	closed        chan struct{}
	stats         stats.Stats
}

func latestDataSeed() (io.Reader, error) {
	return seederSource("latest-export")
}

func fullDataSeed() (io.Reader, error) {
	return seederSource("full-export")
}

func seederSource(endpoint string) (io.Reader, error) {
	client := http.Client{
		Timeout: config.GetDuration("HttpClient.suppressUser.timeout", 600, time.Second),
	}
	baseURL := config.GetString("SUPPRESS_BACKUP_URL", "https://api.rudderstack.com")
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%s", baseURL, endpoint), nil)
	if err != nil {
		return nil, fmt.Errorf("could not create request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("could not perform request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("could not read response body: %w", err)
	}
	return bytes.NewReader(respBody), nil
}

// NewRepository returns a new repository backed by badgerdb.
func NewRepository(basePath string, useBackupSvc bool, log logger.Logger, stats stats.Stats, opts ...Opt) (*Repository, error) {
	b := &Repository{
		repo: &repository{
			log:           log,
			path:          path.Join(basePath, "badgerdbv3"),
			maxGoroutines: config.GetInt("SuppressUser.maxGoroutines", 1),
			maxSeedWait:   10 * time.Second,
			stats:         stats,
		},
	}
	for _, opt := range opts {
		opt(b)
	}
	var originalRestore <-chan error
	var err error
	originalRestore = lo.Async(func() error {
		if useBackupSvc {
			b.repo.seederSource = fullDataSeed
		}
		err := b.repo.start()
		if err != nil {
			return fmt.Errorf("could not start main repository: %w", err)
		}
		return nil
	})
	restoreDone := lo.Async(func() error {
		if useBackupSvc {
			b.tempRepo = &repository{
				log:           log,
				path:          path.Join(basePath, "badgerdbv3-temp"),
				maxGoroutines: config.GetInt("SuppressUser.maxGoroutines", 1),
				maxSeedWait:   config.GetDuration("SuppressUser.latestDataSeedWait", 30, time.Second),
				stats:         stats,
				seederSource:  latestDataSeed,
			}
			err = b.tempRepo.start()
			if err != nil {
				return fmt.Errorf("could not start temp repository: %w", err)
			}
			return nil
		} else {
			return <-originalRestore
		}
	})

	return b, <-restoreDone
}

func (r *repository) getToken() ([]byte, error) {
	r.restoringLock.RLock()
	defer r.restoringLock.RUnlock() // release the read lock at the end of the operation
	if r.restoring {
		return nil, model.ErrRestoring
	}
	if r.db.IsClosed() {
		return nil, badger.ErrDBClosed
	}

	var token []byte
	err := r.db.View(func(txn *badger.Txn) error {
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

// GetToken returns the current token
func (b *Repository) GetToken() ([]byte, error) {
	if b.repo.restoring && b.tempRepo != nil {
		return b.tempRepo.getToken()
	}
	return b.repo.getToken()
}

func (r *repository) suppressed(workspaceID, userID, sourceID string) (bool, error) {
	r.restoringLock.RLock()
	defer r.restoringLock.RUnlock()
	if r.restoring {
		return false, model.ErrRestoring
	}
	if r.db.IsClosed() {
		return false, badger.ErrDBClosed
	}

	keyPrefix := keyPrefix(workspaceID, userID)
	err := r.db.View(func(txn *badger.Txn) error {
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

// Suppressed returns true if the given user is suppressed, false otherwise
func (b *Repository) Suppressed(workspaceID, userID, sourceID string) (bool, error) {
	if b.repo.restoring && b.tempRepo != nil {
		return b.tempRepo.suppressed(workspaceID, userID, sourceID)
	}
	return b.repo.suppressed(workspaceID, userID, sourceID)
}

func (r *repository) add(suppressions []model.Suppression, token []byte) error {
	r.restoringLock.RLock()
	defer r.restoringLock.RUnlock()
	if r.restoring {
		return model.ErrRestoring
	}
	if r.db.IsClosed() {
		return badger.ErrDBClosed
	}
	wb := r.db.NewWriteBatch()
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

// Add adds the given suppressions to the repository
func (b *Repository) Add(suppressions []model.Suppression, token []byte) error {
	if b.repo.restoring && b.tempRepo != nil {
		return b.tempRepo.add(suppressions, token)
	}
	return b.repo.add(suppressions, token)
}

// start the repository
func (b *repository) start() error {
	b.closed = make(chan struct{})
	var seeder io.Reader
	if _, err := os.Stat(b.path); os.IsNotExist(err) && b.seederSource != nil {
		if seeder, err = b.seederSource(); err != nil {
			return err
		}
	}

	opts := badger.
		DefaultOptions(b.path).
		WithLogger(blogger{b.log}).
		WithCompression(options.None).
		WithIndexCacheSize(16 << 20). // 16mb
		WithNumGoroutines(b.maxGoroutines)
	var err error
	b.db, err = badger.Open(opts)
	if err != nil {
		return err
	}

	if seeder != nil {
		restoreDone := lo.Async(func() error {
			if err := b.restore(seeder); err != nil {
				b.log.Error("Failed to restore badgerdb", "error", err)
				return err
			}
			return nil
		})
		select {
		case <-restoreDone:
		case <-time.After(b.maxSeedWait):
			b.log.Warn("Badgerdb still restoring after %s, proceeding...", b.maxSeedWait)
		}
	}

	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-b.closed:
				return
			case <-ticker.C:
			}
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
	}()
	return nil
}

// Stop stops the repository
func (b *Repository) Stop() error {
	var err error
	b.repo.closeOnce.Do(func() {
		close(b.repo.closed)
		err = b.repo.db.Close()
	})
	return err
}

func (r *repository) backup(w io.Writer) error {
	r.restoringLock.RLock()
	defer r.restoringLock.RUnlock()
	if r.restoring {
		return model.ErrRestoring
	}
	if r.db.IsClosed() {
		return badger.ErrDBClosed
	}
	_, err := r.db.Backup(w, 0)
	return err
}

// Backup writes a backup of the repository to the given writer
func (b *Repository) Backup(w io.Writer) error {
	if b.repo.restoring && b.tempRepo != nil {
		return b.tempRepo.backup(w)
	}
	return b.repo.backup(w)
}

func (b *repository) restore(r io.Reader) (err error) {
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
	return b.db.Load(r, b.maxGoroutines)
}

// Restore restores the repository from the given reader
func (b *Repository) Restore(r io.Reader) (err error) {
	return b.repo.restore(r)
}

func (b *repository) setRestoring(restoring bool) {
	b.restoringLock.Lock()
	b.restoring = restoring
	b.restoringLock.Unlock()
}

func (b *repository) isRestoring() bool {
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
