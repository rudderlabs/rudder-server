//go:generate mockgen -destination=./mock_cache.go -package=cache -source=./cache.go cache
package cache

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

var pkgLogger = logger.NewLogger().Child("backend-config-cache")

type Cache interface {
	Get(ctx context.Context) ([]byte, error)
}

type cacheStore struct {
	*sql.DB
	secret [32]byte
	key    string
}

// Start returns a new Cache instance, and starts a goroutine to cache the config
//
// secret is the secret key to encrypt the config with before storing it
//
// key is the key to use to store and fetch the config from the cache store
//
// ch is the channel to listen on for config updates and store them
func Start(ctx context.Context, secret [32]byte, key string, channelProvider func() pubsub.DataChannel) (Cache, error) {
	var (
		err    error
		dbConn *sql.DB
	)
	// setup db connection
	dbConn, err = setupDBConn()
	if err != nil {
		pkgLogger.Errorn("failed to setup db", obskit.Error(err))
		return nil, err
	}
	dbStore := cacheStore{
		dbConn,
		secret,
		key,
	}

	// apply migrations
	err = migrate(dbConn)
	if err != nil {
		pkgLogger.Errorn("failed to apply db migrations", obskit.Error(err))
		return nil, err
	}

	// clear config for other keys
	err = dbStore.clear(ctx)
	if err != nil {
		pkgLogger.Errorn("failed to clear previous config", obskit.Error(err))
		return nil, err
	}

	// writeDebounce coalesces a burst of config updates into a single write
	writeDebounce := config.GetDurationVar(10, time.Second, "BackendConfig.dbCacheWriteDebounce")
	go func() {
		var (
			latestConfig any
			gotConfig    bool
			configMu     sync.Mutex
		)
		// persist writes the most recently received config to the database
		persist := func() {
			configMu.Lock()
			data, ok := latestConfig, gotConfig
			configMu.Unlock()
			if !ok {
				return
			}
			if err := dbStore.set(ctx, data); err != nil {
				pkgLogger.Errorn("failed writing config to database", obskit.Error(err))
			}
		}
		debouncedPersist, cancelDebounce := lo.NewDebounce(writeDebounce, persist)
		// subscribe to config and debounce writes to db
		for update := range channelProvider() {
			configMu.Lock()
			latestConfig, gotConfig = update.Data, true
			configMu.Unlock()
			debouncedPersist()
		}
		// channel closed: flush the latest config before shutting down
		cancelDebounce()
		persist()
		dbStore.Close()
	}()
	return &dbStore, nil
}

// Encrypt and store the config to the database
func (db *cacheStore) set(ctx context.Context, config any) error {
	configBytes, err := jsonrs.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}
	configSum := sha256.Sum256(configBytes)
	configHash := hex.EncodeToString(configSum[:])

	// Skip the write if the stored config is already identical. This avoids
	// encrypting and transferring the (potentially large) config blob to the
	// database when nothing has changed.
	var storedHash sql.NullString
	err = db.QueryRowContext(
		ctx,
		`SELECT config_hash FROM config_cache WHERE key = $1`,
		db.key,
	).Scan(&storedHash)
	switch err {
	case nil:
		if storedHash.Valid && storedHash.String == configHash {
			return nil
		}
	case sql.ErrNoRows:
	default:
		return err
	}

	// encrypt
	encrypted, err := db.encryptAES(configBytes)
	if err != nil {
		return err
	}
	// write to config table
	_, err = db.ExecContext(
		ctx,
		`INSERT INTO config_cache (key, config, config_hash) VALUES ($1, $2, $3)
		on conflict (key)
		do update set
		config = $2,
		config_hash = $3,
		updated_at = NOW()
		where config_cache.config_hash is distinct from excluded.config_hash`,
		db.key,
		encrypted,
		configHash,
	)
	return err
}

// Fetch the cached config when needed
func (db *cacheStore) Get(ctx context.Context) ([]byte, error) {
	// read from database
	var (
		config []byte
		err    error
	)
	err = db.QueryRowContext(
		ctx,
		`SELECT config FROM config_cache WHERE key = $1`,
		db.key,
	).Scan(&config)
	switch err {
	case nil:
	case sql.ErrNoRows:
		// maybe fetch the config where workspaces = ''?
		return nil, err
	default:
		return nil, err
	}
	// decrypt and return
	return db.decryptAES(config)
}

// setupDBConn sets up the database connection, creates the config table if it doesn't exist
func setupDBConn() (*sql.DB, error) {
	psqlInfo := misc.GetConnectionString(config.Default, "backend-config")
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		pkgLogger.Errorn("failed to open db", obskit.Error(err))
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		pkgLogger.Errorn("failed to ping db", obskit.Error(err))
		return nil, err
	}
	return db, nil
}

// clear config for all other keys
func (db *cacheStore) clear(ctx context.Context) error {
	_, err := db.ExecContext(ctx, `DELETE FROM config_cache WHERE key != $1`, db.key)
	return err
}

// apply config_cache migrations to the database
func migrate(db *sql.DB) error {
	m := &migrator.Migrator{
		Handle:                     db,
		MigrationsTable:            "config_cache_migrations",
		ShouldForceSetLowerVersion: config.GetBoolVar(true, "SQLMigrator.forceSetLowerVersion"),
	}

	return m.Migrate("config_cache")
}

func (db *cacheStore) encryptAES(data []byte) ([]byte, error) {
	gcm, err := newGCM(db.secret)
	if err != nil {
		return nil, fmt.Errorf("failed to create encrypt gcm: %w", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}
	return gcm.Seal(nonce, nonce, data, nil), nil
}

func (db *cacheStore) decryptAES(data []byte) ([]byte, error) {
	gcm, err := newGCM(db.secret)
	if err != nil {
		return nil, fmt.Errorf("failed to create decrypt gcm: %w", err)
	}
	nonceSize := gcm.NonceSize()
	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	out, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt: %w", err)
	}
	return out, nil
}

func newGCM(secret [32]byte) (cipher.AEAD, error) {
	// We need a 32-bytes key for AES-256
	// thus converting the secret to md5 (32-digit hexadecimal number)
	c, err := aes.NewCipher(secret[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(c)
}
