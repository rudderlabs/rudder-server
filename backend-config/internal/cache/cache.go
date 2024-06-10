//go:generate mockgen -destination=./mock_cache.go -package=cache -source=./cache.go cache
package cache

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"fmt"

	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

var (
	pkgLogger = logger.NewLogger().Child("backend-config-cache")
	json      = jsoniter.ConfigCompatibleWithStandardLibrary
)

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
		pkgLogger.Errorf("failed to setup db: %v", err)
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
		pkgLogger.Errorf("failed to apply db migrations: %v", err)
		return nil, err
	}

	// clear config for other keys
	err = dbStore.clear(ctx)
	if err != nil {
		pkgLogger.Errorf("failed to clear previous config: %v", err)
		return nil, err
	}

	go func() {
		// subscribe to config and write to db
		for config := range channelProvider() {
			// persist to database
			err = dbStore.set(ctx, config.Data)
			if err != nil {
				pkgLogger.Errorf("failed writing config to database: %v", err)
			}
		}
		dbStore.Close()
	}()
	return &dbStore, nil
}

// Encrypt and store the config to the database
func (db *cacheStore) set(ctx context.Context, config interface{}) error {
	configBytes, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}
	// encrypt
	encrypted, err := db.encryptAES(configBytes)
	if err != nil {
		return err
	}
	// write to config table
	_, err = db.ExecContext(
		ctx,
		`INSERT INTO config_cache (key, config) VALUES ($1, $2)
		on conflict (key)
		do update set
		config = $2,
		updated_at = NOW()`,
		db.key,
		encrypted,
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
		pkgLogger.Errorf("failed to open db: %v", err)
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		pkgLogger.Errorf("failed to ping db: %v", err)
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
		ShouldForceSetLowerVersion: config.GetBool("SQLMigrator.forceSetLowerVersion", true),
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
