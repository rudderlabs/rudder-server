package drain_config

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/collectors"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	defaultPollFrequency      = 10
	defaultPollFrequencyUnits = time.Second

	defaultCleanupFrequency      = 1
	defaultCleanupFrequencyUnits = time.Hour

	defaultMaxAge      = 24
	defaultMaxAgeUnits = time.Hour

	// drain configurations

	jobRunIDKey = "drain.jobRunIDs"
)

type drainConfigManager struct {
	log  logger.Logger
	conf *config.Config
	db   *sql.DB

	done *atomic.Bool
	wg   sync.WaitGroup
}

// NewDrainConfigManager returns a drainConfigManager
// If migration fails while setting up drain config, drainConfigManager object will be returned along with error
// Consumers must handle errors and non-nil drainConfigManager object according to their use case.
func NewDrainConfigManager(conf *config.Config, log logger.Logger, stats stats.Stats) (*drainConfigManager, error) {
	db, err := setupDBConn(conf, stats)
	if err != nil {
		log.Errorw("db setup", "error", err)
		return nil, fmt.Errorf("db setup: %v", err)
	}
	if err = migrate(db); err != nil {
		log.Errorw("db migrations", "error", err)
	}
	return &drainConfigManager{
		log:  log,
		conf: conf,
		db:   db,

		done: &atomic.Bool{},
	}, err
}

func (d *drainConfigManager) CleanupRoutine(ctx context.Context) error {
	d.wg.Add(1)
	defer d.wg.Done()
	for {
		if d.done.Load() {
			return nil
		}
		if _, err := d.db.ExecContext(
			ctx,
			"DELETE FROM drain_config WHERE created_at < $1",
			time.Now().Add(-d.conf.GetDuration("drain.age", defaultMaxAge, defaultMaxAgeUnits)),
		); err != nil && ctx.Err() == nil {
			d.log.Errorw("db cleanup", "error", err)
			return fmt.Errorf("db cleanup: %v", err)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(
			d.conf.GetDuration(
				"drainConfig.cleanupFrequency",
				defaultCleanupFrequency,
				defaultCleanupFrequencyUnits,
			),
		):
		}
	}
}

func (d *drainConfigManager) DrainConfigRoutine(ctx context.Context) error {
	d.wg.Add(1)
	defer d.wg.Done()
	// map to hold the config values
	configMap := map[string][]string{
		jobRunIDKey: nil,
	}
	for {
		if d.done.Load() {
			return nil
		}
		// holds the config values fetched from the db
		dbConfigMap := make(map[string][]string)

		collectFromDB := func() error {
			rows, err := d.db.QueryContext(
				ctx,
				"SELECT key, value FROM drain_config where created_at > $1 ORDER BY key, value ASC",
				time.Now().Add(-1*d.conf.GetDuration("drain.age", defaultMaxAge, defaultMaxAgeUnits)),
			)
			if err != nil {
				d.log.Errorw("db query", "error", err)
				return fmt.Errorf("db query: %v", err)
			}
			for rows.Next() {
				var (
					key   string
					value string
				)
				if err := rows.Scan(&key, &value); err != nil {
					d.log.Errorw("db scan", "error", err)
					return fmt.Errorf("db scan: %v", err)
				}

				if value == "" {
					continue
				}
				dbConfigMap[key] = append(dbConfigMap[key], value)
			}
			if err := rows.Err(); err != nil {
				d.log.Errorw("db rows", "error", err)
				return fmt.Errorf("db rows.Err: %v", err)
			}
			if err := rows.Close(); err != nil {
				d.log.Errorw("db rows close", "error", err)
				return fmt.Errorf("db rows close: %v", err)
			}
			return nil
		}
		if err := collectFromDB(); err != nil && ctx.Err() == nil {
			return fmt.Errorf("read from db: %v", err)
		}

		// compare config values, if different set the config
		for key := range configMap {
			if !slices.Equal(configMap[key], dbConfigMap[key]) {
				configMap[key] = dbConfigMap[key]
				d.conf.Set(key, dbConfigMap[key])
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(
			d.conf.GetDuration(
				"drainConfig.pollFrequency",
				defaultPollFrequency,
				defaultPollFrequencyUnits,
			),
		):
		}
	}
}

func (d *drainConfigManager) Stop() {
	d.done.Store(true)
	_ = d.db.Close()
	d.wg.Wait()
}

func migrate(db *sql.DB) error {
	m := &migrator.Migrator{
		Handle:                     db,
		MigrationsTable:            "drain_config_migrations",
		ShouldForceSetLowerVersion: config.GetBool("SQLMigrator.forceSetLowerVersion", true),
	}

	return m.Migrate("drain_config")
}

// setupDBConn sets up the database connection
func setupDBConn(conf *config.Config, stats stats.Stats) (*sql.DB, error) {
	psqlInfo := misc.GetConnectionString(conf, "drain-config")
	if conf.IsSet("SharedDB.dsn") {
		psqlInfo = conf.GetString("SharedDB.dsn", "")
	}
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, fmt.Errorf("db open: %v", err)
	}
	db.SetMaxIdleConns(conf.GetInt("drainConfig.maxIdleConns", 1))
	db.SetMaxOpenConns(conf.GetInt("drainConfig.maxOpenConns", 2))
	err = stats.RegisterCollector(collectors.NewDatabaseSQLStats("drain_config", db))
	if err != nil {
		return nil, fmt.Errorf("registering collector: %v", err)
	}
	return db, nil
}
