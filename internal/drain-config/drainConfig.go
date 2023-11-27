package drain_config

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lib/pq"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
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

	jobRunID       = "drain.jobRunIDs"
	configJobRunID = "RSources.toAbortJobRunIDs"
)

type drainConfigManager struct {
	log  logger.Logger
	conf *config.Config
	db   *sql.DB

	done *atomic.Bool
	wg   sync.WaitGroup
}

func NewDrainConfigManager(conf *config.Config, log logger.Logger) (*drainConfigManager, error) {
	db, err := setupDBConn(conf)
	if err != nil {
		log.Errorw("db setup", "error", err)
		return nil, fmt.Errorf("db setup: %v", err)
	}
	if err := migrate(db); err != nil {
		log.Errorw("db migrations", "error", err)
		return nil, fmt.Errorf("db migrations: %v", err)
	}
	return &drainConfigManager{
		log:  log,
		conf: conf,
		db:   db,

		done: &atomic.Bool{},
	}, nil
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
	configMap := make(map[string][]string)
	for {
		if d.done.Load() {
			return nil
		}
		// holds the config values fetched from the db
		dbConfigMap := make(map[string][]string)

		collectFromDB := func() error {
			// fetch the config values from the db
			rows, err := d.db.QueryContext(
				ctx, "SELECT key, value FROM drain_config where key = ANY($1) and created_at > $2 ORDER BY key, value ASC",
				pq.Array([]string{jobRunID}),
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
		configVals := lo.Uniq(append(dbConfigMap[jobRunID], d.conf.GetStringSlice(configJobRunID, nil)...))
		if !slices.Equal(configMap[jobRunID], configVals) {
			configMap[jobRunID] = configVals
			d.conf.Set(jobRunID, configVals)
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
func setupDBConn(conf *config.Config) (*sql.DB, error) {
	var psqlInfo string
	if conf.IsSet("SharedDB.dsn") {
		psqlInfo = conf.GetString("SharedDB.dsn", "")
	} else {
		psqlInfo = misc.GetConnectionString(conf)
	}
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, fmt.Errorf("db open: %v", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("db ping: %v", err)
	}
	return db, nil
}
