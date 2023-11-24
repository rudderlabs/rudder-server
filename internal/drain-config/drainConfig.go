package drain_config

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	defaultPollFrequency      = 5
	defaultPollFrequencyUnits = time.Second

	defaultCleanupFrequency      = 24
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
}

func NewDrainConfigManager(conf *config.Config, log logger.Logger) (*drainConfigManager, error) {
	db, err := setupDBConn(misc.GetConnectionString(conf))
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
	}, nil
}

func (d *drainConfigManager) CleanupRoutine(ctx context.Context) error {
	for {
		maxAgeInMinutes := d.conf.GetDuration("drain.age", defaultMaxAge, defaultMaxAgeUnits) / time.Minute
		if _, err := d.db.ExecContext(
			ctx,
			"DELETE FROM drain_config WHERE created_at < NOW() - $1 * interval '1 MIN'",
			maxAgeInMinutes,
		); err != nil {
			d.log.Errorw("db cleanup", "error", err)
			return fmt.Errorf("db cleanup: %v", err)
		}
		if err := misc.SleepCtx(
			ctx,
			d.conf.GetDuration(
				"drainConfig.cleanupFrequency",
				defaultCleanupFrequency,
				defaultCleanupFrequencyUnits,
			),
		); err != nil {
			return err
		}
	}
}

func (d *drainConfigManager) DrainConfigRoutine(ctx context.Context) error {
	// map to hold the config values
	configMap := make(map[string][]string)
	for {
		// holds the config values fetched from the db
		dbConfigMap := make(map[string][]string)

		// fetch the config values from the db
		rows, err := d.db.QueryContext(
			ctx, "SELECT id, created_at, value FROM drain_config where key=$1 order by id asc",
			jobRunID,
		)
		if err != nil {
			d.log.Errorw("db query", "error", err)
			return fmt.Errorf("db query: %v", err)
		}
		for rows.Next() {
			var (
				id        int64
				value     string
				createdAt time.Time
			)
			if err := rows.Scan(&id, &createdAt, &value); err != nil {
				d.log.Errorw("db scan", "error", err)
				return fmt.Errorf("db scan: %v", err)
			}

			if value == "" || time.Since(createdAt) > defaultMaxAge*defaultMaxAgeUnits {
				continue
			}
			dbConfigMap[jobRunID] = append(dbConfigMap[jobRunID], separateAndTrim(value)...)
		}
		if err := rows.Err(); err != nil {
			d.log.Errorw("db rows", "error", err)
			return fmt.Errorf("db rows: %v", err)
		}
		if err := rows.Close(); err != nil {
			d.log.Errorw("db rows close", "error", err)
			return fmt.Errorf("db rows close: %v", err)
		}

		// compare config values, if different set the config
		configVals := append(dbConfigMap[jobRunID], d.conf.GetStringSlice(configJobRunID, nil)...)
		if !slices.Equal(
			configMap[jobRunID],
			configVals,
		) {
			configMap[jobRunID] = configVals
			d.conf.Set(jobRunID, configVals)
		}

		if err := misc.SleepCtx(
			ctx,
			d.conf.GetDuration(
				"drainConfig.pollFrequency",
				defaultPollFrequency,
				defaultPollFrequencyUnits,
			),
		); err != nil {
			return err
		}
	}
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
func setupDBConn(psqlInfo string) (*sql.DB, error) {
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, fmt.Errorf("db open: %v", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("db ping: %v", err)
	}
	return db, nil
}

// separateAndTrim splits the string by comma and trims the values
//
// returns slice of non empty strings
func separateAndTrim(s string) []string {
	return lo.FilterMap(
		strings.Split(s, ","),
		func(val string, _ int) (string, bool) {
			trimmed := strings.TrimSpace(val)
			return trimmed, trimmed != ""
		},
	)
}
