package flusher

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path"
	"slices"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/collectors"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/aggregator"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var supportedTables = []string{"tracked_users_reports"}

// This function has to be called once for each table
func CreateRunner(ctx context.Context, table string, log logger.Logger, stats stats.Stats, conf *config.Config, module string) (Runner, error) {
	if !slices.Contains(supportedTables, table) {
		return nil, errors.New("invalid table name. Only tracked_users_reports is supported now")
	}

	connStr := misc.GetConnectionString(conf, "reporting")
	maxOpenConns := conf.GetIntVar(4, 1, "Reporting.flusher.maxOpenConnections")
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("opening postgres: %w", err)
	}
	db.SetMaxOpenConns(maxOpenConns)
	err = stats.RegisterCollector(collectors.NewDatabaseSQLStats(fmt.Sprintf("reporting_flusher_%s", table), db))
	if err != nil {
		log.Errorn("error registering database sql stats", obskit.Error(err))
	}

	if table == "tracked_users_reports" {

		if !conf.GetBool("TrackedUsers.enabled", false) {
			return &NOPCronRunner{}, nil
		}

		reportingBaseURL := config.GetString("REPORTING_URL", "https://reporting.rudderstack.com/")
		parsedURL, err := url.Parse(reportingBaseURL)
		if err != nil {
			return nil, fmt.Errorf("error parsing reporting url %w", err)
		}
		parsedURL.Path = path.Join(parsedURL.Path, "trackedUser")
		reportingURL := parsedURL.String()

		a := aggregator.NewTrackedUsersInAppAggregator(db, stats, conf, module)
		f, err := NewFlusher(db, log, stats, conf, table, reportingURL, a, module)
		if err != nil {
			return nil, fmt.Errorf("error creating flusher %w", err)
		}

		c := NewCronRunner(ctx, log, stats, conf, f, table, module)

		return c, err
	}

	return nil, errors.New("invalid table name. Only tracked_users_reports is supported now")
}
