package flusher

import (
	"context"
	"errors"
	"net/url"
	"path"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/aggregator"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/db"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func CreateRunner(ctx context.Context, table string, log logger.Logger, stats stats.Stats, conf *config.Config) (*CronRunner, error) {
	connStr := misc.GetConnectionString(conf, "reporting")
	maxOpenConns := conf.GetIntVar(4, 1, "Reporting.flusher.maxOpenConnections")
	db, err := db.New(connStr, maxOpenConns)
	if err != nil {
		return nil, err
	}

	if table == "tracked_users_reports" {

		reportingBaseURL := config.GetString("REPORTING_URL", "https://reporting.rudderstack.com/")
		parsedURL, err := url.Parse(reportingBaseURL)
		if err != nil {
			return nil, err
		}
		parsedURL.Path = path.Join(parsedURL.Path, "trackedUser")
		reportingURL := parsedURL.String()

		a := aggregator.NewTrackedUsersInAppAggregator(db.DB)

		f := NewFlusher(ctx, db, log, stats, conf, table, reportingURL, true, a)

		c := NewCronRunner(ctx, log, stats, conf, f, a, table)

		return c, err
	}

	return nil, errors.New("invalid table name. Only tracked_users_reports is supported now")
}
