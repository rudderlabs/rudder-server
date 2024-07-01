package flusher

import (
	"context"
	"errors"
	"net/url"
	"path"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/db"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/handler"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func CreateFlusher(ctx context.Context, table string, log logger.Logger, stats stats.Stats, conf *config.Config) (*Flusher, error) {
	connStr := misc.GetConnectionString(conf, "reporting")
	maxOpenConns := conf.GetIntVar(4, 1, "Reporting.flusher.maxOpenConnections")
	db, err := db.New(connStr, maxOpenConns)
	if err != nil {
		return nil, err
	}

	if table == "tracked_users_reports" {
		labels := []string{"workspace_id", "source_id", "instance_id"}

		reportingBaseURL := config.GetString("REPORTING_URL", "https://reporting.rudderstack.com/")
		parsedURL, err := url.Parse(reportingBaseURL)
		if err != nil {
			return nil, err
		}
		parsedURL.Path = path.Join(parsedURL.Path, "trackedUser")
		reportingURL := parsedURL.String()

		tuHandler := handler.NewTrackedUsersHandler(table, labels)
		f := NewFlusher(ctx, db, log, stats, conf, table, labels, reportingURL, true, tuHandler)

		return f, err
	}

	return nil, errors.New("invalid table name for flusher. Only tracked_users_reports is supported now")
}
