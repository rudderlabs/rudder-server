package flusher

import (
	"context"
	"errors"
	"net/url"
	"path"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/aggregator"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/db"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	flusherInstances = make(map[string]*Flusher)
	flusherMu        sync.Mutex
)

func getFlusher(table string) *Flusher {
	flusherMu.Lock()
	defer flusherMu.Unlock()

	if instance, exists := flusherInstances[table]; exists {
		return instance
	}
	return nil
}

func addFlusher(table string, flusher *Flusher) {
	flusherMu.Lock()
	defer flusherMu.Unlock()

	flusherInstances[table] = flusher
}

// This function has to be called once for each table
func CreateRunner(ctx context.Context, table string, log logger.Logger, stats stats.Stats, conf *config.Config, module string) (Runner, error) {
	connStr := misc.GetConnectionString(conf, "reporting")
	maxOpenConns := conf.GetIntVar(4, 1, "Reporting.flusher.maxOpenConnections")
	db, err := db.New(connStr, maxOpenConns)
	if err != nil {
		return nil, err
	}

	if table == "tracked_users_reports" {

		if !conf.GetBool("TrackedUsers.enabled", false) {
			return &NOPCronRunner{}, nil
		}

		reportingBaseURL := config.GetString("REPORTING_URL", "https://reporting.rudderstack.com/")
		parsedURL, err := url.Parse(reportingBaseURL)
		if err != nil {
			return nil, err
		}
		parsedURL.Path = path.Join(parsedURL.Path, "trackedUser")
		reportingURL := parsedURL.String()

		a := aggregator.NewTrackedUsersInAppAggregator(db.DB, stats, conf, table, module)

		f := getFlusher(table)

		if f == nil {
			f = NewFlusher(db, log, stats, conf, table, reportingURL, a, module)
			addFlusher(table, f)
		}

		c := NewCronRunner(ctx, log, stats, conf, f, a, table, module)

		return c, err
	}

	return nil, errors.New("invalid table name. Only tracked_users_reports is supported now")
}
