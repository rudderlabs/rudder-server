package flusher

import (
	"context"
	"fmt"
	"strings"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/db"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/handler"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func CreateFlusher(ctx context.Context, table string, log logger.Logger, stats stats.Stats) *Flusher {
	connStr := misc.GetConnectionString(config.Default, "reporting")
	maxOpenConns := config.GetIntVar(4, 1, "Reporting.flusher.maxOpenConnections")
	db := db.NewPostgresDB(connStr, maxOpenConns)

	if table == "tracked_users_reports" {
		labels := []string{"workspace_id", "source_id", "instance_id"}
		values := []string{"userid_hll", "anonymousid_hll", "identified_anonymousid_hll"}

		reportingURL := fmt.Sprintf("%s/trackedUser", strings.TrimSuffix(config.GetString("REPORTING_URL", "https://reporting.rudderstack.com/"), "/"))

		tuHandler := handler.NewTrackedUsersHandler(table, labels)
		f := NewFlusher(ctx, db, log, stats, table, labels, values, reportingURL, true, tuHandler)

		return f
	}

	return nil
}
