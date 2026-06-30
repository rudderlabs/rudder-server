package flusher_test

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/segmentio/go-hll"
	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/aggregator"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/testhelper/webhook"
)

func migrateActivationRecordsDatabase(dbConn string, conf *config.Config) error {
	dbHandle, err := sql.Open("postgres", dbConn)
	if err != nil {
		return err
	}
	dbHandle.SetMaxOpenConns(1)

	m := &migrator.Migrator{
		Handle:                     dbHandle,
		MigrationsTable:            "activation_records_reports_migrations",
		ShouldForceSetLowerVersion: conf.GetBoolVar(true, "SQLMigrator.forceSetLowerVersion"),
	}
	err = m.Migrate("activation_records")
	if err != nil {
		return fmt.Errorf("migrating `activation_records_reports` table: %w", err)
	}
	return nil
}

func hllSettingsAR() hll.Settings {
	return hll.Settings{
		Log2m:             14,
		Regwidth:          5,
		ExplicitThreshold: hll.AutoExplicitThreshold,
		SparseEnabled:     true,
	}
}

func addDataToHLLAR(hllData *hll.Hll, min, max, count int) {
	for range count {
		userId := fmt.Sprintf("user_%d", rand.Intn(max-min+1)+min)
		hllData.AddRaw(murmur3.Sum64WithSeed([]byte(userId), 123))
	}
}

func generateActivationRecordsReport(reportedAt time.Time, workspaceId, destinationId string) *aggregator.ActivationRecordsReport {
	fingerprintHLL, _ := hll.NewHll(hllSettingsAR())
	addDataToHLLAR(&fingerprintHLL, 1, 10000, 10000)

	return &aggregator.ActivationRecordsReport{
		ReportedAt:     reportedAt,
		WorkspaceID:    workspaceId,
		SourceID:       "source1",
		DestinationID:  destinationId,
		InstanceID:     "instance1",
		FingerprintHLL: &fingerprintHLL,
	}
}

func addActivationRecordsReportsToDB(ctx context.Context, db *sql.DB, reports []*aggregator.ActivationRecordsReport) error {
	for _, report := range reports {
		_, err := db.ExecContext(ctx, `
			INSERT INTO activation_records_reports (
				reported_at,
				workspace_id,
				source_id,
				destination_id,
				instance_id,
				fingerprint_hll
			) VALUES ($1, $2, $3, $4, $5, $6)
		`,
			report.ReportedAt,
			report.WorkspaceID,
			report.SourceID,
			report.DestinationID,
			report.InstanceID,
			hllToStringAR(report.FingerprintHLL),
		)
		if err != nil {
			return fmt.Errorf("inserting activation records report: %v", err)
		}
	}
	return nil
}

func hllToStringAR(h *hll.Hll) string {
	if h != nil {
		return hex.EncodeToString(h.ToBytes())
	}
	return ""
}

// 1. Add 5 reports to the DB, 4 reports which are 10 minutes old and 1 report which is 1 minute old
// 2. Validate 5 reports were added
// 3. Start the runner
// 4. Wait till all the reports which were added 10 minutes ago to be flushed
// 5. Stop the runner
// 6. Validate we get 2 requests, since we have batch size of 2 (3 aggregated items → 2 batches)
// 7. Validate the payloads sent to Reporting Service
// Reports 0 and 1 share the same key (workspace1/source1/dest1), so their HLLs are unioned.
// Report 2 has a different destination (dest2) → separate aggregation.
// Report 3 has a different workspace (workspace2) → separate aggregation.
// This validates that destination_id is part of the aggregation key.
func TestActivationRecordsFlush(t *testing.T) {
	ctx := context.Background()

	currentUTC := time.Now().UTC()
	oneMinAgo := currentUTC.Add(-1 * time.Minute)
	tenMinAgo := currentUTC.Add(-2 * time.Minute)

	// reports generated with tenMinAgo should be flushed
	reports := []*aggregator.ActivationRecordsReport{
		generateActivationRecordsReport(tenMinAgo, "workspace1", "dest1"), // aggregates with report[1]
		generateActivationRecordsReport(tenMinAgo, "workspace1", "dest1"), // same key as report[0]
		generateActivationRecordsReport(tenMinAgo, "workspace1", "dest2"), // different dest → separate
		generateActivationRecordsReport(tenMinAgo, "workspace2", "dest1"), // different workspace → separate

		generateActivationRecordsReport(oneMinAgo, "workspace1", "dest1"), // recent, NOT flushed
	}

	// set up db
	pool, _ := dockertest.NewPool("")
	pgContainer, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	err = migrateActivationRecordsDatabase(pgContainer.DBDsn, config.Default)
	require.NoError(t, err)
	config.Set("ActivationRecords.enabled", true)
	config.Set("DB.host", pgContainer.Host)
	config.Set("DB.port", pgContainer.Port)
	config.Set("DB.user", pgContainer.User)
	config.Set("DB.name", pgContainer.Database)
	config.Set("DB.password", pgContainer.Password)
	config.Set("Reporting.flusher.batchSizeToReporting", 2)

	err = addActivationRecordsReportsToDB(ctx, pgContainer.DB, reports)
	assert.NoError(t, err)

	// validate 5 reports were added
	var count int
	err = pgContainer.DB.QueryRow("SELECT COUNT(*) FROM activation_records_reports").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 5, count)

	// setup test webhook with reporting url
	testWebhook := webhook.NewRecorder()
	t.Cleanup(testWebhook.Close)
	webhookURL := testWebhook.Server.URL
	config.Set("REPORTING_URL", webhookURL)

	// create runner and start it
	runner, err := flusher.CreateRunner(ctx, "activation_records_reports", logger.NOP, stats.NOP, config.Default, "test")
	require.NoError(t, err)

	runnerDone := make(chan struct{})
	go func() {
		defer close(runnerDone)
		runner.Run()
	}()

	// Wait till all the reports which were added tenMinAgo to be flushed (1 recent report remains)
	require.Eventually(t, func() bool {
		var count int
		_ = pgContainer.DB.QueryRow("SELECT COUNT(*) FROM activation_records_reports").Scan(&count)
		return count == 1
	}, time.Second*30, time.Millisecond*1000)

	runner.Stop()

	// validate we get 2 requests: 3 aggregated items with batch size 2 → 2 requests
	reqCount := testWebhook.RequestsCount()
	assert.Equal(t, 2, reqCount)

	// union the HLLs for the two reports with the same key before building expectations
	reports[0].FingerprintHLL.Union(*reports[1].FingerprintHLL)

	expectedPayloads := map[string]map[string]any{
		"workspace1-source1-dest1-instance1": {
			"reportedAt":     tenMinAgo,
			"workspaceId":    "workspace1",
			"sourceId":       "source1",
			"destinationId":  "dest1",
			"instanceId":     "instance1",
			"fingerprintHLL": hllToStringAR(reports[0].FingerprintHLL),
		},
		"workspace1-source1-dest2-instance1": {
			"reportedAt":     tenMinAgo,
			"workspaceId":    "workspace1",
			"sourceId":       "source1",
			"destinationId":  "dest2",
			"instanceId":     "instance1",
			"fingerprintHLL": hllToStringAR(reports[2].FingerprintHLL),
		},
		"workspace2-source1-dest1-instance1": {
			"reportedAt":     tenMinAgo,
			"workspaceId":    "workspace2",
			"sourceId":       "source1",
			"destinationId":  "dest1",
			"instanceId":     "instance1",
			"fingerprintHLL": hllToStringAR(reports[3].FingerprintHLL),
		},
	}

	// validate the payloads sent to Reporting Service
	reqBodies := testWebhook.Requests()
	for _, req := range reqBodies {
		var items []map[string]any

		body, _ := io.ReadAll(req.Body)
		err := jsonrs.Unmarshal(body, &items)
		require.NoError(t, err)

		for _, item := range items {
			k := fmt.Sprintf("%s-%s-%s-%s", item["workspaceId"], item["sourceId"], item["destinationId"], item["instanceId"])
			expectedPayload := expectedPayloads[k]

			// compare reportedAt only to the second
			reportedAtExpected, _ := expectedPayload["reportedAt"].(time.Time)
			reportedAtItem, err := time.Parse(time.RFC3339, item["reportedAt"].(string))
			assert.NoError(t, err)
			reportedAtExpected = reportedAtExpected.Truncate(time.Second)
			reportedAtItem = reportedAtItem.Truncate(time.Second)
			assert.Equal(t, reportedAtExpected, reportedAtItem)

			assert.Equal(t, expectedPayload["workspaceId"], item["workspaceId"])
			assert.Equal(t, expectedPayload["sourceId"], item["sourceId"])
			assert.Equal(t, expectedPayload["destinationId"], item["destinationId"])
			assert.Equal(t, expectedPayload["instanceId"], item["instanceId"])
			assert.Equal(t, expectedPayload["fingerprintHLL"], item["fingerprintHLL"])
		}
	}
}
