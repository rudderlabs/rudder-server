package flusher_test

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/segmentio/go-hll"
	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/aggregator"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/testhelper/webhook"
)

func migrateDatabase(dbConn string, conf *config.Config) error {
	dbHandle, err := sql.Open("postgres", dbConn)
	if err != nil {
		return err
	}
	dbHandle.SetMaxOpenConns(1)

	m := &migrator.Migrator{
		Handle:                     dbHandle,
		MigrationsTable:            "tracked_users_reports_migrations",
		ShouldForceSetLowerVersion: conf.GetBool("SQLMigrator.forceSetLowerVersion", true),
	}
	err = m.Migrate("tracked_users")
	if err != nil {
		return fmt.Errorf("migrating `tracked_users_reports` table: %w", err)
	}
	return nil
}

func hllSettings() hll.Settings {
	return hll.Settings{
		Log2m:             14,
		Regwidth:          5,
		ExplicitThreshold: hll.AutoExplicitThreshold,
		SparseEnabled:     true,
	}
}

func addDataToHLL(hllData *hll.Hll, min, max, count int) {
	for i := 0; i < count; i++ {
		userId := fmt.Sprintf("user_%d", rand.Intn(max-min+1)+min)
		hllData.AddRaw(murmur3.Sum64WithSeed([]byte(userId), 123))
	}
}

// generate a report where there can be overlap of users, anon-users, anon-identified-users
func generateReport(reportedAt time.Time, workspaceId, sourceId string, numUsers, numAnons, numIdentAnons int) *aggregator.TrackedUsersReport {
	userIdHLL, _ := hll.NewHll(hllSettings())
	anonymousIdHLL, _ := hll.NewHll(hllSettings())
	identifiedAnonymousIdHLL, _ := hll.NewHll(hllSettings())

	addDataToHLL(&userIdHLL, 1, 10000, numUsers)
	addDataToHLL(&anonymousIdHLL, 1, 10000, numAnons)
	addDataToHLL(&identifiedAnonymousIdHLL, 1, 10000, numIdentAnons)

	return &aggregator.TrackedUsersReport{
		ReportedAt:               reportedAt,
		WorkspaceID:              workspaceId,
		SourceID:                 sourceId,
		InstanceID:               "instance1",
		UserIDHLL:                &userIdHLL,
		AnonymousIDHLL:           &anonymousIdHLL,
		IdentifiedAnonymousIDHLL: &identifiedAnonymousIdHLL,
	}
}

func addReportsToDB(ctx context.Context, db *sql.DB, reports []*aggregator.TrackedUsersReport) error {
	for _, report := range reports {
		_, err := db.ExecContext(ctx, `
			INSERT INTO tracked_users_reports (
				reported_at,
				workspace_id,
				source_id,
				instance_id,
				userid_hll,
				anonymousid_hll,
				identified_anonymousid_hll
			) VALUES ($1, $2, $3, $4, $5, $6, $7)
		`,
			report.ReportedAt,
			report.WorkspaceID,
			report.SourceID,
			report.InstanceID,
			hllToString(report.UserIDHLL),
			hllToString(report.AnonymousIDHLL),
			hllToString(report.IdentifiedAnonymousIDHLL),
		)
		if err != nil {
			return fmt.Errorf("inserting report: %v", err)
		}
	}

	return nil
}

func hllToString(hll *hll.Hll) string {
	if hll != nil {
		return hex.EncodeToString(hll.ToBytes())
	}
	return ""
}

// 1. Add 5 reports to the DB, 4 reports which are 10 minutes old and 1 report which is 1 minute old
// 2. Validate 5 reports were added
// 3. Start the runner
// 4. Wait till all the reports which were added 10 minutes ago to be flushed
// 5. Stop the runner
// 6. Validate we get 2 requests, since we have batch size of 2
// 7. Validate the payloads sent to Reporting Service
func TestTrackedUsersFlush(t *testing.T) {
	ctx := context.Background()

	currentUTC := time.Now().UTC()
	oneMinAgo := currentUTC.Add(-1 * time.Minute)
	tenMinAgo := currentUTC.Add(-2 * time.Minute)

	// reports generated with tenMinAgo should be flushed
	reports := []*aggregator.TrackedUsersReport{
		generateReport(tenMinAgo, "workspace1", "source1", 10000, 10000, 10000),
		generateReport(tenMinAgo, "workspace1", "source1", 500, 500, 1000),
		generateReport(tenMinAgo, "workspace1", "source2", 1000, 1000, 1000),
		generateReport(tenMinAgo, "workspace2", "source1", 1000, 1000, 1000),

		generateReport(oneMinAgo, "workspace1", "source1", 1000, 1000, 1000),
	}

	// set up db
	pool, _ := dockertest.NewPool("")
	pgContainer, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	err = migrateDatabase(pgContainer.DBDsn, config.Default)
	require.NoError(t, err)
	config.Set("TrackedUsers.enabled", true)
	config.Set("DB.host", pgContainer.Host)
	config.Set("DB.port", pgContainer.Port)
	config.Set("DB.user", pgContainer.User)
	config.Set("DB.name", pgContainer.Database)
	config.Set("DB.password", pgContainer.Password)
	config.Set("Reporting.flusher.batchSizeToReporting", 2)

	err = addReportsToDB(ctx, pgContainer.DB, reports)
	assert.NoError(t, err)

	// validate 5 reports were added
	var count int
	err = pgContainer.DB.QueryRow("SELECT COUNT(*) FROM tracked_users_reports").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 5, count)

	// setup test webhook with reporting url
	testWebhook := webhook.NewRecorder()
	t.Cleanup(testWebhook.Close)
	webhookURL := testWebhook.Server.URL
	config.Set("REPORTING_URL", webhookURL)

	// create runner and start it
	runner, err := flusher.CreateRunner(ctx, "tracked_users_reports", logger.NOP, stats.NOP, config.Default, "test")
	require.NoError(t, err)

	runnerDone := make(chan struct{})
	go func() {
		defer close(runnerDone)
		runner.Run()
	}()

	// Wait till all the reports which were added tenMinAgo to be flushed
	require.Eventually(t, func() bool {
		var count int
		_ = pgContainer.DB.QueryRow("SELECT COUNT(*) FROM tracked_users_reports").Scan(&count)
		return count == 1
	}, time.Second*30, time.Millisecond*1000)

	runner.Stop()

	// validate we get 2 requests, we have batch size of 2
	reqCount := testWebhook.RequestsCount()
	assert.Equal(t, 2, reqCount)

	// validate the payloads sent to Reporting Service
	reqBodies := testWebhook.Requests()
	for _, req := range reqBodies {
		var items []map[string]interface{}

		body, _ := io.ReadAll(req.Body)
		err := json.Unmarshal(body, &items)
		if err != nil {
			log.Fatalf("Error parsing body: %v", err)
		}

		reports[0].UserIDHLL.Union(*reports[1].UserIDHLL)
		reports[0].AnonymousIDHLL.Union(*reports[1].AnonymousIDHLL)
		reports[0].IdentifiedAnonymousIDHLL.Union(*reports[1].IdentifiedAnonymousIDHLL)

		expectedPayloads := map[string]map[string]interface{}{
			"workspace1-source1-instance1": {
				"reportedAt":               tenMinAgo,
				"workspaceId":              "workspace1",
				"sourceId":                 "source1",
				"instanceId":               "instance1",
				"userIdHLL":                hllToString(reports[0].UserIDHLL),
				"anonymousIdHLL":           hllToString(reports[0].AnonymousIDHLL),
				"identifiedAnonymousIdHLL": hllToString(reports[0].IdentifiedAnonymousIDHLL),
			},
			"workspace1-source2-instance1": {
				"reportedAt":               tenMinAgo,
				"workspaceId":              "workspace1",
				"sourceId":                 "source2",
				"instanceId":               "instance1",
				"userIdHLL":                hllToString(reports[2].UserIDHLL),
				"anonymousIdHLL":           hllToString(reports[2].AnonymousIDHLL),
				"identifiedAnonymousIdHLL": hllToString(reports[2].IdentifiedAnonymousIDHLL),
			},
			"workspace2-source1-instance1": {
				"reportedAt":               tenMinAgo,
				"workspaceId":              "workspace2",
				"sourceId":                 "source1",
				"instanceId":               "instance1",
				"userIdHLL":                hllToString(reports[3].UserIDHLL),
				"anonymousIdHLL":           hllToString(reports[3].AnonymousIDHLL),
				"identifiedAnonymousIdHLL": hllToString(reports[3].IdentifiedAnonymousIDHLL),
			},
		}

		for _, item := range items {
			k := fmt.Sprintf("%s-%s-%s", item["workspaceId"], item["sourceId"], item["instanceId"])
			expectedPayload := expectedPayloads[k]

			// compare reportedAt only till second
			reportedAtExpected, _ := expectedPayload["reportedAt"].(time.Time)
			reportedAtItem, err := time.Parse(time.RFC3339, item["reportedAt"].(string))
			assert.NoError(t, err)
			reportedAtExpected = reportedAtExpected.Truncate(time.Second)
			reportedAtItem = reportedAtItem.Truncate(time.Second)
			assert.Equal(t, reportedAtExpected, reportedAtItem)

			assert.Equal(t, expectedPayload["workspaceId"], item["workspaceId"])
			assert.Equal(t, expectedPayload["sourceId"], item["sourceId"])
			assert.Equal(t, expectedPayload["instanceId"], item["instanceId"])
			assert.Equal(t, expectedPayload["userIdHLL"], item["userIdHLL"])
			assert.Equal(t, expectedPayload["anonymousIdHLL"], item["anonymousIdHLL"])
			assert.Equal(t, expectedPayload["identifiedAnonymousIdHLL"], item["identifiedAnonymousIdHLL"])
		}
	}
}
