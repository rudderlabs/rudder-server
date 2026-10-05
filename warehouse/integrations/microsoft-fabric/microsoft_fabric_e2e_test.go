package microsoftfabric_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/microsoft/go-mssqldb/azuread"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/utils/misc"
	whth "github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

// fabricCredentials is read from the JSON file named by FABRIC_E2E_CREDENTIALS_FILE.
type fabricCredentials struct {
	Host              string `json:"host"`
	Port              string `json:"port"`
	Database          string `json:"database"`
	TenantID          string `json:"tenantId"`
	ClientID          string `json:"clientId"`
	ClientSecret      string `json:"clientSecret"`
	FabricWorkspaceID string `json:"fabricWorkspaceId"`
	LakehouseID       string `json:"lakehouseId"`
	OneLakeHost       string `json:"oneLakeHost"`
}

var eventTables = []string{"identifies", "users", "tracks", "product_reviewed", "pages", "screens", "aliases", "groups"}

// tableState is the per-table row count for one user, plus the users.name trait.
type tableState struct {
	counts    map[string]int
	usersName string
}

func expectedCounts(perTable int) map[string]int {
	counts := make(map[string]int, len(eventTables))
	for _, table := range eventTables {
		counts[table] = perTable
	}
	counts["users"] = 1
	return counts
}

// TestE2EGatewayToFabric sends events through the gateway and verifies they land in Fabric
// via OneLake staging and COPY INTO.
func TestE2EGatewayToFabric(t *testing.T) {
	credsFile := os.Getenv("FABRIC_E2E_CREDENTIALS_FILE")
	if credsFile == "" {
		t.Skip("FABRIC_E2E_CREDENTIALS_FILE not set")
	}
	raw, err := os.ReadFile(credsFile)
	require.NoError(t, err)
	var creds fabricCredentials
	require.NoError(t, jsonrs.Unmarshal(raw, &creds))
	if creds.Port == "" {
		creds.Port = "1433"
	}

	misc.Init()
	validations.Init()
	whutils.Init()

	destType := whutils.MicrosoftFabric

	c := testcompose.New(t, compose.FilePaths([]string{"../testdata/docker-compose.jobsdb.yml", "../testdata/docker-compose.transformer.yml"}))
	c.Start(context.Background())

	jobsDBPort := c.Port("jobsDb", 5432)
	jobsDB := whth.JobsDB(t, jobsDBPort)

	whPort, err := kithelper.GetFreePort()
	require.NoError(t, err)
	gwPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	var (
		workspaceID   = whutils.RandHex()
		sourceID      = whutils.RandHex()
		destinationID = whutils.RandHex()
		writeKey      = whutils.RandHex()
		namespace     = whth.RandSchema(destType)
		userID        = whth.GetUserId(destType)
	)
	t.Logf("namespace=%s userID=%s", namespace, userID)

	builder := backendconfigtest.NewDestinationBuilder(destType).
		WithID(destinationID).
		WithRevisionID(destinationID).
		WithConfigOption("host", creds.Host).
		WithConfigOption("port", creds.Port).
		WithConfigOption("database", creds.Database).
		WithConfigOption("tenantId", creds.TenantID).
		WithConfigOption("clientId", creds.ClientID).
		WithConfigOption("clientSecret", creds.ClientSecret).
		WithConfigOption("fabricWorkspaceId", creds.FabricWorkspaceID).
		WithConfigOption("lakehouseId", creds.LakehouseID).
		WithConfigOption("prefix", "rudder-e2e").
		WithConfigOption("namespace", namespace).
		WithConfigOption("preferAppend", false).
		WithConfigOption("syncFrequency", "30").
		WithConfigOption("allowUsersContextTraits", true).
		WithConfigOption("underscoreDivideNumbers", true)
	if creds.OneLakeHost != "" {
		builder = builder.WithConfigOption("oneLakeHost", creds.OneLakeHost)
	}
	destination := builder.Build()

	workspaceConfig := backendconfigtest.NewConfigBuilder().
		WithSource(
			backendconfigtest.NewSourceBuilder().
				WithID(sourceID).
				WithWriteKey(writeKey).
				WithWorkspaceID(workspaceID).
				WithConnection(destination).
				Build(),
		).
		WithWorkspaceID(workspaceID).
		Build()

	t.Setenv("RSERVER_GATEWAY_WEB_PORT", strconv.Itoa(gwPort))
	t.Setenv("DEST_TRANSFORM_URL", fmt.Sprintf("http://localhost:%d", c.Port("transformer", 9090)))
	t.Setenv("RSERVER_BATCH_ROUTER_MICROSOFT_FABRIC_UPLOAD_FREQ", "5s")
	t.Setenv("RSERVER_WAREHOUSE_MICROSOFT_FABRIC_SLOW_QUERY_THRESHOLD", "0s")

	bootstrapEmbedded(t, workspaceConfig, whPort, gwPort, jobsDBPort)

	db := openFabric(t, creds)
	t.Cleanup(func() { dropFabricSchema(t, db, namespace) })

	runRound := func(round, name string) tableState {
		start := time.Now().UTC()
		sendBatch(t, gwPort, writeKey, buildEvents(userID, round, name, 2))
		waitForUpload(t, jobsDB, destinationID, start)
		return readState(t, db, namespace, userID)
	}

	// Round 1: two of each event type.
	state := runRound("round1", "Alice")
	require.Equal(t, expectedCounts(2), state.counts)
	require.Equal(t, "Alice", state.usersName)

	// Round 2: new events for the same user with updated traits; users must stay one row with latest traits.
	state = runRound("round2", "Bob")
	require.Equal(t, expectedCounts(4), state.counts)
	require.Equal(t, "Bob", state.usersName)

	// Round 3: replay round 2 message IDs; with preferAppend=false every table merges on id, so nothing is added.
	state = runRound("round2", "Bob")
	require.Equal(t, expectedCounts(4), state.counts)
	require.Equal(t, "Bob", state.usersName)
}

// bootstrapEmbedded is adapted from whth.BootstrapSvc, which forces Warehouse.mode=master_and_slave
// (warehouse-only). Leaving the mode at its embedded default also starts the gateway, processor and
// batch router, so events can flow from the gateway into Fabric.
func bootstrapEmbedded(t *testing.T, workspaceConfig backendconfig.ConfigT, whPort, gwPort, jobsDBPort int) {
	bcServer := backendconfigtest.NewBuilder().WithWorkspaceConfig(workspaceConfig).Build()
	t.Cleanup(bcServer.Close)

	t.Setenv("JOBS_DB_HOST", "localhost")
	t.Setenv("JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("JOBS_DB_DB_NAME", "jobsdb")
	t.Setenv("JOBS_DB_USER", "rudder")
	t.Setenv("JOBS_DB_PASSWORD", "password")
	t.Setenv("JOBS_DB_SSL_MODE", "disable")
	t.Setenv("RSERVER_WAREHOUSE_WEB_PORT", strconv.Itoa(whPort))
	t.Setenv("WORKSPACE_TOKEN", "token")
	t.Setenv("CONFIG_BACKEND_URL", bcServer.URL)
	t.Setenv("GO_ENV", "production")
	t.Setenv("LOG_LEVEL", "INFO")
	t.Setenv("CONFIG_PATH", "../../../config/config.yaml")
	t.Setenv("RSERVER_WAREHOUSE_WAREHOUSE_SYNC_FREQ_IGNORE", "true")
	t.Setenv("RSERVER_WAREHOUSE_UPLOAD_FREQ_IN_S", "1")
	t.Setenv("RSERVER_WAREHOUSE_MAIN_LOOP_SLEEP", "1s")
	t.Setenv("RSERVER_WAREHOUSE_ENABLE_JITTER_FOR_SYNCS", "false")
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_FROM_FILE", "false")
	t.Setenv("RSERVER_ADMIN_SERVER_ENABLED", "false")
	t.Setenv("RUDDER_GRACEFUL_SHUTDOWN_TIMEOUT_EXIT", "false")
	t.Setenv("RSERVER_ENABLE_STATS", "false")
	t.Setenv("RUDDER_TMPDIR", t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())
	svcDone := make(chan struct{})
	go func() {
		r := runner.New(runner.ReleaseInfo{EnterpriseToken: "TOKEN"})
		_ = r.Run(ctx, cancel, []string{"fabric-e2e"})
		close(svcDone)
	}()
	t.Cleanup(func() { <-svcDone })
	t.Cleanup(cancel)

	health.WaitUntilReady(ctx, t, fmt.Sprintf("http://localhost:%d/health", gwPort), time.Minute, 250*time.Millisecond, "gateway")
}

func buildEvents(userID, round, name string, n int) []map[string]any {
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	var events []map[string]any
	for i := 0; i < n; i++ {
		event := func(typ string, fields map[string]any) map[string]any {
			e := map[string]any{
				"type": typ, "userId": userID, "anonymousId": "anon-" + userID,
				"messageId":         fmt.Sprintf("%s-%s-%s-%d", userID, round, typ, i),
				"originalTimestamp": ts, "sentAt": ts, "timestamp": ts,
			}
			maps.Copy(e, fields)
			return e
		}
		events = append(events,
			event("identify", map[string]any{
				"traits":  map[string]any{"name": name, "email": strings.ToLower(name) + "@example.com", "logins": i + 1},
				"context": map[string]any{"traits": map[string]any{"name": name, "plan": "pro"}},
			}),
			event("track", map[string]any{
				"event":      "Product Reviewed",
				"properties": map[string]any{"review_id": fmt.Sprintf("%s-%d", round, i), "product_id": "p-1", "rating": 4.5, "is_verified": true},
			}),
			event("page", map[string]any{"name": "Home", "properties": map[string]any{"title": "Home", "url": "https://example.com"}}),
			event("screen", map[string]any{"name": "Main", "properties": map[string]any{"title": "Main"}}),
			event("alias", map[string]any{"previousId": "prev-" + userID}),
			event("group", map[string]any{"groupId": "g-1", "traits": map[string]any{"name": "Acme", "employees": 10, "industry": "Tech"}}),
		)
	}
	return events
}

func sendBatch(t *testing.T, gwPort int, writeKey string, events []map[string]any) {
	t.Helper()
	body, err := jsonrs.Marshal(map[string]any{"batch": events})
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:%d/v1/batch", gwPort), bytes.NewReader(body))
	require.NoError(t, err)
	req.SetBasicAuth(writeKey, "")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode, "gateway rejected batch")
}

// waitForUpload blocks until an upload created after start reaches exported_data, failing on abort.
func waitForUpload(t *testing.T, jobsDB *sql.DB, destinationID string, start time.Time) {
	t.Helper()
	var lastStatus, lastErr string
	require.Eventuallyf(t, func() bool {
		var status string
		var errJSON sql.NullString
		err := jobsDB.QueryRow(`SELECT status, error::text FROM wh_uploads
			WHERE destination_id = $1 AND created_at > $2 ORDER BY id DESC LIMIT 1`, destinationID, start).
			Scan(&status, &errJSON)
		if err != nil {
			return false
		}
		if status != lastStatus || errJSON.String != lastErr {
			t.Logf("upload status=%s error=%s", status, errJSON.String)
			lastStatus, lastErr = status, errJSON.String
		}
		if status == "aborted" {
			t.Fatalf("upload aborted: %s", errJSON.String)
		}
		return status == "exported_data"
	}, 15*time.Minute, 2*time.Second, "upload did not reach exported_data (last status %q)", lastStatus)
}

func openFabric(t *testing.T, creds fabricCredentials) *sql.DB {
	t.Helper()
	q := url.Values{}
	q.Set("database", creds.Database)
	q.Set("fedauth", azuread.ActiveDirectoryServicePrincipal)
	q.Set("encrypt", "true")
	dsn := (&url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(creds.ClientID+"@"+creds.TenantID, creds.ClientSecret),
		Host:     net.JoinHostPort(creds.Host, creds.Port),
		RawQuery: q.Encode(),
	}).String()
	connector, err := azuread.NewConnector(dsn)
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	require.NoError(t, db.Ping())
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// readState fetches every table's row count for userID, plus users.name, in a single round-trip.
func readState(t *testing.T, db *sql.DB, namespace, userID string) tableState {
	t.Helper()
	parts := make([]string, 0, len(eventTables))
	for _, table := range eventTables {
		col, name := "user_id", "NULL"
		if table == "users" {
			col, name = "id", "MAX([name])"
		}
		parts = append(parts, fmt.Sprintf(`SELECT '%s', COUNT(*), %s FROM [%s].[%s] WHERE [%s] = @p1`, table, name, namespace, table, col))
	}
	rows, err := db.Query(strings.Join(parts, " UNION ALL "), userID)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	state := tableState{counts: map[string]int{}}
	for rows.Next() {
		var table string
		var count int
		var name sql.NullString
		require.NoError(t, rows.Scan(&table, &count, &name))
		state.counts[table] = count
		if table == "users" {
			state.usersName = name.String
		}
	}
	require.NoError(t, rows.Err())
	t.Logf("fabric state: %+v", state)
	return state
}

func dropFabricSchema(t *testing.T, db *sql.DB, namespace string) {
	t.Helper()
	if os.Getenv("FABRIC_E2E_KEEP_SCHEMA") == "true" {
		t.Logf("keeping schema %s", namespace)
		return
	}
	rows, err := db.Query(`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = @p1`, namespace)
	if err != nil {
		t.Logf("listing tables for cleanup: %v", err)
		return
	}
	var stmts []string
	for rows.Next() {
		var name string
		if rows.Scan(&name) == nil {
			stmts = append(stmts, fmt.Sprintf(`DROP TABLE [%s].[%s];`, namespace, name))
		}
	}
	_ = rows.Close()
	stmts = append(stmts, fmt.Sprintf(`DROP SCHEMA [%s];`, namespace))
	if _, err := db.Exec(strings.Join(stmts, " ")); err != nil {
		t.Logf("dropping schema %s: %v", namespace, err)
	}
}
