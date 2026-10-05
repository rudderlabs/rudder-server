package microsoftfabric_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
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

	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
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
	transformerURL := fmt.Sprintf("http://localhost:%d", c.Port("transformer", 9090))
	jobsDB := whth.JobsDB(t, jobsDBPort)

	httpPort, err := kithelper.GetFreePort()
	require.NoError(t, err)
	gwPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	var (
		workspaceID   = whutils.RandHex()
		sourceID      = whutils.RandHex()
		destinationID = whutils.RandHex()
		writeKey      = whutils.RandHex()
		namespace     = "fabric_e2e_" + strings.ToLower(whutils.RandHex()[:10])
		userID        = "fabric_e2e_user_" + whutils.RandHex()[:8]
	)
	t.Logf("namespace=%s userID=%s", namespace, userID)

	destConfig := map[string]any{
		"host":                    creds.Host,
		"port":                    creds.Port,
		"database":                creds.Database,
		"tenantId":                creds.TenantID,
		"clientId":                creds.ClientID,
		"clientSecret":            creds.ClientSecret,
		"fabricWorkspaceId":       creds.FabricWorkspaceID,
		"lakehouseId":             creds.LakehouseID,
		"prefix":                  "rudder-e2e",
		"namespace":               namespace,
		"syncFrequency":           "30",
		"allowUsersContextTraits": true,
		"underscoreDivideNumbers": true,
	}
	if creds.OneLakeHost != "" {
		destConfig["oneLakeHost"] = creds.OneLakeHost
	}
	builder := backendconfigtest.NewDestinationBuilder(destType).
		WithID(destinationID).
		WithRevisionID(destinationID)
	for k, v := range destConfig {
		builder = builder.WithConfigOption(k, v)
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
	t.Setenv("DEST_TRANSFORM_URL", transformerURL)
	t.Setenv("RSERVER_BATCH_ROUTER_MICROSOFT_FABRIC_UPLOAD_FREQ", "5s")
	t.Setenv("RSERVER_WAREHOUSE_MICROSOFT_FABRIC_SLOW_QUERY_THRESHOLD", "0s")

	whth.BootstrapSvc(t, workspaceConfig, httpPort, jobsDBPort)

	db := openFabric(t, creds)
	t.Cleanup(func() { dropFabricSchema(t, db, namespace) })

	// Round 1: two of each event type.
	start := time.Now().UTC()
	sendBatch(t, gwPort, writeKey, buildEvents(userID, "round1", "Alice", 2))
	waitForUpload(t, jobsDB, destinationID, start)

	counts := tableCounts(t, db, namespace, userID)
	t.Logf("round 1 counts: %v", counts)
	require.Equal(t, map[string]int{
		"identifies": 2, "users": 1, "tracks": 2, "product_reviewed": 2,
		"pages": 2, "screens": 2, "aliases": 2, "groups": 2,
	}, counts)
	require.Equal(t, "Alice", usersName(t, db, namespace, userID))

	// Round 2: new events for the same user with updated traits; users must stay one row with latest traits.
	start = time.Now().UTC()
	sendBatch(t, gwPort, writeKey, buildEvents(userID, "round2", "Bob", 2))
	waitForUpload(t, jobsDB, destinationID, start)

	counts = tableCounts(t, db, namespace, userID)
	t.Logf("round 2 counts: %v", counts)
	require.Equal(t, map[string]int{
		"identifies": 4, "users": 1, "tracks": 4, "product_reviewed": 4,
		"pages": 4, "screens": 4, "aliases": 4, "groups": 4,
	}, counts)
	require.Equal(t, "Bob", usersName(t, db, namespace, userID))

	// Round 3: replay round 2 messageIds; non-append tables must dedupe on id.
	start = time.Now().UTC()
	sendBatch(t, gwPort, writeKey, buildEvents(userID, "round2", "Bob", 2))
	waitForUpload(t, jobsDB, destinationID, start)
	t.Logf("round 3 (replayed message IDs) counts: %v", tableCounts(t, db, namespace, userID))
}

func buildEvents(userID, round, name string, n int) []map[string]any {
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	var events []map[string]any
	for i := 0; i < n; i++ {
		id := func(kind string) string { return fmt.Sprintf("%s-%s-%s-%d", userID, round, kind, i) }
		common := func(kind string) map[string]any {
			return map[string]any{
				"userId": userID, "messageId": id(kind), "anonymousId": "anon-" + userID,
				"originalTimestamp": ts, "sentAt": ts, "timestamp": ts,
			}
		}
		identify := common("identify")
		identify["type"] = "identify"
		identify["traits"] = map[string]any{"name": name, "email": strings.ToLower(name) + "@example.com", "logins": i + 1}
		identify["context"] = map[string]any{"traits": map[string]any{"name": name, "plan": "pro"}}

		track := common("track")
		track["type"] = "track"
		track["event"] = "Product Reviewed"
		track["properties"] = map[string]any{"review_id": id("review"), "product_id": "p-1", "rating": 4.5, "is_verified": true}

		page := common("page")
		page["type"] = "page"
		page["name"] = "Home"
		page["properties"] = map[string]any{"title": "Home", "url": "https://example.com"}

		screen := common("screen")
		screen["type"] = "screen"
		screen["name"] = "Main"
		screen["properties"] = map[string]any{"title": "Main"}

		alias := common("alias")
		alias["type"] = "alias"
		alias["previousId"] = "prev-" + userID

		group := common("group")
		group["type"] = "group"
		group["groupId"] = "g-1"
		group["traits"] = map[string]any{"name": "Acme", "employees": 10, "industry": "Tech"}

		events = append(events, identify, track, page, screen, alias, group)
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
	require.Eventually(t, func() bool {
		resp, err := http.DefaultClient.Do(req.Clone(context.Background()))
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		req.Body, _ = req.GetBody()
		return resp.StatusCode == http.StatusOK
	}, time.Minute, time.Second, "gateway did not accept batch")
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

func tableCounts(t *testing.T, db *sql.DB, namespace, userID string) map[string]int {
	t.Helper()
	counts := map[string]int{}
	for _, table := range []string{"identifies", "users", "tracks", "product_reviewed", "pages", "screens", "aliases", "groups"} {
		col := "user_id"
		if table == "users" {
			col = "id"
		}
		var n int
		err := db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM [%s].[%s] WHERE [%s] = @p1`, namespace, table, col), userID).Scan(&n)
		require.NoErrorf(t, err, "counting %s", table)
		counts[table] = n
	}
	return counts
}

func usersName(t *testing.T, db *sql.DB, namespace, userID string) string {
	t.Helper()
	var name sql.NullString
	require.NoError(t, db.QueryRow(fmt.Sprintf(`SELECT TOP 1 [name] FROM [%s].[users] WHERE [id] = @p1`, namespace), userID).Scan(&name))
	return name.String
}

func dropFabricSchema(t *testing.T, db *sql.DB, namespace string) {
	if os.Getenv("FABRIC_E2E_KEEP_SCHEMA") == "true" {
		t.Logf("keeping schema %s", namespace)
		return
	}
	rows, err := db.Query(`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = @p1`, namespace)
	if err != nil {
		t.Logf("listing tables for cleanup: %v", err)
		return
	}
	var tables []string
	for rows.Next() {
		var name string
		if rows.Scan(&name) == nil {
			tables = append(tables, name)
		}
	}
	_ = rows.Close()
	for _, table := range tables {
		if _, err := db.Exec(fmt.Sprintf(`DROP TABLE [%s].[%s]`, namespace, table)); err != nil {
			t.Logf("dropping %s: %v", table, err)
		}
	}
	if _, err := db.Exec(fmt.Sprintf(`DROP SCHEMA [%s]`, namespace)); err != nil {
		t.Logf("dropping schema: %v", err)
	}
}
