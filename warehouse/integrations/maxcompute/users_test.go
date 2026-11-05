package maxcompute

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type usersUploader struct {
	loadUploader
	users  model.TableSchema
	err    error
	tables []string
}

func (u *usersUploader) GetTableSchemaInWarehouse(string) model.TableSchema { return u.users }
func (u *usersUploader) GetLoadFilesMetadata(_ context.Context, options warehouseutils.GetLoadFilesOptions) ([]warehouseutils.LoadFile, error) {
	u.tables = append(u.tables, options.Table)
	return u.files, u.err
}

func testUsersManager(t *testing.T, source, granularity string) (*MaxCompute, *usersUploader, *[]string) {
	t.Helper()
	m := testManager()
	expression := fmt.Sprintf(`[{"leafExprDesc":{"reference":{"name":"%s"}}},{"leafExprDesc":{"constant":"%s"}},{"functionCall":{"name":"trunc_time"}}]`, source, granularity)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		schema, _ := jsonrs.Marshal(map[string]any{"columns": []any{map[string]any{"name": "id", "type": "string"}, map[string]any{"name": source, "type": "timestamp"}}, "PartitionKeys": []any{map[string]any{"name": "pt", "type": "string", "hasGenerateExpression": true, "generateExpression": expression}}})
		_, _ = fmt.Fprintf(w, "<Table><Schema>%s</Schema></Table>", strings.ReplaceAll(string(schema), "&", "&amp;"))
	}))
	t.Cleanup(server.Close)
	m.odps = odps.NewOdps(account.NewAliyunAccount("id", "secret"), server.URL)
	m.odps.SetDefaultProjectName("project")
	u := &usersUploader{
		files: []warehouseutils.LoadFile{{Location: "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/11111111-1111-1111-1111-111111111111-users/one.json.gz", Metadata: []byte(`{"content_length":10}`)}},
		users: model.TableSchema{"id": "string", "received_at": "datetime", "loaded_at": "datetime", "uuid_ts": "datetime", "email": "string", "name": "string"},
	}
	m.uploader = u
	statements := &[]string{}
	m.exec = func(_ context.Context, sql string) error { *statements = append(*statements, sql); return nil }
	m.catalog = func(context.Context) (model.Schema, error) {
		return model.Schema{warehouseutils.UsersTable: u.users}, nil
	}
	return m, u, statements
}

func TestUsersCarryForwardUnpartitionedStage(t *testing.T) {
	for _, tc := range []struct{ source, granularity string }{
		{"uuid_ts", "day"}, {"uuid_ts", "hour"}, {"received_at", "day"},
	} {
		t.Run(tc.source+"_"+tc.granularity, func(t *testing.T) {
			m, u, statements := testUsersManager(t, tc.source, tc.granularity)
			m.warehouse.Destination.Config["skipViews"] = false
			require.NoError(t, m.loadUsersWithCarryForward(context.Background()))
			require.Equal(t, []string{warehouseutils.UsersTable}, u.tables)
			require.Len(t, *statements, 5)
			stage := strings.TrimPrefix(strings.SplitN((*statements)[0], " (", 2)[0], "CREATE TABLE ")
			require.Contains(t, (*statements)[0], "`loaded_at` TIMESTAMP")
			require.Contains(t, (*statements)[0], "TBLPROPERTIES ('table.format.version'='2')")
			require.NotContains(t, (*statements)[0], "AUTO PARTITIONED")
			require.Contains(t, (*statements)[1], "LOAD INTO TABLE "+stage+" FROM LOCATION")
			require.Contains(t, (*statements)[2], "CREATE OR REPLACE VIEW `project`.`users_view`")
			insert := (*statements)[3]
			require.Contains(t, insert, "INSERT INTO `project`.`users`")
			require.Contains(t, insert, "FROM "+stage+" WHERE `id` IS NOT NULL")
			require.NotContains(t, insert, "FROM "+stage+" WHERE `pt`")
			require.Contains(t, insert, "FROM `project`.`users` WHERE `pt` >= '"+time.Now().UTC().AddDate(0, 0, -60).Format("2006-01-02")+"' AND `pt` < '"+time.Now().UTC().AddDate(0, 0, 1).Format("2006-01-02")+"' AND `id` IN (SELECT `id` FROM "+stage+" WHERE `id` IS NOT NULL)")
			require.NotContains(t, insert, "FROM `project`.`identifies`")
			require.Contains(t, insert, " UNION ALL ")
			require.Contains(t, insert, "SELECT DISTINCT")
			require.Contains(t, insert, "FIRST_VALUE(`email`, true) OVER (PARTITION BY `id` ORDER BY `received_at` DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)")
			require.Contains(t, insert, "FIRST_VALUE(`received_at`, true)")
			require.Contains(t, insert, "FIRST_VALUE(CASE WHEN `__current_upload` = 1 THEN `uuid_ts` END, true)")
			require.Contains(t, insert, "FIRST_VALUE(CASE WHEN `__current_upload` = 1 THEN `loaded_at` END, true)")
			require.Equal(t, "DROP TABLE IF EXISTS "+stage, (*statements)[4])
		})
	}
}

func TestUsersCarryForwardPreflightAndCleanup(t *testing.T) {
	m, u, statements := testUsersManager(t, "uuid_ts", "day")
	u.files = nil
	require.NoError(t, m.loadUsersWithCarryForward(context.Background()))
	require.Empty(t, *statements)
	u.files = []warehouseutils.LoadFile{{Location: "https://evil.example/bad.gz", Metadata: []byte(`{"content_length":10}`)}}
	require.ErrorContains(t, m.loadUsersWithCarryForward(context.Background()), "load file")
	require.Empty(t, *statements)
	u.err = errors.New("router failed with secret")
	require.ErrorContains(t, m.loadUsersWithCarryForward(context.Background()), "load files failed")
	require.NotContains(t, m.loadUsersWithCarryForward(context.Background()).Error(), "secret")
	require.Empty(t, *statements)
	u.err = nil
	u.files = []warehouseutils.LoadFile{{Location: "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/11111111-1111-1111-1111-111111111111-users/one.json.gz", Metadata: []byte(`{"content_length":10}`)}}
	m.exec = func(_ context.Context, sql string) error {
		*statements = append(*statements, sql)
		if strings.HasPrefix(sql, "INSERT INTO") {
			return errors.New("insert failed")
		}
		return nil
	}
	require.ErrorContains(t, m.loadUsersWithCarryForward(context.Background()), "insert failed")
	require.Contains(t, (*statements)[len(*statements)-1], "DROP TABLE IF EXISTS")
}
