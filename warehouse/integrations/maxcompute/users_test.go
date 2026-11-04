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

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type usersUploader struct {
	loadUploader
	users      model.TableSchema
	identifies model.TableSchema
	err        error
	tables     []string
}

func (u *usersUploader) GetTableSchemaInWarehouse(table string) model.TableSchema {
	if table == warehouseutils.UsersTable {
		return u.users
	}
	return u.identifies
}

func (u *usersUploader) GetLoadFilesMetadata(_ context.Context, options warehouseutils.GetLoadFilesOptions) ([]warehouseutils.LoadFile, error) {
	u.tables = append(u.tables, options.Table)
	return u.files, u.err
}

func testUsersManager(t *testing.T) (*MaxCompute, *usersUploader, *[]string) {
	t.Helper()
	m := testManager()
	// Serve real SDK catalog and partition requests; the generated expression
	// is table metadata, not a mutable upload/destination setting.
	expression := `[{"leafExprDesc":{"reference":{"name":"received_at"}}},{"leafExprDesc":{"constant":"day"}},{"functionCall":{"name":"trunc_time"}}]`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := r.URL.Query()["partitions"]; ok {
			_, _ = w.Write([]byte(`<Partitions/>`))
			return
		}
		schema, _ := jsonrs.Marshal(map[string]any{"columns": []any{map[string]any{"name": "id", "type": "string"}, map[string]any{"name": "received_at", "type": "datetime"}}, "PartitionKeys": []any{map[string]any{"name": "pt", "type": "string", "hasGenerateExpression": true, "generateExpression": expression}}})
		_, _ = fmt.Fprintf(w, "<Table><Schema>%s</Schema></Table>", strings.ReplaceAll(strings.ReplaceAll(string(schema), "&", "&amp;"), "<", "&lt;"))
	}))
	t.Cleanup(server.Close)
	m.odps = odps.NewOdps(account.NewAliyunAccount("id", "secret"), server.URL)
	m.odps.SetDefaultProjectName("project")
	u := &usersUploader{
		files:      []warehouseutils.LoadFile{{Location: "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/one.gz"}, {Location: "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/two.gz"}},
		users:      model.TableSchema{"id": "string", "received_at": "datetime", "loaded_at": "datetime", "uuid_ts": "datetime", "email": "string", "name": "string"},
		identifies: model.TableSchema{"user_id": "string", "received_at": "datetime", "email": "string"},
	}
	m.uploader = u
	statements := &[]string{}
	m.exec = func(_ context.Context, sql string) error { *statements = append(*statements, sql); return nil }
	now := time.Now().UTC()
	m.partitions = func(_ context.Context, table string) ([]string, error) {
		if strings.Contains(table, "users_upload") {
			return []string{"pt=2024-02-01", "pt=2024-02-03"}, nil
		}
		return []string{"pt=" + now.Format("2006-01-02"), "pt=2000-01-01"}, nil
	}
	oldSize := metadataSize
	metadataSize = func(context.Context, *config.Config, map[string]any, string) (int64, error) { return 10, nil }
	t.Cleanup(func() { metadataSize = oldSize })
	return m, u, statements
}

func TestUsersCarryForwardOnlyCurrentUploadAndExactPartitions(t *testing.T) {
	m, u, statements := testUsersManager(t)
	require.NoError(t, m.loadUsersWithCarryForward(context.Background()))
	require.Equal(t, []string{warehouseutils.UsersTable}, u.tables)
	require.Len(t, *statements, 5)
	require.Contains(t, (*statements)[0], "CREATE TABLE `project`.")
	require.Contains(t, (*statements)[0], "`loaded_at` TIMESTAMP")
	require.Contains(t, (*statements)[0], "AUTO PARTITIONED BY (TRUNC_TIME(`received_at`, 'day') AS `pt`) TBLPROPERTIES ('table.format.version'='2')")
	require.Contains(t, (*statements)[1], "LOAD DATA INPATH 'oss://my-bucket/one.gz' INTO TABLE `project`.")
	require.Contains(t, (*statements)[2], "LOAD DATA INPATH 'oss://my-bucket/two.gz' INTO TABLE `project`.")
	insert := (*statements)[3]
	require.Contains(t, insert, "INSERT INTO `project`.`users`")
	require.Contains(t, insert, "FROM `project`.`users` WHERE `pt` IN ('"+time.Now().UTC().Format("2006-01-02")+"') AND `id` IN (")
	require.Contains(t, insert, "FROM `project`.`identifies` WHERE `pt` IN ('"+time.Now().UTC().Format("2006-01-02")+"') AND `user_id` IN (")
	require.NotContains(t, insert, "2000-01-01")
	stageName := strings.TrimPrefix(strings.SplitN((*statements)[0], " (", 2)[0], "CREATE TABLE ")
	require.Contains(t, insert, "FROM "+stageName+" WHERE `pt` IN ('2024-02-01', '2024-02-03') AND `id` IS NOT NULL")
	require.Equal(t, 3, strings.Count(insert, "FROM "+stageName+" WHERE `pt` IN ('2024-02-01', '2024-02-03') AND `id` IS NOT NULL")) // rows and both ID subqueries
	require.NotContains(t, insert, "FROM "+stageName+" WHERE `id` IS NOT NULL")
	require.NotContains(t, insert, "`pt` IN ('"+time.Now().UTC().Format("2006-01-02")+"') AND `id` IS NOT NULL")
	require.Equal(t, 1, strings.Count(insert, "`id` IN (SELECT DISTINCT `id` FROM `project`.")) // historical users only
	require.Contains(t, insert, "`user_id` IN (SELECT DISTINCT `id` FROM `project`.")
	require.Contains(t, insert, "FIRST_VALUE(`email`, true) OVER (PARTITION BY `id` ORDER BY `received_at` DESC")
	require.Contains(t, insert, "FIRST_VALUE(CASE WHEN `__current_upload` = 1 THEN `loaded_at` END, true)")
	require.Contains(t, insert, "FIRST_VALUE(CASE WHEN `__current_upload` = 1 THEN `uuid_ts` END, true)")
	require.Contains(t, insert, "FIRST_VALUE(CASE WHEN `__current_upload` = 1 THEN `received_at` END, true)")
	require.NotContains(t, insert, "GETDATE")
	require.Contains(t, (*statements)[4], "DROP TABLE IF EXISTS `project`.")
}

func TestUsersCarryForwardWithoutHistoryStillInsertsCurrentUpload(t *testing.T) {
	m, _, statements := testUsersManager(t)
	m.partitions = func(_ context.Context, table string) ([]string, error) {
		if strings.Contains(table, "users_upload") {
			return []string{"pt=2024-02-01"}, nil
		}
		return nil, nil
	}
	require.NoError(t, m.loadUsersWithCarryForward(context.Background()))
	require.Len(t, *statements, 5)
	insert := (*statements)[3]
	require.Contains(t, insert, "FROM `project`.")
	require.NotContains(t, insert, "FROM `project`.`users` WHERE")
	require.NotContains(t, insert, "FROM `project`.`identifies` WHERE")
}

func TestUsersCarryForwardEmptyStageDoesNotInsert(t *testing.T) {
	m, _, statements := testUsersManager(t)
	m.partitions = func(_ context.Context, table string) ([]string, error) {
		if strings.Contains(table, "users_upload") {
			return nil, nil
		}
		return nil, errors.New("history must not be listed")
	}
	require.NoError(t, m.loadUsersWithCarryForward(context.Background()))
	require.Len(t, *statements, 4) // CREATE, two LOADs, DROP
	require.NotContains(t, strings.Join(*statements, "\n"), "INSERT INTO")
}

func TestUsersCarryForwardInvalidStagePartitionFailsClosed(t *testing.T) {
	m, _, statements := testUsersManager(t)
	m.partitions = func(_ context.Context, table string) ([]string, error) {
		if strings.Contains(table, "users_upload") {
			return []string{"pt=2024-02-01' OR 1=1"}, nil
		}
		return nil, errors.New("history must not be listed")
	}
	require.ErrorContains(t, m.loadUsersWithCarryForward(context.Background()), "partition")
	require.Len(t, *statements, 4) // DROP even when listing cannot be trusted
	require.NotContains(t, strings.Join(*statements, "\n"), "INSERT INTO")
}

func TestUsersStagePartitionValidation(t *testing.T) {
	for _, test := range []struct {
		parts       []string
		granularity string
		want        []string
	}{
		{[]string{"pt=2024-02-03", "pt=2024-02-01"}, "day", []string{"2024-02-01", "2024-02-03"}},
		{[]string{"pt=2024-02-03 12:00:00"}, "hour", []string{"2024-02-03 12:00:00"}},
		{[]string{"pt=2024-02-03 12:00:00"}, "day", nil},
		{[]string{"pt=2024-02-03' OR 1=1"}, "day", nil},
		{[]string{"other=2024-02-03"}, "day", nil},
		{[]string{"pt=2024-02-03"}, "minute", nil},
	} {
		values, err := userStagePartitionValues(test.parts, test.granularity)
		if test.want == nil {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, test.want, values)
		}
	}
	users := model.TableSchema{"received_at": "datetime", "loaded_at": "datetime", "email": "string"}
	source, granularity, err := userStageSourceFromExpression("trunc_time(`loaded_at`, 'hour')", users)
	require.NoError(t, err)
	require.Equal(t, "loaded_at", source)
	require.Equal(t, "hour", granularity)
	for _, expression := range []string{"trunc_time(GETDATE(), 'day')", "trunc_time(`email`, 'day')", "trunc_time(`missing`, 'day')", "trunc_time(`received_at`, 'minute')"} {
		_, _, err := userStageSourceFromExpression(expression, users)
		require.Error(t, err)
	}
}

func TestUsersCarryForwardPreflightAndCleanup(t *testing.T) {
	m, u, statements := testUsersManager(t)
	u.files = nil
	require.NoError(t, m.loadUsersWithCarryForward(context.Background()))
	require.Empty(t, *statements)
	u.files = []warehouseutils.LoadFile{{Location: "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/one.gz"}, {Location: "https://evil.example/bad.gz"}}
	require.ErrorContains(t, m.loadUsersWithCarryForward(context.Background()), "load file")
	require.Empty(t, *statements)
	u.err = errors.New("router failed with secret")
	require.ErrorContains(t, m.loadUsersWithCarryForward(context.Background()), "load files failed")
	require.NotContains(t, m.loadUsersWithCarryForward(context.Background()).Error(), "secret")
	require.Empty(t, *statements)
	u.err = nil
	u.files = []warehouseutils.LoadFile{{Location: "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/one.gz"}}
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
