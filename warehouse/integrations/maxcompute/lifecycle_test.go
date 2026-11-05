package maxcompute

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestCleanupDropsOnlyMaxComputeStagingTables(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "<Tables><Table><Name>rudder_staging_users_upload_123</Name></Table><Table><Name>users</Name></Table><Table><Name>users_view</Name></Table></Tables>")
	}))
	defer server.Close()
	m := testManager()
	m.odps = odps.NewOdps(account.NewAliyunAccount("id", "secret"), server.URL)
	m.odps.SetDefaultProjectName("project")
	var statements []string
	m.exec = func(_ context.Context, sql string) error { statements = append(statements, sql); return nil }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m.Cleanup(ctx)
	require.Equal(t, []string{"DROP TABLE IF EXISTS `project`.`rudder_staging_users_upload_123`"}, statements)
}

func TestIsEmptyUsesBoundedPTProbe(t *testing.T) {
	m := testManager()
	m.catalog = func(context.Context) (model.Schema, error) {
		return model.Schema{"tracks": {"id": "string"}, "users": {"id": "string"}}, nil
	}
	var statements []string
	m.query = func(_ context.Context, sql string) (warehouseutils.QueryResult, error) {
		statements = append(statements, sql)
		return warehouseutils.QueryResult{}, nil
	}
	empty, err := m.IsEmpty(context.Background(), m.warehouse)
	require.NoError(t, err)
	require.True(t, empty)
	require.Len(t, statements, 2)
	for _, sql := range statements {
		require.True(t, strings.HasPrefix(sql, "SELECT 1 FROM `project`.`"))
		require.Contains(t, sql, "`pt` >= '")
		require.Contains(t, sql, "`pt` < '")
		require.True(t, strings.HasSuffix(sql, " LIMIT 1"))
	}
	m.query = func(context.Context, string) (warehouseutils.QueryResult, error) {
		return warehouseutils.QueryResult{Values: [][]string{{"1"}}}, nil
	}
	empty, err = m.IsEmpty(context.Background(), m.warehouse)
	require.NoError(t, err)
	require.False(t, empty)
}
