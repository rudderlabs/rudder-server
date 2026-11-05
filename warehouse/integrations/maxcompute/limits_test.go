package maxcompute

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestMaxComputeIdentifierLimits(t *testing.T) {
	accepted := strings.Repeat("a", 128)
	_, err := quote(accepted)
	require.NoError(t, err)
	_, err = quote(accepted + "a")
	require.ErrorContains(t, err, "invalid MaxCompute identifier")

	service := odps.NewOdps(nil, "https://service.cn-hangzhou.maxcompute.aliyun.com/api")
	service.SetDefaultProjectName("project")
	m := &MaxCompute{odps: service, warehouse: model.Warehouse{Namespace: accepted}}
	_, err = m.qualified(accepted)
	require.NoError(t, err)
	m.warehouse.Namespace = accepted + "a"
	_, err = m.qualified("table")
	require.Error(t, err)
	m.warehouse.Namespace = "schema"
	_, err = m.qualified(accepted + "a")
	require.Error(t, err)
	staging := warehouseutils.StagingTableName(provider, accepted, 128)
	require.LessOrEqual(t, len(staging), 128)
	_, err = quote(staging)
	require.NoError(t, err)
	view := viewName(accepted)
	require.LessOrEqual(t, len(view), 128)
	_, err = quote(view)
	require.NoError(t, err)
	m.conf = config.New()
	m.warehouse.Destination.Config = map[string]any{"skipViews": true}
	var statements []string
	m.exec = func(_ context.Context, statement string) error {
		statements = append(statements, statement)
		return nil
	}
	require.NoError(t, m.CreateTable(context.Background(), "events", model.TableSchema{accepted: "string"}))
	require.Error(t, m.CreateTable(context.Background(), "events", model.TableSchema{accepted + "a": "string"}))
	require.Len(t, statements, 1, "overlength column must submit no DDL")
}

func TestMaxComputeColumnLimitsBeforeDDL(t *testing.T) {
	service := odps.NewOdps(nil, "https://service.cn-hangzhou.maxcompute.aliyun.com/api")
	service.SetDefaultProjectName("project")
	m := &MaxCompute{odps: service, conf: config.New()}
	m.warehouse.Destination.Config = map[string]any{"skipViews": true}
	var statements []string
	m.exec = func(_ context.Context, statement string) error {
		statements = append(statements, statement)
		return nil
	}
	columns := make(model.TableSchema)
	for i := range 1199 {
		columns[fmt.Sprintf("c_%d", i)] = "string"
	}
	require.NoError(t, m.CreateTable(context.Background(), "events", columns)) // uuid_ts is added as column 1200.
	require.Len(t, statements, 1)
	columns["extra"] = "string"
	require.ErrorContains(t, m.CreateTable(context.Background(), "events", columns), "1200 columns")
	require.Len(t, statements, 1, "over-limit CREATE must submit no DDL")

	existing := make(model.TableSchema)
	for i := range 1199 {
		existing[fmt.Sprintf("c_%d", i)] = "string"
	}
	m.catalog = func(context.Context) (model.Schema, error) {
		return model.Schema{"events": existing}, nil
	}
	require.NoError(t, m.AddColumns(context.Background(), "events", []warehouseutils.ColumnInfo{{Name: "new", Type: "string"}}))
	require.Len(t, statements, 2)
	existing["new"] = "string" // catalog now reflects the successful ALTER
	require.ErrorContains(t, m.AddColumns(context.Background(), "events", []warehouseutils.ColumnInfo{{Name: "another", Type: "string"}}), "1200 columns")
	require.Len(t, statements, 2, "over-limit ALTER must submit no DDL")
}
