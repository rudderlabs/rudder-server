package maxcompute

import (
	"context"
	"fmt"
	"html"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
)

func TestFetchSchemaKeepsTableNamesAndExcludesGeneratedPTAndViews(t *testing.T) {
	expression := "[{\"leafExprDesc\":{\"reference\":{\"name\":\"received_at\"}}},{\"leafExprDesc\":{\"constant\":\"hour\"}},{\"functionCall\":{\"name\":\"trunc_time\"}}]"
	catalogSchema, err := jsonrs.Marshal(map[string]any{
		"columns": []any{
			map[string]any{"name": "id", "type": "string"},
			map[string]any{"name": "received_at", "type": "timestamp"},
		},
		"PartitionKeys": []any{
			map[string]any{"name": "pt", "type": "string", "hasGenerateExpression": true, "generateExpression": expression},
		},
	})
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/tables") {
			_, _ = fmt.Fprint(w, "<Tables><Table><Name>product_view_clicked</Name><Owner>owner</Owner></Table><Table><Name>actual_view</Name><Owner>owner</Owner></Table></Tables>")
			return
		}
		kind := odps.ManagedTable.String()
		if strings.HasSuffix(r.URL.Path, "/actual_view") {
			kind = odps.VirtualView.String()
		}
		if strings.HasSuffix(r.URL.Path, "/without_partition") {
			_, _ = fmt.Fprintf(w, "<Table><Owner>owner</Owner><Type>%s</Type><Schema>%s</Schema></Table>", kind, html.EscapeString(`{"columns":[{"name":"id","type":"string"}]}`))
			return
		}
		_, _ = fmt.Fprintf(w, "<Table><Owner>owner</Owner><Type>%s</Type><Schema>%s</Schema></Table>", kind, html.EscapeString(string(catalogSchema)))
	}))
	defer server.Close()
	instance := odps.NewOdps(account.NewAliyunAccount("id", "secret"), server.URL)
	instance.SetDefaultProjectName("project")
	m := &MaxCompute{odps: instance}
	schema, err := m.FetchSchema(context.Background())
	require.NoError(t, err)
	require.Contains(t, schema, "product_view_clicked")
	require.NotContains(t, schema, "actual_view")
	require.Equal(t, "datetime", schema["product_view_clicked"]["received_at"])
	require.NotContains(t, schema["product_view_clicked"], "pt")

	plan, err := m.CatalogPartition(context.Background(), "product_view_clicked")
	require.NoError(t, err)
	require.Equal(t, "received_at", plan.Source)
	require.Equal(t, "hour", plan.Granularity)
	_, err = m.CatalogPartition(context.Background(), "without_partition")
	require.ErrorContains(t, err, "no supported pt partition expression")
}

func TestFetchSchemaMissingNamespaceIsEmpty(t *testing.T) {
	for _, tc := range []struct {
		name, code, message string
		missing             bool
	}{
		{
			name: "missing schema", code: "ODPS-0420095",
			message: "Access Denied: NO privilege 'odps:List' on /projects/project/schemas/missing. Schema project.missing does not exist Tips: Principal:RAM$123456789:987654321",
			missing: true,
		},
		{name: "other permission error", code: "ODPS-0420095", message: "Access Denied: NO privilege 'odps:List' on /projects/project/schemas/existing"},
		{name: "other error code", code: "ODPS-0420096", message: "Schema project.missing does not exist"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusForbidden)
				_, _ = fmt.Fprintf(w, "<Error><Code>%s</Code><Message>%s</Message></Error>", tc.code, html.EscapeString(tc.message))
			}))
			defer server.Close()
			instance := odps.NewOdps(account.NewAliyunAccount("id", "secret"), server.URL)
			instance.SetDefaultProjectName("project")
			m := &MaxCompute{odps: instance}
			schema, err := m.FetchSchema(context.Background())
			if tc.missing {
				require.NoError(t, err)
				require.NotNil(t, schema)
				require.Empty(t, schema)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.code)
		})
	}
}
