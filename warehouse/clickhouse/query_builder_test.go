package clickhouse

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"regexp"
)

var _ = Describe("Clickhouse QueryBuilder", func() {
	DescribeTable("addColumnsSQLStatement", func(clusterClause string, columnsInfo warehouseutils.ColumnsInto, expected string) {
		got := addColumnsSQLStatement("testNamespace", "testTableName", clusterClause, columnsInfo)
		r := regexp.MustCompile(`\s+`)
		Expect(r.ReplaceAllString(got, "")).To(BeEquivalentTo(r.ReplaceAllString(expected, "")))
	},
		Entry(nil, "", warehouseutils.ColumnsInto{
			{
				Name: "testColumnName-1",
				Type: "string",
			},
			{
				Name: "testColumnName-2",
				Type: "int",
			},
		},
			`
		ALTER TABLE
		"testNamespace"."testTableName"
		ADD COLUMN IF NOT EXISTS "testColumnName-1" Nullable(String),
		ADD COLUMN IF NOT EXISTS "testColumnName-2" Nullable(Int64);`,
		),
		Entry(nil, "ON CLUSTER testCluster", warehouseutils.ColumnsInto{
			{
				Name: "testColumnName-1",
				Type: "string",
			},
			{
				Name: "testColumnName-2",
				Type: "int",
			},
		},
			`
		ALTER TABLE
		"testNamespace"."testTableName" ON CLUSTER testCluster
		ADD COLUMN IF NOT EXISTS "testColumnName-1" Nullable(String),
		ADD COLUMN IF NOT EXISTS "testColumnName-2" Nullable(Int64);`,
		),
	)
})
