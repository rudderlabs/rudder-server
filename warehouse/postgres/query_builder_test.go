package postgres

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"regexp"
)

var _ = Describe("Postgres QueryBuilder", func() {
	DescribeTable("addColumnsSQLStatement", func(columnsInfo warehouseutils.ColumnsInto, expected string) {
		got := addColumnsSQLStatement("testNamespace", "testTableName", columnsInfo)
		r := regexp.MustCompile(`\s+`)
		Expect(r.ReplaceAllString(got, "")).To(BeEquivalentTo(r.ReplaceAllString(expected, "")))
	},
		Entry(nil, warehouseutils.ColumnsInto{
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
		testNamespace.testTableName
		ADD COLUMN IF NOT EXISTS testColumnName-1 text,
		ADD COLUMN IF NOT EXISTS testColumnName-2 bigint;`,
		),
	)
})
