package deltalake

import (
	"regexp"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var _ = Describe("Deltalake QueryBuilder", func() {
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
		ADD COLUMNS ( testColumnName-1 STRING,testColumnName-2 BIGINT );`,
		),
	)
})
