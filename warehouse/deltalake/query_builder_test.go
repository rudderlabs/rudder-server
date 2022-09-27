package deltalake

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"strings"
)

var _ = Describe("Deltalake QueryBuilder", func() {
	DescribeTable("addColumnsSQLStatement", func(columnsInfo warehouseutils.ColumnsInto, expected string) {
		got := addColumnsSQLStatement("testNamespace", "testTableName", columnsInfo)
		Expect(strings.Trim(got, " ")).To(BeEquivalentTo(expected))
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
