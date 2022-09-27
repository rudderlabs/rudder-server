package postgres

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var _ = Describe("Postgres QueryBuilder", func() {
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
		ADD COLUMN IF NOT EXISTS testColumnName-1 text,
		ADD COLUMN IF NOT EXISTS testColumnName-2 bigint`,
		),
	)
})
