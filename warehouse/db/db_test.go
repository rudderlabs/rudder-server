//go:build !warehouse_integration

package db_test

import (
	"github.com/lib/pq"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/warehouse/db"
	"github.com/rudderlabs/rudder-server/warehouse/utils"
)

var _ = Describe("Db", func() {
	DescribeTable("ClauseQueryArgs", func(filterClauses []warehouseutils.FilterClause, expectedQuery string, expectedArgs []interface{}) {
		query, args := db.ClauseQueryArgs(filterClauses...)
		Expect(query).To(Equal(expectedQuery))
		Expect(args).To(Equal(expectedArgs))
	},
		Entry(nil, []warehouseutils.FilterClause{}, "", nil),
		Entry(nil, []warehouseutils.FilterClause{{Clause: "id = <noop>", ClauseArg: 1}}, "id = $1", []interface{}{1}),
		Entry(nil, []warehouseutils.FilterClause{{Clause: "id = <noop>", ClauseArg: 1}, {Clause: "val = <noop>", ClauseArg: 2}}, "id = $1 AND val = $2", []interface{}{1, 2}),
		Entry(nil, []warehouseutils.FilterClause{{Clause: "id = ANY(<noop>)", ClauseArg: pq.Array([]interface{}{1})}, {Clause: "val = <noop>", ClauseArg: 2}}, "id = ANY($1) AND val = $2", []interface{}{pq.GenericArray{A: []interface{}{1}}, 2}),
	)
})
