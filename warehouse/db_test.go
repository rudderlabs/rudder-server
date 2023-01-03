package warehouse_test

import (
	"github.com/lib/pq"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/warehouse"
)

var _ = Describe("Db", func() {
	DescribeTable("ClauseQueryArgs", func(filterClauses []warehouse.FilterClause, expectedQuery string, expectedArgs []interface{}) {
		query, args := warehouse.ClauseQueryArgs(filterClauses...)
		Expect(query).To(Equal(expectedQuery))
		Expect(args).To(Equal(expectedArgs))
	},
		Entry(nil, []warehouse.FilterClause{}, "", nil),
		Entry(nil, []warehouse.FilterClause{{Clause: "id = <noop>", ClauseArg: 1}}, "id = $1", []interface{}{1}),
		Entry(nil, []warehouse.FilterClause{{Clause: "id = <noop>", ClauseArg: 1}, {Clause: "val = <noop>", ClauseArg: 2}}, "id = $1 AND val = $2", []interface{}{1, 2}),
		Entry(nil, []warehouse.FilterClause{{Clause: "id = ANY(<noop>)", ClauseArg: pq.Array([]interface{}{1})}, {Clause: "val = <noop>", ClauseArg: 2}}, "id = ANY($1) AND val = $2", []interface{}{pq.GenericArray{A: []interface{}{1}}, 2}),
	)
})
