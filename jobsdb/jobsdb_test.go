package jobsdb

import (
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("Calculate newDSIdx", func() {
	var _ = DescribeTable("newDSIdx tests",
		func(before, after, expected string) {
			computedSlice, err := computeInsertIdx(before, after)
			computedIdx := strings.Trim(strings.Replace(fmt.Sprint(computedSlice), " ", "_", -1), "[]")
			Expect(computedIdx).To(Equal(expected))
			Expect(err).To(BeNil())
		},
		//dList => 1 2 3 4 5
		Entry("Normal Case 1 Test 1 : ", "1", "2", "1_1"),
		Entry("Normal Case 1 Test 2 : ", "2", "3", "2_1"),

		//dList => 1_1 2 3 4 5
		Entry("Normal Case 2 Test 1 : ", "1_1", "2", "1_2"),
		Entry("Normal Case 2 Test 2 : ", "2", "3", "2_1"),

		//dList => 1 2_1 3 4 5
		Entry("Normal Case 3 Test 1 : ", "1", "2_1", "1_1"),
		Entry("Normal Case 3 Test 2 : ", "2_1", "3", "2_2"),
		Entry("Normal Case 3 Test 3 : ", "3", "4", "3_1"),

		//dList => 1_1 2_1 3 4 5
		Entry("Normal Case 4 Test 1 : ", "1_1", "2_1", "1_2"),

		//dList => 0_1 1 2 3 4 5
		Entry("ClusterMigraion Case 1 Test 1 : ", "0_1", "1", "0_1_1"), //Failing test - Do we really need to level up here?
		Entry("ClusterMigraion Case 1 Test 2 : ", "1", "2", "1_1"),

		//dList => 0_1 0_2 1 2 3 4 5
		Entry("ClusterMigraion Case 2 Test 1 : ", "0_1", "0_2", "0_1_1"),
		Entry("ClusterMigraion Case 2 Test 2 : ", "0_2", "1", "0_2_1"), //Failing test - Do we really need to level up here?
		Entry("ClusterMigraion Case 2 Test 3 : ", "1", "2", "1_1"),

		//dList => 0_1_1 0_2 1 2 3 4 5
		Entry("ClusterMigraion Case 3 Test 1 : ", "0_1_1", "0_2", "0_1_2"),
		Entry("ClusterMigraion Case 3 Test 2 : ", "0_2", "1", "0_2_1"), //Failing test - Do we really need to level up here?

		//dList => 0_1_1 0_2_1 1 2 3 4 5
		Entry("ClusterMigraion Case 4 Test 1 : ", "0_2_1", "1", "0_2_2"),
		Entry("ClusterMigraion Case 4 Test 2 : ", "0_1_1", "0_2_1", "0_1_2"),

		//dList => 0_1 0_2_1 1 2 3
		Entry("ClusterMigraion Case 5 Test 1 : ", "0_1", "0_2_1", "0_1_1"), //Failing test - Do we really need to level up here?
	)
})
