package jobsdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	uuid "github.com/gofrs/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb/prebackup"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
)

var _ = Describe("Calculate newDSIdx for internal migrations", func() {
	initJobsDB()

	var _ = DescribeTable("newDSIdx tests",
		func(before, after, expected string) {
			computedIdx, err := computeInsertIdx(before, after)
			Expect(computedIdx).To(Equal(expected))
			Expect(err).To(BeNil())
		},
		//dList => 1 2 3 4 5
		Entry("Internal Migration for regular tables 1 Test 1 : ", "1", "2", "1_1"),
		Entry("Internal Migration for regular tables 1 Test 2 : ", "2", "3", "2_1"),

		//dList => 1_1 2 3 4 5
		Entry("Internal Migration for regular tables 2 Test 1 : ", "1_1", "2", "1_2"),
		Entry("Internal Migration for regular tables 2 Test 2 : ", "2", "3", "2_1"),

		//dList => 1 2_1 3 4 5
		Entry("Internal Migration for regular tables 3 Test 1 : ", "1", "2_1", "1_1"),
		Entry("Internal Migration for regular tables 3 Test 2 : ", "2_1", "3", "2_2"),
		Entry("Internal Migration for regular tables 3 Test 3 : ", "3", "4", "3_1"),

		//dList => 1_1 2_1 3 4 5
		Entry("Internal Migration for regular tables 4 Test 1 : ", "1_1", "2_1", "1_2"),

		//dList => 0_1 1 2 3 4 5
		Entry("Internal Migration for import tables Case 1 Test 1 : ", "0_1", "1", "0_1_1"),
		Entry("Internal Migration for import tables Case 1 Test 2 : ", "1", "2", "1_1"),

		//dList => 0_1 0_2 1 2 3 4 5
		Entry("Internal Migration for import tables Case 2 Test 1 : ", "0_1", "0_2", "0_1_1"),
		Entry("Internal Migration for import tables Case 2 Test 2 : ", "0_2", "1", "0_2_1"),
		Entry("Internal Migration for import tables Case 2 Test 3 : ", "1", "2", "1_1"),

		//dList => 0_1_1 0_2 1 2 3 4 5
		Entry("Internal Migration for import tables Case 3 Test 1 : ", "0_1_1", "0_2", "0_1_2"),
		Entry("Internal Migration for import tables Case 3 Test 2 : ", "0_2", "1", "0_2_1"),

		//dList => 0_1_1 0_2_1 1 2 3 4 5
		Entry("Internal Migration for import tables Case 4 Test 1 : ", "0_2_1", "1", "0_2_2"),
		Entry("Internal Migration for import tables Case 4 Test 2 : ", "0_1_1", "0_2_1", "0_1_2"),

		//dList => 0_1 0_2_1 1 2 3
		Entry("Internal Migration for import tables Case 5 Test 1 : ", "0_1", "0_2_1", "0_1_1"),

		Entry("OrderTest Case 1 Test 1 : ", "9", "10", "9_1"),

		Entry("Internal Migration for tables : ", "10_1", "11_3", "10_2"),
		Entry("Internal Migration for tables : ", "0_1", "1", "0_1_1"),
		Entry("Internal Migration for tables : ", "0_1", "20", "0_1_1"),
		Entry("Internal Migration for tables : ", "0_1", "0_2", "0_1_1"),
	)

	Context("computeInsertIdx - bad input tests", func() {
		It("Should throw error for input 1, 1_1", func() {
			_, err := computeInsertIdx("1", "1_1")
			Expect(err).To(HaveOccurred())
		})
		It("Should throw error for input 10_1, 10_2", func() {
			_, err := computeInsertIdx("10_1", "10_2")
			Expect(err).To(HaveOccurred())
		})
		It("Should throw error for input 10_1, 10_1", func() {
			_, err := computeInsertIdx("10_1", "10_1")
			Expect(err).To(HaveOccurred())
		})
		It("Should throw error for input 10, 9", func() {
			_, err := computeInsertIdx("10", "9")
			Expect(err).To(HaveOccurred())
		})
		It("Should throw error for input 10_1_2, 11_3", func() {
			_, err := computeInsertIdx("10_1_2", "11_3")
			Expect(err).To(HaveOccurred())
		})
		It("Should throw error for input 0, 1", func() {
			_, err := computeInsertIdx("0", "1")
			Expect(err).To(HaveOccurred())
		})
		It("Should throw error for input 0_1, 0", func() {
			_, err := computeInsertIdx("0_1", "0")
			Expect(err).To(HaveOccurred())
		})
	})

	var _ = DescribeTable("newDSIdx tests with skipZeroAssertionForMultitenant",
		func(before, after, expected string) {
			setSkipZeroAssertionForMultitenant(true)
			computedIdx, err := computeInsertIdx(before, after)
			Expect(computedIdx).To(Equal(expected))
			Expect(err).To(BeNil())
			setSkipZeroAssertionForMultitenant(false)
		},
		//dList => 1 2 3 4 5
		Entry("Internal Migration for regular tables 1 Test 1 with skipZeroAssertionForMultitenant: ", "1", "2", "1_1"),
		Entry("Internal Migration for regular tables 1 Test 2 with skipZeroAssertionForMultitenant: ", "2", "3", "2_1"),

		//dList => 1_1 2 3 4 5
		Entry("Internal Migration for regular tables 2 Test 1 with skipZeroAssertionForMultitenant: ", "1_1", "2", "1_2"),
		Entry("Internal Migration for regular tables 2 Test 2 with skipZeroAssertionForMultitenant: ", "2", "3", "2_1"),

		//dList => 1 2_1 3 4 5
		Entry("Internal Migration for regular tables 3 Test 1 with skipZeroAssertionForMultitenant: ", "1", "2_1", "1_1"),
		Entry("Internal Migration for regular tables 3 Test 2 with skipZeroAssertionForMultitenant: ", "2_1", "3", "2_2"),
		Entry("Internal Migration for regular tables 3 Test 3 with skipZeroAssertionForMultitenant: ", "3", "4", "3_1"),

		//dList => 1_1 2_1 3 4 5
		Entry("Internal Migration for regular tables 4 Test 1 with skipZeroAssertionForMultitenant: ", "1_1", "2_1", "1_2"),

		//dList => 0_1 1 2 3 4 5
		Entry("Internal Migration for import tables Case 1 Test 2 with skipZeroAssertionForMultitenant: ", "1", "2", "1_1"),

		Entry("Internal Migration for import tables Case 2 Test 3 with skipZeroAssertionForMultitenant: ", "1", "2", "1_1"),

		Entry("OrderTest Case 1 Test 1 with skipZeroAssertionForMultitenant: ", "9", "10", "9_1"),

		Entry("Internal Migration for tables with skipZeroAssertionForMultitenant: ", "10_1", "11_3", "10_2"),
		Entry("Internal Migration for tables with skipZeroAssertionForMultitenant: ", "0_1", "1", "0_2"),
		Entry("Internal Migration for tables with skipZeroAssertionForMultitenant: ", "0_1", "20", "0_2"),
		Entry("Internal Migration for tables with Negative Indexes and skipZeroAssertionForMultitenant: ", "-1", "0", "-1_1"),
		Entry("Internal Migration for tables with Negative Indexes and skipZeroAssertionForMultitenant: ", "0", "1", "0_1"),
		Entry("Internal Migration for tables with Negative Indexes and skipZeroAssertionForMultitenant: ", "-2_1", "-1_1", "-2_2"),
		Entry("Internal Migration for tables with Negative Indexes and skipZeroAssertionForMultitenant: ", "-2_1", "-1", "-2_2"),
		Entry("Internal Migration for tables with Negative Indexes and skipZeroAssertionForMultitenant: ", "-2_1", "0", "-2_2"),
		Entry("Internal Migration for tables with Negative Indexes and skipZeroAssertionForMultitenant: ", "-2_1", "20", "-2_2"),
	)

	Context("computeInsertVals - good input tests", func() {
		It("Should not throw error for input 0_1, 0_2", func() {
			calculatedIdx, err := computeInsertVals([]string{"0", "1"}, []string{"0", "2"})
			Expect(err).To(BeNil())
			Expect(calculatedIdx).To(Equal([]string{"0", "1", "1"}))
		})
	})

	Context("computeInsertVals - bad input tests", func() {
		It("Should throw error for input 1, 1_1", func() {
			_, err := computeInsertVals([]string{"1"}, []string{"1", "1"})
			Expect(err).To(HaveOccurred())
		})
		It("Should throw error for input 10_1, 10_2", func() {
			_, err := computeInsertVals([]string{"10", "1"}, []string{"10", "2"})
			Expect(err).To(HaveOccurred())
		})
		It("Should throw error for input 10_1, 10_1", func() {
			_, err := computeInsertVals([]string{"10", "1"}, []string{"10", "1"})
			Expect(err).To(HaveOccurred())
		})
		It("Should throw error for input 10, 9", func() {
			_, err := computeInsertVals([]string{"10"}, []string{"9"})
			Expect(err).To(HaveOccurred())
		})
	})
})

var _ = Describe("Calculate newDSIdx for cluster migrations", func() {
	initJobsDB()

	var _ = DescribeTable("newDSIdx tests",
		func(dList []dataSetT, after dataSetT, expected string) {
			computedIdx, err := computeIdxForClusterMigration("table_prefix", dList, after)
			Expect(computedIdx).To(Equal(expected))
			Expect(err).To(BeNil())
		},

		Entry("ClusterMigration Case 1",
			[]dataSetT{
				{
					JobTable:       "",
					JobStatusTable: "",
					Index:          "1",
				},
			},
			dataSetT{
				JobTable:       "",
				JobStatusTable: "",
				Index:          "1",
			}, "0_1"),

		Entry("ClusterMigration Case 2",
			[]dataSetT{
				{
					JobTable:       "",
					JobStatusTable: "",
					Index:          "0_1",
				},
				{
					JobTable:       "",
					JobStatusTable: "",
					Index:          "1",
				},
				{
					JobTable:       "",
					JobStatusTable: "",
					Index:          "2",
				},
			},
			dataSetT{
				JobTable:       "",
				JobStatusTable: "",
				Index:          "1",
			}, "0_2"),
	)

	var _ = DescribeTable("Error cases",
		func(dList []dataSetT, after dataSetT) {
			_, err := computeIdxForClusterMigration("table_prefix", dList, after)
			Expect(err != nil).Should(BeTrue())
		},

		Entry("ClusterMigration Case 1",
			[]dataSetT{
				{
					JobTable:       "",
					JobStatusTable: "",
					Index:          "1_1",
				},
			},
			dataSetT{
				JobTable:       "",
				JobStatusTable: "",
				Index:          "1_1",
			},
		),

		Entry("ClusterMigration Case 2",
			[]dataSetT{
				{
					JobTable:       "",
					JobStatusTable: "",
					Index:          "1",
				},
				{
					JobTable:       "",
					JobStatusTable: "",
					Index:          "1_1",
				},
			},
			dataSetT{
				JobTable:       "",
				JobStatusTable: "",
				Index:          "1_1",
			},
		),

		Entry("ClusterMigration Case 4",
			[]dataSetT{},
			dataSetT{
				JobTable:       "",
				JobStatusTable: "",
				Index:          "1_1",
			},
		),

		Entry("ClusterMigration Case 5",
			[]dataSetT{},
			dataSetT{
				JobTable:       "",
				JobStatusTable: "",
				Index:          "1_1_1_1",
			},
		),

		Entry("ClusterMigration Case 6",
			[]dataSetT{},
			dataSetT{
				JobTable:       "",
				JobStatusTable: "",
				Index:          "1_1_!_1",
			},
		),
	)
})

var sampleTestJob = JobT{
	Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
	EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
	UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
	UUID:         uuid.Must(uuid.NewV4()),
	CustomVal:    "MOCKDS",
}

func initJobsDB() {
	config.Load()
	logger.Init()
	admin.Init()
	Init()
	Init2()
	Init3()
}

var _ = Describe("jobsdb", func() {
	initJobsDB()

	BeforeEach(func() {
		// setup static requirements of dependencies
		stats.Setup()
	})

	Context("getDSList", func() {
		var jd *HandleT

		BeforeEach(func() {
			jd = &HandleT{}

			jd.skipSetupDBSetup = true
			jd.Setup(ReadWrite, false, "tt", 0*time.Hour, "", false, QueryFiltersT{}, []prebackup.Handler{})
		})

		AfterEach(func() {
			jd.TearDown()
		})

		It("doesn't make db calls if !refreshFromDB", func() {
			jd.datasetList = dsListInMemory

			Expect(jd.getDSList(false)).To(Equal(dsListInMemory))
		})
	})
})

var d1 = dataSetT{JobTable: "tt_jobs_1",
	JobStatusTable: "tt_job_status_1"}

var d2 = dataSetT{
	JobTable:       "tt_jobs_2",
	JobStatusTable: "tt_job_status_2",
}

var dsListInMemory = []dataSetT{
	d1,
	d2,
}

func BenchmarkSanitizeJson(b *testing.B) {
	size := 4_000
	nulls := 100

	// string with nulls
	inputWithoutNulls := randomString(size - nulls*len(`\u0000`))
	inputWithNulls := insertStringInString(inputWithoutNulls, `\u0000`, nulls)
	require.Equal(b, json.RawMessage(inputWithoutNulls), sanitizedJsonUsingStrings(json.RawMessage(inputWithNulls)))
	require.Equal(b, json.RawMessage(inputWithoutNulls), sanitizedJsonUsingBytes(json.RawMessage(inputWithNulls)))
	require.Equal(b, json.RawMessage(inputWithoutNulls), sanitizedJsonUsingRegexp(json.RawMessage(inputWithNulls)))
	b.Run(fmt.Sprintf("SanitizeUsingStrings string of size %d with null characters", size), func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			sanitizedJsonUsingStrings(json.RawMessage(inputWithNulls))
		}
	})
	b.Run(fmt.Sprintf("SanitizeUsingBytes string of size %d with null characters", size), func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			sanitizedJsonUsingBytes(json.RawMessage(inputWithNulls))
		}
	})
	b.Run(fmt.Sprintf("SanitizeUsingRegexp string of size %d with null characters", size), func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			sanitizedJsonUsingRegexp(json.RawMessage(inputWithNulls))
		}
	})

	// string without null characters
	input := randomString(size)
	b.Run(fmt.Sprintf("SanitizeUsingStrings string of size %d without null characters", size), func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			sanitizedJsonUsingStrings(json.RawMessage(input))
		}
	})
	b.Run(fmt.Sprintf("SanitizeUsingBytes string of size %d without null characters", size), func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			sanitizedJsonUsingBytes(json.RawMessage(input))
		}
	})
	b.Run(fmt.Sprintf("SanitizeUsingRegexp string of size %d without null characters", size), func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			sanitizedJsonUsingRegexp(json.RawMessage(input))
		}
	})

}

func randomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

func insertStringInString(input string, c string, times int) string {
	if times == 0 {
		return input
	}
	pos := map[int]struct{}{}
	for len(pos) < times {
		newPos := rand.Intn(len(input))
		pos[newPos] = struct{}{}
	}
	keys := make([]int, 0, len(pos))
	for k := range pos {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	offset := len(c)
	for i, idx := range keys {
		oidx := idx + i*offset
		input = input[:oidx] + c + input[oidx:]
	}
	return input
}

func sanitizedJsonUsingStrings(input json.RawMessage) json.RawMessage {
	return json.RawMessage(strings.Replace(string(input), `\u0000`, "", -1))
}

func sanitizedJsonUsingBytes(input json.RawMessage) json.RawMessage {
	return bytes.ReplaceAll(input, []byte(`\u0000`), []byte(""))
}

var sanitizeRegexp = regexp.MustCompile(`\\u0000`)

func sanitizedJsonUsingRegexp(input json.RawMessage) json.RawMessage {
	return json.RawMessage(sanitizeRegexp.ReplaceAllString(string(input), ""))
}

func setSkipZeroAssertionForMultitenant(b bool) {
	skipZeroAssertionForMultitenant = b
}
