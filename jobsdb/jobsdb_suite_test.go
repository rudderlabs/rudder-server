package jobsdb_test

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/stats"
	uuid "github.com/satori/go.uuid"
)

func TestJobsdb(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Jobsdb Suite")
}

type context struct {
	testDB jobsdb.HandleT

	//Mocks
	// cokCtrl *gomock.Controller
	// mockDB  *mocks_jobsdb.MockJobsDB
}

func (c *context) contextSetup() {
	c.testDB = jobsdb.HandleT{}
	stats.Setup()
	c.testDB.Setup(jobsdb.ReadWrite, false, "gw", 0*time.Hour, "", false, jobsdb.QueryFiltersT{})

	//Setting Up mocks now

}

// var _ = Describe("testing jobsdb functions", func() {
// 	var c *context
// 	BeforeEach(func() {
// 		c = &context{}
// 		c.contextSetup()
// 	})
// 	AfterEach(func() {
// 		c.testDB.TearDown()
// 	})
// 	Describe("jobsdb.Store", func() {
// 		Context("jobsdb.Store with a proper JobT", func() {
// 			for _, testCase := range properStoreJobs {
// 				It("Should return nil error", func() {
// 					Expect(c.testDB.Store([]*jobsdb.JobT{&testCase.testJob})).To(BeNil())
// 				})
// 			}
// 		})
// 		// Context("jobsdb.Store with failing JobT", func() {
// 		// 	It("returns some error", func() {
// 		// 		//Think of a test case which would return a non-nil error
// 		// 	})
// 		// })
// 	})
// 	Describe("jobsdb.StoreWithRetryEach", func() {
// 		Context("jobsdb.StoreWithRetryEach with a proper JobT", func() {
// 			for _, testCase := range properStoreJobs {
// 				It("Should return nil error", func() {
// 					Expect(c.testDB.StoreWithRetryEach([]*jobsdb.JobT{&testCase.testJob})).To(BeNil())
// 				})
// 			}
// 		})
// 	})
// })

/*
	Test cases
*/
type storeTest struct {
	testJob   jobsdb.JobT
	testError error
}

var properStoreJobs = []storeTest{
	{
		testJob: jobsdb.JobT{
			Parameters:   []byte(`{"batch_id":1,"source_id":"1rNMpysD4lTuzglyfmPzsmihAbK","source_job_run_id":""}`),
			EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"1rNMpxFxVdoaAdItcXTbVVWdonD","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"90ca6da0-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
			UserID:       "90ca6da0-292e-4e79-9880-f8009e0ae4a3",
			UUID:         uuid.NewV4(),
			CustomVal:    "GW",
		},
		testError: nil,
	},
	{
		testJob: jobsdb.JobT{
			Parameters:   []byte(`{"batch_id":2,"source_id":"1rNMpysD4lTuzglyfmPzsmihAbK","source_job_run_id":"random_sourceJobRunID"}`),
			EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"1rNMpxFxVdoaAdItcXTbVVWdonD","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"90ca6da0-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
			UserID:       "dummy_90ca6da0-292e-4e79-9880-f8009e0ae4a3",
			UUID:         uuid.NewV4(),
			CustomVal:    "WEBHOOK",
		},
		testError: nil,
	},
	{
		//this example was while trying for a job that would fail
		testJob: jobsdb.JobT{
			Parameters:   []byte(`{}`),
			EventPayload: []byte(`{}`),
			UserID:       "",
			// UUID:         uuid.NewV4(),
			UUID:      uuid.UUID{},
			CustomVal: "WEBHOOK",
		},
		testError: nil,
	},
}

// var errorStoreJobs = []storeTest{}
