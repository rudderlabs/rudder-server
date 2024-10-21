package jobsdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	rsRand "github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
	"github.com/rudderlabs/rudder-server/utils/misc"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
)

func TestComputeInsertIdx(t *testing.T) {
	t.Run("newDSIdx tests", func(t *testing.T) {
		testCases := []struct {
			before   string
			after    string
			expected string
		}{
			// dList => 1 2 3 4 5
			{"1", "2", "1_1"},
			{"2", "3", "2_1"},
			// dList => 1_1 2 3 4 5
			{"1_1", "2", "1_2"},
			{"2", "3", "2_1"},
			// dList => 1 2_1 3 4 5
			{"1", "2_1", "1_1"},
			{"2_1", "3", "2_2"},
			{"3", "4", "3_1"},
			// dList => 1_1 2_1 3 4 5
			{"1_1", "2_1", "1_2"},
			// dList => 0_1 1 2 3 4 5
			{"0_1", "1", "0_2"},
			{"1", "2", "1_1"},
			{"0_2", "1", "0_3"},
			{"1", "2", "1_1"},
			{"0_2", "1", "0_3"},
			{"9", "10", "9_1"},
			{"10_1", "11_3", "10_2"},
			{"0_1", "1", "0_2"},
			{"0_1", "20", "0_2"},
			{"10_1_2", "11_3", "10_2"},
		}
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("computeInsertIdx(%s, %s)", tc.before, tc.after), func(t *testing.T) {
				computedIdx, err := computeInsertIdx(tc.before, tc.after)
				require.NoError(t, err)
				require.Equal(t, tc.expected, computedIdx)
			})
		}
	})

	t.Run("computeInsertIdx - bad input tests", func(t *testing.T) {
		testCases := []struct {
			before string
			after  string
		}{
			{"1", "1_1"},
			{"10_1", "10_2"},
			{"10_1", "10_1"},
			{"10", "9"},
			{"0_1", "0_2"},
			{"0_1", "0"},
		}
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("computeInsertIdx(%s, %s)", tc.before, tc.after), func(t *testing.T) {
				computedIdx, err := computeInsertIdx(tc.before, tc.after)
				require.Error(t, err)
				require.Empty(t, computedIdx)
			})
		}
	})

	t.Run("computeInsertIdx - skipZeroAssertionForMultitenant", func(t *testing.T) {
		testCases := []struct {
			before   string
			after    string
			expected string
		}{
			// dList => 1 2 3 4 5
			{"1", "2", "1_1"},
			{"2", "3", "2_1"},
			// dList => 1_1 2 3 4 5
			{"1_1", "2", "1_2"},
			{"2", "3", "2_1"},
			// dList => 1 2_1 3 4 5
			{"1", "2_1", "1_1"},
			{"2_1", "3", "2_2"},
			{"3", "4", "3_1"},
			// dList => 1_1 2_1 3 4 5
			{"1_1", "2_1", "1_2"},
			// dList => 0_1 1 2 3 4 5
			{"1", "2", "1_1"},
			{"1", "2", "1_1"},
			{"1", "2", "1_1"},
			{"9", "10", "9_1"},
			{"10_1", "11_3", "10_2"},
			{"0_1", "1", "0_2"},
			{"0_1", "20", "0_2"},
			{"-1", "0", "-1_1"},
			{"0", "1", "0_1"},
			{"-2_1", "-1_1", "-2_2"},
			{"-2_1", "-1", "-2_2"},
			{"-2_1", "0", "-2_2"},
			{"-2_1", "20", "-2_2"},
		}
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("computeInsertIdx(%s, %s)", tc.before, tc.after), func(t *testing.T) {
				computedIdx, err := computeInsertIdx(tc.before, tc.after)
				require.NoError(t, err)
				require.Equal(t, tc.expected, computedIdx)
			})
		}
	})
}

func TestJobsdbLifecycle(t *testing.T) {
	t.Run("getDSList", func(t *testing.T) {
		jd := startTestJobsDB(t)
		defer jd.TearDown()
		t.Run("doesn't make db calls if !refreshFromDB", func(t *testing.T) {
			jd.datasetList = dsListInMemory
			require.Equal(t, dsListInMemory, jd.getDSList())
		})
	})

	t.Run("Start & Stop", func(t *testing.T) {
		t.Run("can call Stop before Start without side-effects", func(t *testing.T) {
			jd := startTestJobsDB(t)
			defer jd.TearDown()
			jd.Stop()
			require.NoError(t, jd.Start())
			require.True(t, jd.lifecycle.started)
		})

		t.Run("can call Start twice without side-effects", func(t *testing.T) {
			jd := startTestJobsDB(t)
			defer jd.TearDown()
			require.NoError(t, jd.Start())
			group1 := jd.backgroundGroup
			require.NoError(t, jd.Start())
			group2 := jd.backgroundGroup
			require.Equal(t, group1, group2)
			require.True(t, jd.lifecycle.started)
		})

		t.Run("can call Start in parallel without side-effects", func(t *testing.T) {
			jd := startTestJobsDB(t)
			defer jd.TearDown()
			var wg sync.WaitGroup
			bgGroups := make([]*errgroup.Group, 10)
			wg.Add(10)
			for i := 0; i < 10; i++ {
				idx := i
				go func() {
					require.NoError(t, jd.Start())
					bgGroups[idx] = jd.backgroundGroup
					wg.Done()
				}()
			}
			wg.Wait()
			for i := 1; i < 10; i++ {
				require.Equal(t, bgGroups[i-1], bgGroups[i])
			}
			require.True(t, jd.lifecycle.started)
		})

		t.Run("can call Stop twice without side-effects", func(t *testing.T) {
			jd := startTestJobsDB(t)
			defer jd.TearDown()
			require.NoError(t, jd.Start())
			require.True(t, jd.lifecycle.started)
			require.NotNil(t, jd.backgroundGroup)
			jd.Stop()
			require.NotNil(t, jd.backgroundGroup)
			require.False(t, jd.lifecycle.started)
			require.NoError(t, jd.backgroundGroup.Wait())
			jd.Stop()
			require.NotNil(t, jd.backgroundGroup)
			require.False(t, jd.lifecycle.started)
			require.NoError(t, jd.backgroundGroup.Wait())
		})

		t.Run("can call Stop in parallel without side-effects", func(t *testing.T) {
			jd := startTestJobsDB(t)
			defer jd.TearDown()
			require.NoError(t, jd.Start())

			var wg sync.WaitGroup
			wg.Add(10)
			for i := 0; i < 10; i++ {
				go func() {
					jd.Stop()
					require.NoError(t, jd.backgroundGroup.Wait())
					wg.Done()
				}()
			}
			wg.Wait()
			require.False(t, jd.lifecycle.started)
		})

		t.Run("can call Start & Stop in parallel without problems", func(t *testing.T) {
			jd := startTestJobsDB(t)
			defer jd.TearDown()
			var wg sync.WaitGroup
			wg.Add(10)
			for i := 0; i < 10; i++ {
				go func() {
					require.NoError(t, jd.Start())
					jd.Stop()
					require.NoError(t, jd.backgroundGroup.Wait())
					wg.Done()
				}()
			}
			wg.Wait()
			require.False(t, jd.lifecycle.started)
		})
	})
}

var d1 = dataSetT{
	JobTable:       "tt_jobs_1",
	JobStatusTable: "tt_job_status_1",
}

var d2 = dataSetT{
	JobTable:       "tt_jobs_2",
	JobStatusTable: "tt_job_status_2",
}

var dsListInMemory = []dataSetT{
	d1,
	d2,
}

func startTestJobsDB(t *testing.T) *Handle {
	_ = startPostgres(t)
	jd := &Handle{}
	require.NoError(
		t,
		jd.Setup(
			ReadWrite,
			false,
			strings.ToLower(rsRand.String(5)),
		),
	)
	return jd
}

func BenchmarkSanitizeJson(b *testing.B) {
	size := 4_000
	nulls := 100

	// string with nulls
	inputWithoutNulls := rsRand.String(size - nulls*len(`\u0000`))
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
	input := rsRand.String(size)
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

func insertStringInString(input, c string, times int) string {
	if times == 0 {
		return input
	}
	pos := map[int]struct{}{}
	for len(pos) < times {
		newPos := rand.Intn(len(input)) // skipcq: GSC-G404
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
	return json.RawMessage(strings.ReplaceAll(string(input), `\u0000`, ""))
}

func sanitizedJsonUsingBytes(input json.RawMessage) json.RawMessage {
	return bytes.ReplaceAll(input, []byte(`\u0000`), []byte(""))
}

var sanitizeRegexp = regexp.MustCompile(`\\u0000`)

func sanitizedJsonUsingRegexp(input json.RawMessage) json.RawMessage {
	return json.RawMessage(sanitizeRegexp.ReplaceAllString(string(input), ""))
}

func TestRefreshDSList(t *testing.T) {
	_ = startPostgres(t)
	statStore, err := memstats.New()
	require.NoError(t, err)
	triggerAddNewDS := make(chan time.Time)
	jobsDB := &Handle{
		TriggerAddNewDS: func() <-chan time.Time {
			return triggerAddNewDS
		},
		stats: statStore,
	}

	prefix := strings.ToLower(rsRand.String(5))
	err = jobsDB.Setup(ReadWrite, false, prefix)
	require.NoError(t, err)
	defer jobsDB.TearDown()

	require.Equal(t, 1, len(jobsDB.getDSList()), "jobsDB should start with a ds list size of 1")
	require.NoError(t, jobsDB.WithTx(func(tx *Tx) error {
		return jobsDB.createDSInTx(tx, newDataSet(prefix, "2"))
	}))
	require.Equal(t, 1, len(jobsDB.getDSList()), "addDS should not refresh the ds list")
	jobsDB.dsListLock.WithLock(func(l lock.LockToken) {
		dsList, err := jobsDB.doRefreshDSList(l)
		require.NoError(t, err)
		require.Equal(t, 2, len(dsList), "after refreshing the ds list jobsDB should have a ds list size of 2")
	})
}

func TestJobsDBTimeout(t *testing.T) {
	_ = startPostgres(t)
	defaultWorkspaceID := "workspaceId"
	c := config.New()
	statStore, err := memstats.New()
	require.NoError(t, err)

	c.Set("JobsDB.maxDSSize", 10)
	jobDB := Handle{
		config: c,
		stats:  statStore,
	}

	customVal := "MOCKDS"
	prefix := strings.ToLower(rsRand.String(5))
	err = jobDB.Setup(ReadWrite, false, prefix)
	require.NoError(t, err)
	defer jobDB.TearDown()

	sampleTestJob := JobT{
		Parameters:   []byte(`{}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient", "device_name":"FooBar\ufffd\u0000\ufffd\u000f\ufffd","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.New(),
		CustomVal:    customVal,
		WorkspaceId:  defaultWorkspaceID,
		EventCount:   1,
	}

	err = jobDB.Store(context.Background(), []*JobT{&sampleTestJob})
	require.NoError(t, err)

	t.Run("Test jobsDB GET request context timeout & retry ", func(t *testing.T) {
		tx, err := jobDB.dbHandle.Begin()
		require.NoError(t, err, "Error in starting transaction to lock the table")
		_, err = tx.Exec(fmt.Sprintf(`LOCK TABLE "%s_jobs_1" IN ACCESS EXCLUSIVE MODE;`, prefix))
		require.NoError(t, err, "Error in locking the table")
		defer func() { _ = tx.Rollback() }()

		ctx, cancelCtx := context.WithTimeout(context.Background(), time.Millisecond*10)
		defer cancelCtx()

		expectedRetries := 2
		var errorsCount int

		jobs, err := misc.QueryWithRetries(context.Background(), 10*time.Millisecond, expectedRetries, func(ctx context.Context) (JobsResult, error) {
			jobs, err := jobDB.GetUnprocessed(ctx, GetQueryParams{
				CustomValFilters: []string{customVal},
				JobsLimit:        1,
				ParameterFilters: []ParameterFilterT{},
			})
			if err != nil {
				errorsCount++
			}
			return jobs, err
		})
		require.True(t, len(jobs.Jobs) == 0, "Error in getting unprocessed jobs")
		require.Error(t, err)
		require.True(t, errors.Is(ctx.Err(), context.DeadlineExceeded))
		require.Equal(t, expectedRetries, errorsCount)
	})

	t.Run("Test jobsDB STORE request context timeout & retry ", func(t *testing.T) {
		tx, err := jobDB.dbHandle.Begin()
		require.NoError(t, err, "Error in starting transaction to lock the table")
		defer func() { _ = tx.Rollback() }()
		_, err = tx.Exec(fmt.Sprintf(`LOCK TABLE "%s_jobs_1" IN ACCESS EXCLUSIVE MODE;`, prefix))
		require.NoError(t, err, "Error in locking the table")

		ctx, cancelCtx := context.WithTimeout(context.Background(), time.Millisecond*10)
		defer cancelCtx()

		expectedRetries := 2
		var errorsCount int
		err = misc.RetryWith(context.Background(), 10*time.Millisecond, expectedRetries, func(ctx context.Context) error {
			err := jobDB.Store(ctx, []*JobT{&sampleTestJob})
			if err != nil {
				errorsCount++
			}
			return err
		})
		require.Error(t, err)
		require.True(t, errors.Is(ctx.Err(), context.DeadlineExceeded))
		require.Equal(t, expectedRetries, errorsCount)
	})
}

func TestThreadSafeAddNewDSLoop(t *testing.T) {
	_ = startPostgres(t)
	c := config.New()
	c.Set("JobsDB.maxDSSize", 1)
	statStore, err := memstats.New()
	require.NoError(t, err)
	triggerAddNewDS1 := make(chan time.Time)
	// jobsDB-1 setup
	jobsDB1 := &Handle{
		TriggerAddNewDS: func() <-chan time.Time {
			return triggerAddNewDS1
		},
		config: c,
		stats:  statStore,
	}
	prefix := strings.ToLower(rsRand.String(5))
	err = jobsDB1.Setup(ReadWrite, false, prefix)
	require.NoError(t, err)
	require.Equal(t, 1, len(jobsDB1.getDSList()), "expected cache to be auto-updated with DS list length 1")
	defer jobsDB1.TearDown()

	// jobsDB-2 setup
	triggerAddNewDS2 := make(chan time.Time)
	jobsDB2 := &Handle{
		TriggerAddNewDS: func() <-chan time.Time {
			return triggerAddNewDS2
		},
		config: c,
		stats:  statStore,
	}
	err = jobsDB2.Setup(ReadWrite, false, prefix)
	require.NoError(t, err)
	defer jobsDB2.TearDown()
	require.Equal(t, 1, len(jobsDB2.getDSList()), "expected cache to be auto-updated with DS list length 1")

	generateJobs := func(numOfJob int) []*JobT {
		customVal := "MOCKDS"
		js := make([]*JobT, numOfJob)
		for i := 0; i < numOfJob; i++ {
			js[i] = &JobT{
				Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
				EventPayload: []byte(`{"testKey":"testValue"}`),
				UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
				UUID:         uuid.New(),
				CustomVal:    customVal,
				EventCount:   1,
			}
		}
		return js
	}

	// adding mock jobs to jobsDB-1
	err = jobsDB1.Store(context.Background(), generateJobs(2))
	require.NoError(t, err)

	// triggerAddNewDS1 to trigger jobsDB-1 to add new DS
	triggerAddNewDS1 <- time.Now()
	require.Eventually(
		t,
		func() bool {
			return len(jobsDB1.getDSList()) == 2
		},
		time.Second, time.Millisecond,
		"expected cache to be auto-updated with DS list length 2")

	// adding mock jobs to jobsDB-1
	// TODO: we should add them to jobsDB-2, but currently it is not possible to do that until we implement the next phase
	err = jobsDB1.Store(context.Background(), generateJobs(2))
	require.NoError(t, err)

	// triggerAddNewDS2 to trigger jobsDB-2 to add new DS after refreshing cache
	triggerAddNewDS2 <- time.Now()
	require.Eventually(
		t,
		func() bool {
			return len(jobsDB2.getDSList()) == 3
		},
		10*time.Second, time.Millisecond,
		"expected jobsDB2 to be refresh the cache before adding new DS, to get to know about the DS-2 already present & hence add DS-3")

	// adding mock jobs to jobsDB-2
	err = jobsDB2.Store(context.Background(), generateJobs(2))
	require.NoError(t, err)

	go func() {
		triggerAddNewDS1 <- time.Now()
	}()
	go func() {
		triggerAddNewDS2 <- time.Now()
	}()
	var dsLen1, dsLen2 int
	require.Eventually(
		t,
		func() bool {
			dsLen1 = len(jobsDB1.getDSList())
			dsLen2 = len(jobsDB2.getDSList())
			return dsLen1 == 4 && dsLen2 == 4
		},
		time.Second, time.Millisecond,
		"expected only one DS to be added, even though both jobsDB-1 & jobsDB-2 are triggered to add new DS (dsLen1: %d, dsLen2: %d)", dsLen1, dsLen2)
}

func TestThreadSafeJobStorage(t *testing.T) {
	_ = startPostgres(t)

	t.Run("verify that `pgErrorCodeTableReadonly` exception is triggered, if we try to insert in any DS other than latest.", func(t *testing.T) {
		triggerAddNewDS := make(chan time.Time)
		c := config.New()
		c.Set("JobsDB.maxDSSize", 1)
		statStore, err := memstats.New()
		require.NoError(t, err)
		jobsDB := &Handle{
			TriggerAddNewDS: func() <-chan time.Time {
				return triggerAddNewDS
			},
			config: c,
			stats:  statStore,
		}
		err = jobsDB.Setup(ReadWrite, true, strings.ToLower(rsRand.String(5)))
		require.NoError(t, err)
		defer jobsDB.TearDown()
		require.Equal(t, 1, len(jobsDB.getDSList()), "expected cache to be auto-updated with DS list length 1")

		generateJobs := func(numOfJob int) []*JobT {
			customVal := "MOCKDS"
			js := make([]*JobT, numOfJob)
			for i := 0; i < numOfJob; i++ {
				js[i] = &JobT{
					Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
					EventPayload: []byte(`{"testKey":"testValue"}`),
					UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
					UUID:         uuid.New(),
					CustomVal:    customVal,
					EventCount:   1,
				}
			}
			return js
		}

		// adding mock jobs to jobsDB
		jobs := generateJobs(2)
		err = jobsDB.Store(context.Background(), jobs)
		require.NoError(t, err)

		// triggerAddNewDS to trigger jobsDB to add new DS
		triggerAddNewDS <- time.Now()
		require.Eventually(
			t,
			func() bool {
				return len(jobsDB.getDSList()) == 2
			},
			time.Second*5, time.Millisecond,
			"expected number of tables to be 2")
		ds := jobsDB.getDSList()
		sqlStatement := fmt.Sprintf(`INSERT INTO %q (uuid, user_id, custom_val, parameters, event_payload, workspace_id)
										   VALUES ($1, $2, $3, $4, $5, $6) RETURNING job_id`, ds[0].JobTable)
		stmt, err := jobsDB.dbHandle.Prepare(sqlStatement)
		require.NoError(t, err)
		defer stmt.Close()
		_, err = stmt.Exec(jobs[0].UUID, jobs[0].UserID, jobs[0].CustomVal, string(jobs[0].Parameters), string(jobs[0].EventPayload), jobs[0].WorkspaceId)
		require.Error(t, err, "expected error as trigger is set on DS")
		require.Equal(t, "pq: table is readonly", err.Error())
		var e *pq.Error
		errors.As(err, &e)
		require.EqualValues(t, e.Code, pgErrorCodeTableReadonly)
	})

	t.Run(`verify that even if jobsDB instance is unaware of new DS addition by other jobsDB instance.
	 And, it tries to Store() in postgres, then the exception thrown is handled properly & DS cache is refreshed`, func(t *testing.T) {
		c := config.New()
		c.Set("JobsDB.maxDSSize", 1)

		triggerRefreshDS := make(chan time.Time)
		triggerAddNewDS1 := make(chan time.Time)

		statStore, err := memstats.New()
		require.NoError(t, err)

		// jobsDB-1 setup
		jobsDB1 := &Handle{
			TriggerAddNewDS: func() <-chan time.Time {
				return triggerAddNewDS1
			},
			config: c,
			stats:  statStore,
		}
		clearAllDS := true
		prefix := strings.ToLower(rsRand.String(5))
		// setting clearAllDS to true to clear all DS, since we are using the same postgres as previous test.
		err = jobsDB1.Setup(ReadWrite, true, prefix)
		require.NoError(t, err)
		defer jobsDB1.TearDown()
		require.Equal(t, 1, len(jobsDB1.getDSList()), "expected cache to be auto-updated with DS list length 1")

		// jobsDB-2 setup
		triggerAddNewDS2 := make(chan time.Time)
		jobsDB2 := &Handle{
			TriggerAddNewDS: func() <-chan time.Time {
				return triggerAddNewDS2
			},
			TriggerRefreshDS: func() <-chan time.Time {
				return triggerRefreshDS
			},
			config: c,
			stats:  statStore,
		}
		err = jobsDB2.Setup(ReadWrite, !clearAllDS, prefix)
		require.NoError(t, err)
		defer jobsDB2.TearDown()
		require.Equal(t, 1, len(jobsDB2.getDSList()), "expected cache to be auto-updated with DS list length 1")

		// jobsDB-3 setup
		triggerAddNewDS3 := make(chan time.Time)
		jobsDB3 := &Handle{
			TriggerAddNewDS: func() <-chan time.Time {
				return triggerAddNewDS3
			},
			TriggerRefreshDS: func() <-chan time.Time {
				return triggerRefreshDS
			},
			config: c,
			stats:  statStore,
		}
		err = jobsDB3.Setup(ReadWrite, !clearAllDS, prefix)
		require.NoError(t, err)
		defer jobsDB3.TearDown()
		require.Equal(t, 1, len(jobsDB3.getDSList()), "expected cache to be auto-updated with DS list length 1")

		generateJobs := func(numOfJob int) []*JobT {
			customVal := "MOCKDS"
			js := make([]*JobT, numOfJob)
			for i := 0; i < numOfJob; i++ {
				js[i] = &JobT{
					Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
					EventPayload: []byte(`{"testKey":"testValue"}`),
					UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
					UUID:         uuid.New(),
					CustomVal:    customVal,
					EventCount:   1,
				}
			}
			return js
		}

		// adding mock jobs to jobsDB-1
		err = jobsDB1.Store(context.Background(), generateJobs(2))
		require.NoError(t, err)

		// triggerAddNewDS1 to trigger jobsDB-1 to add new DS
		triggerAddNewDS1 <- time.Now()
		require.Eventually(
			t,
			func() bool {
				return len(jobsDB1.getDSList()) == 2
			},
			10*time.Second, time.Millisecond,
			"expected cache to be auto-updated with DS list length 2")

		require.Equal(t, 1, len(jobsDB2.getDSList()), "expected jobsDB2 to still have a list length of 1")
		err = jobsDB2.Store(context.Background(), generateJobs(2))
		require.NoError(t, err)
		require.Equal(t, 2, len(jobsDB2.getDSList()), "expected jobsDB2 to have refreshed its ds list")

		require.Equal(t, 1, len(jobsDB3.getDSList()), "expected jobsDB3 to still have a list length of 1")
		errorsMap := jobsDB3.StoreEachBatchRetry(context.Background(), [][]*JobT{generateJobs(2)})
		require.Equal(t, 0, len(errorsMap))

		require.Equal(t, 2, len(jobsDB3.getDSList()), "expected jobsDB3 to have refreshed its ds list")

		// since DS-2 is added, if storing jobs from jobsDB-2, should automatically add DS-2. So, both DS-1 and DS-2 should have 2 jobs
		row := jobsDB2.dbHandle.QueryRow(fmt.Sprintf("select count(*) from %q", jobsDB2.getDSList()[0].JobTable))
		var count int
		err = row.Scan(&count)
		require.NoError(t, err, "expected no error while scanning rows")
		require.Equal(t, 2, count, "expected 2 jobs in DS-1")

		row = jobsDB1.dbHandle.QueryRow(fmt.Sprintf("select count(*) from %q", jobsDB1.getDSList()[1].JobTable))
		err = row.Scan(&count)
		require.NoError(t, err, "expected no error while scanning rows")
		require.Equal(t, 4, count, "expected 4 jobs in DS-2")
	})
}

func TestCacheScenarios(t *testing.T) {
	_ = startPostgres(t)

	customVal := "CUSTOMVAL"
	generateJobs := func(numOfJob int, destinationID string) []*JobT {
		js := make([]*JobT, numOfJob)
		for i := 0; i < numOfJob; i++ {
			js[i] = &JobT{
				Parameters:   []byte(fmt.Sprintf(`{"batch_id":1,"source_id":"sourceID","destination_id":%q}`, destinationID)),
				EventPayload: []byte(`{"testKey":"testValue"}`),
				UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
				UUID:         uuid.New(),
				CustomVal:    customVal,
				EventCount:   1,
			}
		}
		return js
	}

	checkDSLimitJobs := func(t *testing.T, limit int) []*JobT {
		maxDSSize := 1
		c := config.New()
		c.Set("JobsDB.maxDSSize", maxDSSize)
		statsStore, err := memstats.New()
		require.NoError(t, err)

		var dbWithOneLimit *Handle
		triggerAddNewDS := make(chan time.Time)
		if limit > 0 {
			dbWithOneLimit = NewForReadWrite(
				"cache",
				WithDSLimit(config.SingleValueLoader(limit)),
				WithConfig(c),
				WithStats(statsStore),
			)
		} else {
			dbWithOneLimit = NewForReadWrite(
				"cache",
				WithConfig(c),
				WithStats(statsStore),
			)
		}
		dbWithOneLimit.TriggerAddNewDS = func() <-chan time.Time {
			return triggerAddNewDS
		}

		prefix := strings.ToLower(rsRand.String(5))
		err = dbWithOneLimit.Setup(ReadWrite, false, prefix)
		require.NoError(t, err)
		require.Equal(t, 1, len(dbWithOneLimit.getDSList()), "expected cache to be auto-updated with DS list length 1")
		defer dbWithOneLimit.TearDown()

		err = dbWithOneLimit.Store(context.Background(), generateJobs(2, ""))
		require.NoError(t, err)

		res, err := dbWithOneLimit.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 100})
		require.NoError(t, err)
		require.Equal(t, 2, len(res.Jobs))

		triggerAddNewDS <- time.Now()
		require.Eventually(
			t,
			func() bool {
				return len(dbWithOneLimit.getDSList()) == 2
			},
			time.Second, time.Millisecond,
			"expected cache to be auto-updated with DS list length 2")

		require.NoError(t, dbWithOneLimit.Store(context.Background(), generateJobs(3, "")))

		res, err = dbWithOneLimit.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 100})
		require.NoError(t, err)
		fmt.Println("res jobs:", len(res.Jobs))
		return res.Jobs
	}

	t.Run("Test cache with ds limit as one", func(t *testing.T) {
		limit := 1
		jobs := checkDSLimitJobs(t, limit)
		fmt.Println("jobs:", jobs)
		require.Equal(t, 2, len(jobs)) // Should return only 2 jobs since ds limit is 1
	})

	t.Run("Test cache with no ds limit i.e. using default limit", func(t *testing.T) {
		limit := -1
		jobs := checkDSLimitJobs(t, limit)
		require.Equal(t, 5, len(jobs)) // Should return all jobs since there is no ds limit
	})

	t.Run("Test cache with 1 writer and 1 reader jobsdb (gateway, processor scenario)", func(t *testing.T) {
		gwDB := NewForWrite("gw_cache")
		require.NoError(t, gwDB.Start())
		defer gwDB.TearDown()

		gwDBForProcessor := NewForRead("gw_cache")
		require.NoError(t, gwDBForProcessor.Start())
		defer gwDBForProcessor.TearDown()

		res, err := gwDB.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 100})
		require.NoError(t, err)
		require.Equal(t, 0, len(res.Jobs), "gwDB should report 0 unprocessed jobs")
		res, err = gwDBForProcessor.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 100})
		require.NoError(t, err)
		require.Equal(t, 0, len(res.Jobs), "gwDBForProcessor should report 0 unprocessed jobs")

		require.NoError(t, gwDB.Store(context.Background(), generateJobs(2, "")))

		res, err = gwDBForProcessor.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 100})
		require.NoError(t, err)
		require.Equal(t, 2, len(res.Jobs), "gwDBForProcessor should report 2 unprocessed jobs since we added 2 jobs through gwDB")
		res, err = gwDB.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 100})
		require.NoError(t, err)
		require.Equal(t, 2, len(res.Jobs), "gwDB should report 2 unprocessed jobs since we added 2 jobs through gwDB")
	})

	t.Run("Test cache with and without using parameter filters", func(t *testing.T) {
		jobsDB := NewForReadWrite("params_cache")
		require.NoError(t, jobsDB.Start())
		defer jobsDB.TearDown()

		destinationID := "destinationID"

		res, err := jobsDB.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 100})
		require.NoError(t, err)
		require.Equal(t, 0, len(res.Jobs), "jobsDB should report 0 unprocessed jobs when not using parameter filters")
		res, err = jobsDB.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, ParameterFilters: []ParameterFilterT{{Name: "destination_id", Value: destinationID}}, JobsLimit: 100})
		require.NoError(t, err)
		require.Equal(t, 0, len(res.Jobs), "jobsDB should report 0 unprocessed jobs when using destination_id in parameter filters")

		require.NoError(t, jobsDB.Store(context.Background(), generateJobs(2, "")))
		res, err = jobsDB.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 100})
		require.NoError(t, err)
		require.Equal(t, 2, len(res.Jobs), "jobsDB should report 2 unprocessed jobs when not using parameter filters, after we added 2 jobs")
		res, err = jobsDB.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, ParameterFilters: []ParameterFilterT{{Name: "destination_id", Value: destinationID}}, JobsLimit: 100})
		require.NoError(t, err)
		require.Equal(t, 0, len(res.Jobs), "jobsDB should report 0 unprocessed jobs when using destination_id in parameter filters, after we added 2 jobs but for another destination_id")

		require.NoError(t, jobsDB.Store(context.Background(), generateJobs(2, destinationID)))
		res, err = jobsDB.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 100})
		require.NoError(t, err)
		require.Equal(t, 4, len(res.Jobs), "jobsDB should report 4 unprocessed jobs when not using parameter filters, after we added 2 more jobs")
		res, err = jobsDB.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, ParameterFilters: []ParameterFilterT{{Name: "destination_id", Value: destinationID}}, JobsLimit: 100})
		require.NoError(t, err)
		require.Equal(t, 2, len(res.Jobs), "jobsDB should report 2 unprocessed jobs when using destination_id in parameter filters, after we added 2 jobs for this destination_id")
	})

	t.Run("Test cache with two parameter filters (destination_id & source_id)", func(t *testing.T) {
		jobsDB := NewForReadWrite("two_params_cache")
		require.NoError(t, jobsDB.Start())
		defer jobsDB.TearDown()

		destinationID := "destinationID"

		res, err := jobsDB.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, ParameterFilters: []ParameterFilterT{{Name: "destination_id", Value: destinationID}, {Name: "source_id", Value: "sourceID"}}, JobsLimit: 100})
		require.NoError(t, err)
		require.Equal(t, 0, len(res.Jobs), "jobsDB should report 0 unprocessed jobs when using both destination_id and source_id as filters")

		require.NoError(t, jobsDB.Store(context.Background(), generateJobs(2, destinationID)))
		res, err = jobsDB.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, ParameterFilters: []ParameterFilterT{{Name: "destination_id", Value: destinationID}, {Name: "source_id", Value: "sourceID"}}, JobsLimit: 100})
		require.NoError(t, err)
		require.Equal(t, 2, len(res.Jobs), "jobsDB should report 2 unprocessed jobs when using both destination_id and source_id as filters, after we added 2 jobs")
	})

	t.Run("Test cache with two less parameter filters (destination_id & source_id)", func(t *testing.T) {
		previousParameterFilters := cacheParameterFilters
		cacheParameterFilters = []string{"destination_id", "source_id"}
		defer func() {
			cacheParameterFilters = previousParameterFilters
		}()
		jobsDB := NewForReadWrite("two_params_cache_query_less")
		require.NoError(t, jobsDB.Start())
		defer jobsDB.TearDown()

		destinationID := "destinationID"

		res, err := jobsDB.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, ParameterFilters: []ParameterFilterT{{Name: "destination_id", Value: destinationID}}, JobsLimit: 100})
		require.NoError(t, err)
		require.Equal(t, 0, len(res.Jobs), "jobsDB should report 0 unprocessed jobs when using destination_id as filter")

		require.NoError(t, jobsDB.Store(context.Background(), generateJobs(2, destinationID)))
		res, err = jobsDB.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, ParameterFilters: []ParameterFilterT{{Name: "destination_id", Value: destinationID}}, JobsLimit: 100})
		require.NoError(t, err)
		require.Equal(t, 2, len(res.Jobs), "jobsDB should report 2 unprocessed jobs when using destination_id as filter, after we added 2 jobs")
	})

	generateWorkspaceJobs := func(count int, workspaceID, destinationID string) []*JobT {
		jobs := generateJobs(count, destinationID)
		lo.ForEach(jobs, func(job *JobT, _ int) {
			job.WorkspaceId = workspaceID
		})
		return jobs
	}

	t.Run("supports with and without workspace query filters at the same time", func(t *testing.T) {
		jobDB := NewForReadWrite("workspace_query_filters")
		require.NoError(t, jobDB.Start())
		defer jobDB.TearDown()

		workspaceID := "workspaceID"
		require.NoError(
			t,
			jobDB.Store(
				context.Background(),
				generateWorkspaceJobs(2, workspaceID, "someDestinationID"),
			), "no error storing jobs",
		)

		res, err := jobDB.GetUnprocessed(
			context.Background(),
			GetQueryParams{
				CustomValFilters: []string{customVal},
				WorkspaceID:      workspaceID,
				JobsLimit:        100,
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			2,
			len(res.Jobs),
			"jobsDB should report 2 unprocessed jobs when using workspace_id as filter",
		)

		res, err = jobDB.GetUnprocessed(
			context.Background(),
			GetQueryParams{
				CustomValFilters: []string{customVal},
				ParameterFilters: []ParameterFilterT{{Name: "destination_id", Value: "someDestinationID"}},
				JobsLimit:        100,
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			2,
			len(res.Jobs),
			"jobsDB should report 2 unprocessed jobs without a workspace filter as well",
		)
	})
}

func TestSortDnumList(t *testing.T) {
	l := []string{"1", "0_1", "0_1_1", "-2"}
	sortDnumList(l)
	require.Equal(t, []string{"-2", "0_1", "0_1_1", "1"}, l)
}

func Test_GetAdvisoryLockForOperation_Unique(t *testing.T) {
	calculated := map[int64]string{}
	for _, operation := range []string{"add_ds", "migrate_ds", "schema_migrate"} {
		for _, prefix := range []string{"gw", "rt", "batch_rt", "proc_error"} {
			h := &Handle{tablePrefix: prefix}
			key := fmt.Sprintf("%s_%s", prefix, operation)
			advLock := h.getAdvisoryLockForOperation(operation)
			if dupKey, ok := calculated[advLock]; ok {
				t.Errorf("Duplicate advisory lock calculated for different keys %s and %s: %d", key, dupKey, advLock)
			}
			calculated[advLock] = key
		}
	}
}

func TestAfterJobIDQueryParam(t *testing.T) {
	_ = startPostgres(t)
	customVal := "CUSTOMVAL"
	generateJobs := func(numOfJob int, destinationID string) []*JobT {
		js := make([]*JobT, numOfJob)
		for i := 0; i < numOfJob; i++ {
			js[i] = &JobT{
				Parameters:   []byte(fmt.Sprintf(`{"batch_id":1,"source_id":"sourceID","destination_id":%q}`, destinationID)),
				EventPayload: []byte(`{"testKey":"testValue"}`),
				UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
				UUID:         uuid.New(),
				CustomVal:    customVal,
				EventCount:   1,
			}
		}
		return js
	}

	t.Run("get unprocessed", func(t *testing.T) {
		var jobsDB *Handle
		prefix := strings.ToLower(rsRand.String(5))
		destinationID := strings.ToLower(rsRand.String(5))
		jobsDB = NewForReadWrite(prefix)
		require.NoError(t, jobsDB.Start())
		defer jobsDB.TearDown()
		require.NoError(t, jobsDB.Store(context.Background(), generateJobs(2, destinationID)))
		unprocessed, err := jobsDB.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, ParameterFilters: []ParameterFilterT{{Name: "destination_id", Value: destinationID}}, JobsLimit: 100})
		require.NoError(t, err)
		require.Equal(t, 2, len(unprocessed.Jobs))

		unprocessed1, err := jobsDB.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 100, afterJobID: &unprocessed.Jobs[0].JobID})
		require.NoError(t, err)
		require.Equal(t, 1, len(unprocessed1.Jobs))

		unprocessed2, err := jobsDB.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 100, afterJobID: &unprocessed.Jobs[1].JobID})
		require.NoError(t, err)
		require.Equal(t, 0, len(unprocessed2.Jobs))
	})

	t.Run("get processed", func(t *testing.T) {
		var jobsDB *Handle
		prefix := strings.ToLower(rsRand.String(5))
		destinationID := strings.ToLower(rsRand.String(5))
		jobsDB = NewForReadWrite(prefix)
		require.NoError(t, jobsDB.Start())
		defer jobsDB.TearDown()
		require.NoError(t, jobsDB.Store(context.Background(), generateJobs(2, destinationID)))
		unprocessed, err := jobsDB.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, ParameterFilters: []ParameterFilterT{{Name: "destination_id", Value: destinationID}}, JobsLimit: 100})
		require.NoError(t, err)
		require.Equal(t, 2, len(unprocessed.Jobs))

		var statuses []*JobStatusT
		for _, job := range unprocessed.Jobs {
			statuses = append(statuses, &JobStatusT{
				JobID:         job.JobID,
				JobState:      Failed.State,
				AttemptNum:    1,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "202",
				ErrorResponse: []byte(`{"success":"OK"}`),
				Parameters:    []byte(`{}`),
				WorkspaceId:   defaultWorkspaceID,
			})
		}
		require.NoError(t, jobsDB.UpdateJobStatus(context.Background(), statuses, []string{customVal}, []ParameterFilterT{}))

		processed1, err := jobsDB.GetFailed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, ParameterFilters: []ParameterFilterT{{Name: "destination_id", Value: destinationID}}, JobsLimit: 100, afterJobID: &unprocessed.Jobs[0].JobID})
		require.NoError(t, err)
		require.Equal(t, 1, len(processed1.Jobs))

		processed2, err := jobsDB.GetFailed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 100, afterJobID: &unprocessed.Jobs[1].JobID})
		require.NoError(t, err)
		require.Equal(t, 0, len(processed2.Jobs))
	})
}

func TestDeleteExecuting(t *testing.T) {
	_ = startPostgres(t)
	customVal := "CUSTOMVAL"
	generateJobs := func(numOfJob int, destinationID string) []*JobT {
		js := make([]*JobT, numOfJob)
		for i := 0; i < numOfJob; i++ {
			js[i] = &JobT{
				Parameters:   []byte(fmt.Sprintf(`{"batch_id":1,"source_id":"sourceID","destination_id":%q}`, destinationID)),
				EventPayload: []byte(`{"testKey":"testValue"}`),
				UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
				UUID:         uuid.New(),
				CustomVal:    customVal,
				EventCount:   1,
			}
		}
		return js
	}

	var jobsDB *Handle
	prefix := strings.ToLower(rsRand.String(5))
	destinationID := strings.ToLower(rsRand.String(5))
	jobsDB = NewForReadWrite(prefix)
	require.NoError(t, jobsDB.Start())
	defer jobsDB.TearDown()
	require.NoError(t, jobsDB.Store(context.Background(), generateJobs(2, destinationID)))
	unprocessed, err := jobsDB.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, ParameterFilters: []ParameterFilterT{{Name: "destination_id", Value: destinationID}}, JobsLimit: 100})
	require.NoError(t, err)
	require.Equal(t, 2, len(unprocessed.Jobs))
	var statuses []*JobStatusT
	for _, job := range unprocessed.Jobs {
		statuses = append(statuses, &JobStatusT{
			JobID:         job.JobID,
			JobState:      Executing.State,
			AttemptNum:    1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "",
			ErrorResponse: []byte(`{}`),
			Parameters:    []byte(`{}`),
			WorkspaceId:   defaultWorkspaceID,
		})
	}
	require.NoError(t, jobsDB.UpdateJobStatus(context.Background(), statuses, []string{customVal}, []ParameterFilterT{}))
	unprocessed, err = jobsDB.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, ParameterFilters: []ParameterFilterT{{Name: "destination_id", Value: destinationID}}, JobsLimit: 100})
	require.NoError(t, err)
	require.Equal(t, 0, len(unprocessed.Jobs))

	jobsDB.DeleteExecuting()

	unprocessed, err = jobsDB.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, ParameterFilters: []ParameterFilterT{{Name: "destination_id", Value: destinationID}}, JobsLimit: 100})
	require.NoError(t, err)
	require.Equal(t, 2, len(unprocessed.Jobs))
}

func TestFailExecuting(t *testing.T) {
	_ = startPostgres(t)
	customVal := "CUSTOMVAL"
	generateJobs := func(numOfJob int, destinationID string) []*JobT {
		js := make([]*JobT, numOfJob)
		for i := 0; i < numOfJob; i++ {
			js[i] = &JobT{
				Parameters:   []byte(fmt.Sprintf(`{"batch_id":1,"source_id":"sourceID","destination_id":%q}`, destinationID)),
				EventPayload: []byte(`{"testKey":"testValue"}`),
				UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
				UUID:         uuid.New(),
				CustomVal:    customVal,
				EventCount:   1,
			}
		}
		return js
	}

	var jobsDB *Handle
	prefix := strings.ToLower(rsRand.String(5))
	destinationID := strings.ToLower(rsRand.String(5))
	jobsDB = NewForReadWrite(prefix)
	require.NoError(t, jobsDB.Start())
	defer jobsDB.TearDown()
	require.NoError(t, jobsDB.Store(context.Background(), generateJobs(2, destinationID)))
	unprocessed, err := jobsDB.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, ParameterFilters: []ParameterFilterT{{Name: "destination_id", Value: destinationID}}, JobsLimit: 100})
	require.NoError(t, err)
	require.Equal(t, 2, len(unprocessed.Jobs))

	var statuses []*JobStatusT
	for _, job := range unprocessed.Jobs {
		statuses = append(statuses, &JobStatusT{
			JobID:         job.JobID,
			JobState:      Executing.State,
			AttemptNum:    1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "",
			ErrorResponse: []byte(`{}`),
			Parameters:    []byte(`{}`),
			WorkspaceId:   defaultWorkspaceID,
		})
	}
	require.NoError(t, jobsDB.UpdateJobStatus(context.Background(), statuses, []string{customVal}, []ParameterFilterT{}))

	unprocessed, err = jobsDB.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, ParameterFilters: []ParameterFilterT{{Name: "destination_id", Value: destinationID}}, JobsLimit: 100})
	require.NoError(t, err)
	require.Equal(t, 0, len(unprocessed.Jobs))

	jobsDB.FailExecuting()

	unprocessed, err = jobsDB.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, ParameterFilters: []ParameterFilterT{{Name: "destination_id", Value: destinationID}}, JobsLimit: 100})
	require.NoError(t, err)
	require.Equal(t, 0, len(unprocessed.Jobs))

	failed, err := jobsDB.GetFailed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, ParameterFilters: []ParameterFilterT{{Name: "destination_id", Value: destinationID}}, JobsLimit: 100})
	require.NoError(t, err)
	require.Equal(t, 2, len(failed.Jobs))
}

func TestMaxAgeCleanup(t *testing.T) {
	pgContainer := startPostgres(t)
	customVal := "CUSTOMVAL"
	workspaceID := "workspaceID"
	generateJobs := func(numOfJob int, destinationID string) []*JobT {
		js := make([]*JobT, numOfJob)
		for i := 0; i < numOfJob; i++ {
			js[i] = &JobT{
				Parameters: []byte(fmt.Sprintf(
					`{"batch_id":1,"source_id":"sourceID","destination_id":%q}`,
					destinationID,
				)),
				EventPayload: []byte(`{"testKey":"testValue"}`),
				UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
				UUID:         uuid.New(),
				CustomVal:    customVal,
				EventCount:   1,
				WorkspaceId:  workspaceID,
			}
		}
		return js
	}

	destinationID := strings.ToLower(rsRand.String(5))
	t.Setenv("RSERVER_JOBS_DB_JOB_MAX_AGE", "1ms")
	jobsDB := &Handle{dbHandle: pgContainer.DB}
	tablePrefix := strings.ToLower(rsRand.String(5))
	require.NoError(t, jobsDB.Setup(
		ReadWrite,
		true,
		tablePrefix,
	))

	// run cleanup once with an empty db
	require.NoError(t, jobsDB.doCleanup(context.Background()))

	// store some jobs
	require.NoError(
		t,
		jobsDB.Store(
			context.Background(),
			generateJobs(2, destinationID),
		),
	)

	unprocessed, err := jobsDB.GetUnprocessed(
		context.Background(),
		GetQueryParams{
			CustomValFilters: []string{customVal},
			ParameterFilters: []ParameterFilterT{
				{Name: "destination_id", Value: destinationID},
			},
			JobsLimit: 100,
		})
	require.NoError(t, err)
	require.Equal(t, 2, len(unprocessed.Jobs))

	// store some journal entries
	timeDelay := time.Duration(-12) * time.Hour * 24
	t.Log(time.Now().Add(timeDelay))
	_, err = jobsDB.dbHandle.Exec(
		fmt.Sprintf(`insert into %s_journal (operation, done, operation_payload, start_time, owner)
				values ($1, $2, $3, $4, $5) returning id`, tablePrefix),
		addDSOperation,
		true,
		[]byte(`{"testKey":"testValue"}`),
		time.Now().Add(timeDelay),
		"",
	)
	require.NoError(t, err)

	// start again to trigger cleanup
	jobsDB.TearDown()
	jobsDB = &Handle{dbHandle: pgContainer.DB}
	require.NoError(t, jobsDB.Setup(
		ReadWrite,
		false,
		tablePrefix,
	))
	defer jobsDB.TearDown()

	abortedJobs, err := jobsDB.GetAborted(
		context.Background(),
		GetQueryParams{
			CustomValFilters: []string{customVal},
			ParameterFilters: []ParameterFilterT{
				{Name: "destination_id", Value: destinationID},
			},
			JobsLimit: 100,
		},
	)
	require.NoError(t, err)
	require.Equal(t, 2, len(abortedJobs.Jobs))

	unprocessed, err = jobsDB.GetUnprocessed(
		context.Background(),
		GetQueryParams{
			CustomValFilters: []string{customVal},
			ParameterFilters: []ParameterFilterT{
				{Name: "destination_id", Value: destinationID},
			},
			JobsLimit: 100,
		},
	)
	require.NoError(t, err)
	require.Equal(t, 0, len(unprocessed.Jobs))

	var journalEntryCount int
	err = jobsDB.dbHandle.QueryRow(`select count(*) from ` + tablePrefix + `_journal`).Scan(&journalEntryCount)
	require.NoError(t, err)
	require.Equal(t, 1, journalEntryCount) // one from jd.Setup(addDS) - the one added during the test is cleaned up
}

func TestConstructParameterJSONQuery(t *testing.T) {
	q := constructParameterJSONQuery("alias", []ParameterFilterT{{Name: "name", Value: "value"}})
	require.Equal(t, `(alias.parameters->>'name'='value')`, q)
}

func TestGetActiveWorkspaces(t *testing.T) {
	_ = startPostgres(t)
	c := config.New()
	c.Set("JobsDB.maxDSSize", 1)
	statStore, err := memstats.New()
	require.NoError(t, err)
	triggerAddNewDS := make(chan time.Time)
	jobsDB := &Handle{
		TriggerAddNewDS: func() <-chan time.Time {
			return triggerAddNewDS
		},
		config: c,
		stats:  statStore,
	}
	err = jobsDB.Setup(ReadWrite, true, strings.ToLower(rsRand.String(5)))
	require.NoError(t, err)
	defer jobsDB.TearDown()

	require.Equal(t, 1, len(jobsDB.getDSList()))
	customVal := "MOCKDS"
	generateJobs := func(workspaceID string, numOfJob int) []*JobT {
		js := make([]*JobT, numOfJob)
		for i := 0; i < numOfJob; i++ {
			js[i] = &JobT{
				WorkspaceId:  workspaceID,
				Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
				EventPayload: []byte(`{"testKey":"testValue"}`),
				UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
				UUID:         uuid.New(),
				CustomVal:    customVal,
				EventCount:   1,
			}
		}
		return js
	}

	// adding mock jobs to jobsDB
	jobs := generateJobs("ws-1", 2)
	err = jobsDB.Store(context.Background(), jobs)
	require.NoError(t, err)

	activeWorkspaces, err := jobsDB.GetActiveWorkspaces(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, activeWorkspaces, 1)
	require.ElementsMatch(t, []string{"ws-1"}, activeWorkspaces)

	activeWorkspaces, err = jobsDB.GetActiveWorkspaces(context.Background(), customVal)
	require.NoError(t, err)
	require.Len(t, activeWorkspaces, 1)
	require.ElementsMatch(t, []string{"ws-1"}, activeWorkspaces)

	activeWorkspaces, err = jobsDB.GetActiveWorkspaces(context.Background(), customVal+"_other")
	require.NoError(t, err)
	require.Len(t, activeWorkspaces, 0)

	// triggerAddNewDS to trigger jobsDB to add new DS
	triggerAddNewDS <- time.Now()
	require.Eventually(
		t,
		func() bool {
			t.Logf("tables %d", len(jobsDB.getDSList()))
			return len(jobsDB.getDSList()) == 2
		},
		time.Second*5, time.Millisecond,
		"expected number of tables to be 2")

	jobs = generateJobs("ws-2", 2)
	err = jobsDB.Store(context.Background(), jobs)
	require.NoError(t, err)

	activeWorkspaces, err = jobsDB.GetActiveWorkspaces(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, activeWorkspaces, 2)
	require.ElementsMatch(t, []string{"ws-1", "ws-2"}, activeWorkspaces)

	activeWorkspaces, err = jobsDB.GetActiveWorkspaces(context.Background(), customVal)
	require.NoError(t, err)
	require.Len(t, activeWorkspaces, 2)
	require.ElementsMatch(t, []string{"ws-1", "ws-2"}, activeWorkspaces)

	activeWorkspaces, err = jobsDB.GetActiveWorkspaces(context.Background(), customVal+"_other")
	require.NoError(t, err)
	require.Len(t, activeWorkspaces, 0)

	triggerAddNewDS <- time.Now()
	require.Eventually(
		t,
		func() bool {
			return len(jobsDB.getDSList()) == 3
		},
		time.Second*5, time.Millisecond,
		"expected number of tables to be 3")

	jobs = generateJobs("ws-3", 2)
	err = jobsDB.Store(context.Background(), jobs)
	require.NoError(t, err)

	res, err := jobsDB.GetUnprocessed(context.Background(), GetQueryParams{WorkspaceID: "ws-3", JobsLimit: 10})
	require.NoError(t, err)
	statuses := lo.Map(res.Jobs, func(job *JobT, _ int) *JobStatusT {
		return &JobStatusT{
			JobID:       job.JobID,
			JobState:    Succeeded.State,
			AttemptNum:  1,
			WorkspaceId: "ws-3",
		}
	})
	require.NoError(t, jobsDB.UpdateJobStatus(context.Background(), statuses, []string{}, []ParameterFilterT{}))

	activeWorkspaces, err = jobsDB.GetActiveWorkspaces(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, activeWorkspaces, 3)
	require.ElementsMatch(t, []string{"ws-1", "ws-2", "ws-3"}, activeWorkspaces)
}

func TestGetDistinctParameterValues(t *testing.T) {
	_ = startPostgres(t)
	c := config.New()
	c.Set("JobsDB.maxDSSize", 1)
	statStore, err := memstats.New()
	require.NoError(t, err)
	triggerAddNewDS := make(chan time.Time)
	jobsDB := &Handle{
		TriggerAddNewDS: func() <-chan time.Time {
			return triggerAddNewDS
		},
		config: c,
		stats:  statStore,
	}
	err = jobsDB.Setup(ReadWrite, true, strings.ToLower(rsRand.String(5)))
	require.NoError(t, err)
	defer jobsDB.TearDown()

	require.Equal(t, 1, len(jobsDB.getDSList()))

	generateJobs := func(paramValue string, numOfJob int) []*JobT {
		customVal := "MOCKDS"
		js := make([]*JobT, numOfJob)
		for i := 0; i < numOfJob; i++ {
			js[i] = &JobT{
				WorkspaceId:  "workspace",
				Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":"", "param":"` + paramValue + `"}`),
				EventPayload: []byte(`{"testKey":"testValue"}`),
				UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
				UUID:         uuid.New(),
				CustomVal:    customVal,
				EventCount:   1,
			}
		}
		return js
	}

	// adding mock jobs to jobsDB
	jobs := generateJobs("param-1", 2)
	err = jobsDB.Store(context.Background(), jobs)
	require.NoError(t, err)
	t.Log("stored jobs")

	parameterValues, err := jobsDB.GetDistinctParameterValues(context.Background(), "param")
	require.NoError(t, err)
	require.Len(t, parameterValues, 1)
	require.ElementsMatch(t, []string{"param-1"}, parameterValues)

	// triggerAddNewDS to trigger jobsDB to add new DS
	triggerAddNewDS <- time.Now()
	require.Eventually(
		t,
		func() bool {
			t.Logf("tables %d", len(jobsDB.getDSList()))
			return len(jobsDB.getDSList()) == 2
		},
		time.Second*5, time.Millisecond,
		"expected number of tables to be 2")

	jobs = generateJobs("param-2", 2)
	err = jobsDB.Store(context.Background(), jobs)
	require.NoError(t, err)
	t.Log("stored jobs again")

	parameterValues, err = jobsDB.GetDistinctParameterValues(context.Background(), "param")
	require.NoError(t, err)
	require.Len(t, parameterValues, 2)
	require.ElementsMatch(t, []string{"param-1", "param-2"}, parameterValues)

	triggerAddNewDS <- time.Now()
	require.Eventually(
		t,
		func() bool {
			return len(jobsDB.getDSList()) == 3
		},
		time.Second*5, time.Millisecond,
		"expected number of tables to be 3")

	jobs = generateJobs("param-3", 2)
	err = jobsDB.Store(context.Background(), jobs)
	require.NoError(t, err)
	t.Log("and stored jobs again")

	res, err := jobsDB.GetUnprocessed(context.Background(), GetQueryParams{ParameterFilters: []ParameterFilterT{{Name: "param", Value: "param-3"}}, JobsLimit: 10})
	require.NoError(t, err)
	statuses := lo.Map(res.Jobs, func(job *JobT, _ int) *JobStatusT {
		return &JobStatusT{
			JobID:       job.JobID,
			JobState:    Succeeded.State,
			AttemptNum:  1,
			WorkspaceId: "workspace",
		}
	})
	require.NoError(t, jobsDB.UpdateJobStatus(context.Background(), statuses, []string{}, []ParameterFilterT{}))

	parameterValues, err = jobsDB.GetDistinctParameterValues(context.Background(), "param")
	require.NoError(t, err)
	require.Len(t, parameterValues, 3)
	require.ElementsMatch(t, []string{"param-1", "param-2", "param-3"}, parameterValues)
}

type testingT interface {
	Errorf(format string, args ...interface{})
	FailNow()
	Setenv(key, value string)
	Log(...interface{})
	Logf(format string, args ...interface{})
	Cleanup(func())
	Failed() bool
}

// startPostgres starts a postgres container and (re)initializes global vars
func startPostgres(t testingT) *postgres.Resource {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	postgresContainer, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	t.Setenv("LOG_LEVEL", "DEBUG")
	t.Setenv("JOBS_DB_DB_NAME", postgresContainer.Database)
	t.Setenv("JOBS_DB_NAME", postgresContainer.Database)
	t.Setenv("JOBS_DB_HOST", postgresContainer.Host)
	t.Setenv("JOBS_DB_PORT", postgresContainer.Port)
	t.Setenv("JOBS_DB_USER", postgresContainer.User)
	t.Setenv("JOBS_DB_PASSWORD", postgresContainer.Password)
	initJobsDB()
	return postgresContainer
}

func initJobsDB() {
	config.Reset()
	logger.Reset()
	admin.Init()
	misc.Init()
}
