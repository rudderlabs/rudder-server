package jobsdb

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
)

func BenchmarkMigration(b *testing.B) {
	config.Reset()
	c := config.New()

	c.Set("LOG_LEVEL", "DEBUG")
	c.Set("DB.name", "jobsdb")
	c.Set("DB.host", "localhost")
	c.Set("DB.port", "6432")
	c.Set("DB.user", "rudder")
	c.Set("DB.password", "password")
	triggerAddNewDS := make(chan time.Time)
	triggerMigrateDS := make(chan time.Time)

	jobDB := Handle{
		TriggerAddNewDS: func() <-chan time.Time {
			return triggerAddNewDS
		},
		TriggerMigrateDS: func() <-chan time.Time {
			return triggerMigrateDS
		},
		config: c,
	}
	tablePrefix := "coemk"
	err := jobDB.Setup(
		ReadWrite,
		false,
		tablePrefix,
	)
	require.NoError(b, err)
	defer jobDB.TearDown()

	c.Set("JobsDB."+tablePrefix+"."+"maxDSRetention", "1ms")

	// for i := 0; i < 50; i++ {
	// 	customVal := rand.String(5)
	// 	jobs := genJobsWithJobID(defaultWorkspaceID, customVal, 100000, 1, int64(i*100000+1))
	// 	require.NoError(b, jobDB.Store(context.Background(), jobs))

	// 	require.NoError(
	// 		b,
	// 		jobDB.UpdateJobStatus(
	// 			context.Background(),
	// 			genJobStatuses(jobs[:90000], "executing"),
	// 			[]string{customVal},
	// 			[]ParameterFilterT{},
	// 		),
	// 	)
	// 	require.NoError(
	// 		b,
	// 		jobDB.UpdateJobStatus(
	// 			context.Background(),
	// 			genJobStatuses(jobs[:80000], "succeeded"),
	// 			[]string{customVal},
	// 			[]ParameterFilterT{},
	// 		),
	// 	)
	// 	triggerAddNewDS <- time.Now()
	// 	triggerAddNewDS <- time.Now()
	// 	b.Logf("finished %d", i)
	// }

	// jobDB.Stop()

	c.Set("JobsDB.maxMigrateOnce", "50")
	c.Set("JobsDB.maxMigrateDSProbe", "50")
	ctx := context.Background()
	b.ResetTimer()
	require.True(b, jobDB.dsListLock.RTryLockWithCtx(ctx))
	dsList := jobDB.getDSList()
	jobDB.dsListLock.RUnlock()
	b.Run("getMigrationList", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// migrateFrom, _, _, _ := jobDB.getMigrationList(ctx, dsList)
			migrateFrom, _, _, _ := jobDB.getMigrationList(dsList)
			b.Logf("found %d datasets eligible for migration", len(migrateFrom))
		}
	})
	// for i := 0; i < b.N; i++ {
	// 	migrateFrom, _, _, _ := jobDB.getMigrationList(dsList)
	// 	b.Logf("found %d datasets eligible for migration", len(migrateFrom))
	// }
}

func genJobsWithJobID(workspaceId, customVal string, jobCount, eventsPerJob int, jobID int64) []*JobT { // nolint: unparam
	js := make([]*JobT, jobCount)
	for i := range js {
		js[i] = &JobT{
			JobID:        jobID + int64(i),
			Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
			EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
			UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
			UUID:         uuid.New(),
			CustomVal:    customVal,
			EventCount:   eventsPerJob,
			WorkspaceId:  workspaceId,
		}
	}
	return js
}

func BenchmarkReadFile(b *testing.B) {
	fileName := "/tmp/jobs_2oyzX5Am1ch5fV817GpUSzNFXuD"
	for i := 0; i < b.N; i++ {
		_, err := ConcurrentReadFromFile(fileName, i%1300, 1024)
		require.NoError(b, err)
	}
}

func TestReadFile(t *testing.T) {
	payload, err := ConcurrentReadFromFile("/tmp/jobs_2oyzX5Am1ch5fV817GpUSzNFXuD", 0, 1127)
	require.NoError(t, err)
	require.Equal(t, `{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b31d9e47-c65f-4187-b605-c8cebc4e3037","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"receivedAt":"2024-11-17T20:47:18.580+05:30","request_ip":"[::1]","rudderId":"90ca6da0-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","timestamp":"2024-11-17T20:47:18.580+05:30","type":"track"}`, string(payload))
}

/*
goos: darwin
goarch: arm64
pkg: github.com/rudderlabs/rudder-server/jobsdb
cpu: Apple M1 Pro
=== RUN   BenchmarkWriteToFile
BenchmarkWriteToFile
RUDDER_TMPDIR not found, falling back to /tmp
BenchmarkWriteToFile-10              351           3099172 ns/op          812626 B/op       9029 allocs/op
PASS
ok      github.com/rudderlabs/rudder-server/jobsdb      2.046s
*/
func BenchmarkWriteToFile(b *testing.B) {
	payload := []byte(`{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b31d9e47-c65f-4187-b605-c8cebc4e3037","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"receivedAt":"2024-11-17T20:47:18.580+05:30","request_ip":"[::1]","rudderId":"90ca6da0-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","timestamp":"2024-11-17T20:47:18.580+05:30","type":"track"}`)
	for i := 0; i < b.N; i++ {
		jobs := make([]*JobT, 1000)
		for i := range jobs {
			jobs[i] = &JobT{
				Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
				EventPayload: payload,
			}
		}
		_, err := WriteToFile(jobs)
		require.NoError(b, err)
	}
}
