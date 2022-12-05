package jobsdb

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rudderlabs/rudder-server/jobsdb/prebackup"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/utils/bytesize"
	"github.com/stretchr/testify/require"
)

func TestMultiTenantHandleT_GetAllJobs(t *testing.T) {
	_ = startPostgres(t)
	maxDSSize := 2
	jobDB := MultiTenantHandleT{HandleT: &HandleT{MaxDSSize: &maxDSSize}}

	err := jobDB.Setup(ReadWrite, false, "rt", true, []prebackup.Handler{}, fileuploader.NewDefaultProvider())
	require.NoError(t, err, "expected no error while jobsDB setup")
	defer jobDB.TearDown()

	customVal := "MOCKDS"
	testWID := "testWorkspaceID"
	sampleTestJob1 := JobT{
		WorkspaceId:  testWID,
		Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.New(),
		CustomVal:    customVal,
	}
	sampleTestJob2 := JobT{
		WorkspaceId:  testWID,
		Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.New(),
		CustomVal:    customVal,
	}
	sampleTestJob3 := JobT{
		WorkspaceId:  testWID,
		Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.New(),
		CustomVal:    customVal,
	}

	workspaceCountMap := make(map[string]int)
	workspaceCountMap[testWID] = 1
	payloadLimit := 100 * bytesize.MB
	unprocessedListEmpty, err := jobDB.GetAllJobs(context.Background(), workspaceCountMap, GetQueryParamsT{
		CustomValFilters: []string{customVal},
		JobsLimit:        1,
		ParameterFilters: []ParameterFilterT{},
		PayloadSizeLimit: payloadLimit,
	}, 10, nil)
	require.NoError(t, err, "Error getting All jobs")
	require.Equal(t, 0, len(unprocessedListEmpty.Jobs))

	err = jobDB.Store(context.Background(), []*JobT{&sampleTestJob1, &sampleTestJob2, &sampleTestJob3})
	require.NoError(t, err)

	payloadLimit = 100 * bytesize.MB
	workspaceCountMap[testWID] = 3
	unprocessedList, err := jobDB.GetAllJobs(context.Background(), workspaceCountMap, GetQueryParamsT{
		CustomValFilters: []string{customVal},
		JobsLimit:        3,
		ParameterFilters: []ParameterFilterT{},
		PayloadSizeLimit: payloadLimit,
	}, 10, nil)
	require.NoError(t, err, "Error getting All jobs")
	require.Equal(t, 3, len(unprocessedList.Jobs))

	status1 := JobStatusT{
		JobID:         unprocessedList.Jobs[0].JobID,
		JobState:      "waiting",
		AttemptNum:    1,
		ExecTime:      time.Now(),
		RetryTime:     time.Now(),
		ErrorCode:     "202",
		ErrorResponse: []byte(`{"success":"OK"}`),
		Parameters:    []byte(`{}`),
		WorkspaceId:   "testWorkspace",
	}
	status2 := JobStatusT{
		JobID:         unprocessedList.Jobs[1].JobID,
		JobState:      "failed",
		AttemptNum:    1,
		ExecTime:      time.Now(),
		RetryTime:     time.Now(),
		ErrorCode:     "202",
		ErrorResponse: []byte(`{"success":"OK"}`),
		Parameters:    []byte(`{}`),
		WorkspaceId:   "testWorkspace",
	}

	err = jobDB.UpdateJobStatus(context.Background(), []*JobStatusT{&status1, &status2}, []string{customVal}, []ParameterFilterT{})
	require.NoError(t, err)

	payloadLimit = 100 * bytesize.MB
	workspaceCountMap[testWID] = 3
	jobs, err := jobDB.GetAllJobs(context.Background(), workspaceCountMap, GetQueryParamsT{
		CustomValFilters: []string{customVal},
		JobsLimit:        3,
		ParameterFilters: []ParameterFilterT{},
		PayloadSizeLimit: payloadLimit,
	}, 10, nil)
	require.NoError(t, err, "Error getting All jobs")
	require.Equal(t, 3, len(jobs.Jobs))
}
