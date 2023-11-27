package rsources

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatusFromQueryResult(t *testing.T) {
	type testCase struct {
		name string
		in   map[JobTargetKey]Stats
		out  JobStatus
	}

	testCases := []testCase{
		{
			"one task with one source with one destination, all completed",
			map[JobTargetKey]Stats{
				{
					SourceID:      "source_id",
					DestinationID: "destination_id",
					TaskRunID:     "task_run_id",
				}: {
					In:     10,
					Out:    4,
					Failed: 6,
				},
			},
			JobStatus{
				ID: "jobRunId",
				TasksStatus: []TaskStatus{
					{
						ID: "task_run_id",
						SourcesStatus: []SourceStatus{
							{
								ID:        "source_id",
								Completed: true,
								Stats:     Stats{},
								DestinationsStatus: []DestinationStatus{
									{
										ID:        "destination_id",
										Stats:     Stats{In: 10, Out: 4, Failed: 6},
										Completed: true,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			"one task with one source with one destination, destination not completed",
			map[JobTargetKey]Stats{
				{
					SourceID:      "source_id",
					DestinationID: "destination_id",
					TaskRunID:     "task_run_id",
				}: {
					In:     10,
					Out:    3,
					Failed: 6,
				},
			},
			JobStatus{
				ID: "jobRunId",
				TasksStatus: []TaskStatus{
					{
						ID: "task_run_id",
						SourcesStatus: []SourceStatus{
							{
								ID:        "source_id",
								Completed: false,
								Stats:     Stats{},
								DestinationsStatus: []DestinationStatus{
									{
										ID:        "destination_id",
										Stats:     Stats{In: 10, Out: 3, Failed: 6},
										Completed: false,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			"one task with one source with one destination, source not completed",
			map[JobTargetKey]Stats{
				{
					SourceID:      "source_id",
					DestinationID: "destination_id",
					TaskRunID:     "task_run_id",
				}: {
					In:     10,
					Out:    4,
					Failed: 6,
				},
				{
					SourceID:  "source_id",
					TaskRunID: "task_run_id",
				}: {
					In:     10,
					Out:    3,
					Failed: 6,
				},
			},
			JobStatus{
				ID: "jobRunId",
				TasksStatus: []TaskStatus{
					{
						ID: "task_run_id",
						SourcesStatus: []SourceStatus{
							{
								ID:        "source_id",
								Completed: false,
								Stats:     Stats{In: 10, Out: 3, Failed: 6},
								DestinationsStatus: []DestinationStatus{
									{
										ID:        "destination_id",
										Stats:     Stats{In: 10, Out: 4, Failed: 6},
										Completed: true,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			"two tasks with one source each (same id) and one destination each (same id), one task completed, other not",
			map[JobTargetKey]Stats{
				{
					SourceID:      "source_id",
					DestinationID: "destination_id",
					TaskRunID:     "task_run_id1",
				}: {
					In:     10,
					Out:    4,
					Failed: 6,
				},
				{
					SourceID:      "source_id",
					DestinationID: "destination_id",
					TaskRunID:     "task_run_id2",
				}: {
					In:     10,
					Out:    3,
					Failed: 6,
				},
			},
			JobStatus{
				ID: "jobRunId",
				TasksStatus: []TaskStatus{
					{
						ID: "task_run_id1",
						SourcesStatus: []SourceStatus{
							{
								ID:        "source_id",
								Completed: true,
								Stats:     Stats{},
								DestinationsStatus: []DestinationStatus{
									{
										ID:        "destination_id",
										Stats:     Stats{In: 10, Out: 4, Failed: 6},
										Completed: true,
									},
								},
							},
						},
					},
					{
						ID: "task_run_id2",
						SourcesStatus: []SourceStatus{
							{
								ID:        "source_id",
								Completed: false,
								Stats:     Stats{},
								DestinationsStatus: []DestinationStatus{
									{
										ID:        "destination_id",
										Stats:     Stats{In: 10, Out: 3, Failed: 6},
										Completed: false,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, e := range testCases {
		res := statusFromQueryResult("jobRunId", e.in)
		sort.Slice(res.TasksStatus, func(i, j int) bool {
			return res.TasksStatus[i].ID < res.TasksStatus[j].ID
		})
		// further sorting would be needed in case there's multiple stats inside a task or a source
		require.Equal(t, e.out, res, "test case: "+e.name)
	}
}

func TestFailedRecordsFromQueryResult(t *testing.T) {
	t.Run("no failed records", func(t *testing.T) {
		res := failedRecordsFromQueryResult("jobRunId", map[JobTargetKey][]json.RawMessage{})
		require.Equal(t, JobFailedRecords[json.RawMessage]{ID: "jobRunId"}, res, "should return an empty JobFailedRecords")
	})

	t.Run("failed records for single source", func(t *testing.T) {
		input := map[JobTargetKey][]json.RawMessage{
			{TaskRunID: "task-1", SourceID: "source-1"}: {[]byte(`"key1"`), []byte(`"key2"`)},
		}
		expected := JobFailedRecords[json.RawMessage]{
			ID: "jobRunId",
			Tasks: []TaskFailedRecords[json.RawMessage]{{
				ID: "task-1",
				Sources: []SourceFailedRecords[json.RawMessage]{{
					ID:      "source-1",
					Records: []json.RawMessage{[]byte(`"key1"`), []byte(`"key2"`)},
				}},
			}},
		}
		require.Equal(t, expected, failedRecordsFromQueryResult("jobRunId", input), "should return the exoected JobFailedRecords")
	})

	t.Run("failed records for single destination", func(t *testing.T) {
		input := map[JobTargetKey][]json.RawMessage{
			{TaskRunID: "task-1", SourceID: "source-1", DestinationID: "destination-1"}: {[]byte(`"key1"`), []byte(`"key2"`)},
		}
		expected := JobFailedRecords[json.RawMessage]{
			ID: "jobRunId",
			Tasks: []TaskFailedRecords[json.RawMessage]{{
				ID: "task-1",
				Sources: []SourceFailedRecords[json.RawMessage]{{
					ID: "source-1",
					Destinations: []DestinationFailedRecords[json.RawMessage]{{
						ID:      "destination-1",
						Records: []json.RawMessage{[]byte(`"key1"`), []byte(`"key2"`)},
					}},
				}},
			}},
		}
		require.Equal(t, expected, failedRecordsFromQueryResult("jobRunId", input), "should return the exoected JobFailedRecords")
	})

	t.Run("failed records for single source and destination", func(t *testing.T) {
		input := map[JobTargetKey][]json.RawMessage{
			{TaskRunID: "task-1", SourceID: "source-1"}:                                 {[]byte(`"key1"`), []byte(`"key2"`)},
			{TaskRunID: "task-1", SourceID: "source-1", DestinationID: "destination-1"}: {[]byte(`"key3"`), []byte(`"key4"`)},
		}
		expected := JobFailedRecords[json.RawMessage]{
			ID: "jobRunId",
			Tasks: []TaskFailedRecords[json.RawMessage]{{
				ID: "task-1",
				Sources: []SourceFailedRecords[json.RawMessage]{{
					ID:      "source-1",
					Records: []json.RawMessage{[]byte(`"key1"`), []byte(`"key2"`)},
					Destinations: []DestinationFailedRecords[json.RawMessage]{{
						ID:      "destination-1",
						Records: []json.RawMessage{[]byte(`"key3"`), []byte(`"key4"`)},
					}},
				}},
			}},
		}
		require.Equal(t, expected, failedRecordsFromQueryResult("jobRunId", input), "should return the exoected JobFailedRecords")
	})

	t.Run("failed records for two tasks with same source and destination", func(t *testing.T) {
		input := map[JobTargetKey][]json.RawMessage{
			{TaskRunID: "task-1", SourceID: "source-1"}:                                 {[]byte(`"key1"`), []byte(`"key2"`)},
			{TaskRunID: "task-1", SourceID: "source-1", DestinationID: "destination-1"}: {[]byte(`"key3"`), []byte(`"key4"`)},
			{TaskRunID: "task-2", SourceID: "source-1"}:                                 {[]byte(`"key5"`), []byte(`"key6"`)},
			{TaskRunID: "task-2", SourceID: "source-1", DestinationID: "destination-1"}: {[]byte(`"key7"`), []byte(`"key8"`)},
		}
		expected := JobFailedRecords[json.RawMessage]{
			ID: "jobRunId",
			Tasks: []TaskFailedRecords[json.RawMessage]{
				{
					ID: "task-1",
					Sources: []SourceFailedRecords[json.RawMessage]{{
						ID:      "source-1",
						Records: []json.RawMessage{[]byte(`"key1"`), []byte(`"key2"`)},
						Destinations: []DestinationFailedRecords[json.RawMessage]{{
							ID:      "destination-1",
							Records: []json.RawMessage{[]byte(`"key3"`), []byte(`"key4"`)},
						}},
					}},
				},
				{
					ID: "task-2",
					Sources: []SourceFailedRecords[json.RawMessage]{{
						ID:      "source-1",
						Records: []json.RawMessage{[]byte(`"key5"`), []byte(`"key6"`)},
						Destinations: []DestinationFailedRecords[json.RawMessage]{{
							ID:      "destination-1",
							Records: []json.RawMessage{[]byte(`"key7"`), []byte(`"key8"`)},
						}},
					}},
				},
			},
		}
		require.Equal(t, expected, failedRecordsFromQueryResult("jobRunId", input), "should return the exoected JobFailedRecords")
	})
}
