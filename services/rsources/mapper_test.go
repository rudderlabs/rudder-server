package rsources

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMapToStatus(t *testing.T) {
	for _, e := range testCases {
		res := statusFromQueryResult("jobRunId", e.in)
		sort.Slice(res.TasksStatus, func(i, j int) bool {
			return res.TasksStatus[i].ID < res.TasksStatus[j].ID
		})
		// further sorting would be needed in case there's multiple stats inside a task or a source
		require.Equal(t, e.out, res, "test case: "+e.name)
	}
}

type testCase struct {
	name string
	in   map[JobTargetKey]Stats
	out  JobStatus
}

var testCases = []testCase{
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
		}, JobStatus{
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
		}},
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
		}, JobStatus{
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
		}, JobStatus{
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
		}, JobStatus{
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
