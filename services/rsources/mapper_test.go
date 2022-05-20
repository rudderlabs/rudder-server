package rsources

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatusFromQueryResult(t *testing.T) {
	t.Run("one task with one source with one destination, all completed", func(t *testing.T) {
		res := statusFromQueryResult("jobRunId", map[JobTargetKey]Stats{
			{
				SourceId:      "source_id",
				DestinationId: "destination_id",
				TaskRunId:     "task_run_id",
			}: {
				In:     10,
				Out:    4,
				Failed: 6,
			},
		},
		)
		expected := JobStatus{
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
		}
		require.Equal(t, expected, res)
	})

	t.Run("one task with one source with one destination, destination not completed", func(t *testing.T) {
		res := statusFromQueryResult("jobRunId", map[JobTargetKey]Stats{
			{
				SourceId:      "source_id",
				DestinationId: "destination_id",
				TaskRunId:     "task_run_id",
			}: {
				In:     10,
				Out:    3,
				Failed: 6,
			},
		})
		expected := JobStatus{
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
		}
		require.Equal(t, expected, res)
	})

	t.Run("one task with one source with one destination, source not completed", func(t *testing.T) {
		res := statusFromQueryResult("jobRunId", map[JobTargetKey]Stats{
			{
				SourceId:      "source_id",
				DestinationId: "destination_id",
				TaskRunId:     "task_run_id",
			}: {
				In:     10,
				Out:    4,
				Failed: 6,
			},
			{
				SourceId:  "source_id",
				TaskRunId: "task_run_id",
			}: {
				In:     10,
				Out:    3,
				Failed: 6,
			},
		})
		expected := JobStatus{
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
		}
		require.Equal(t, expected, res)
	})

	t.Run("two tasks with one source each (same id) and one destination each (same id), one task completed, other not", func(t *testing.T) {
		res := statusFromQueryResult("jobRunId", map[JobTargetKey]Stats{
			{
				SourceId:      "source_id",
				DestinationId: "destination_id",
				TaskRunId:     "task_run_id1",
			}: {
				In:     10,
				Out:    4,
				Failed: 6,
			},
			{
				SourceId:      "source_id",
				DestinationId: "destination_id",
				TaskRunId:     "task_run_id2",
			}: {
				In:     10,
				Out:    3,
				Failed: 6,
			},
		})

		sort.Slice(res.TasksStatus, func(i, j int) bool {
			return res.TasksStatus[i].ID < res.TasksStatus[j].ID
		})

		expected := JobStatus{
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
		}
		require.Equal(t, expected, res)
	})
}
