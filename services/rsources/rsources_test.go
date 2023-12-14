package rsources_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/services/rsources"
)

func TestCorruptedJobStatus(t *testing.T) {
	t.Run("Corrupted Source Stats", func(t *testing.T) {
		js := rsources.JobStatus{
			ID: "job-id",
			TasksStatus: []rsources.TaskStatus{
				{
					ID: "task-id",
					SourcesStatus: []rsources.SourceStatus{
						{
							ID:        "source-id",
							Completed: false,
							Stats: rsources.Stats{
								In:     2,
								Out:    3,
								Failed: 0,
							},
							DestinationsStatus: []rsources.DestinationStatus{
								{
									ID:        "destination-id",
									Completed: false,
									Stats: rsources.Stats{
										In:     3,
										Out:    2,
										Failed: 1,
									},
								},
							},
						},
					},
				},
			},
		}

		js.FixCorruptedStats(logger.NOP)
		require.EqualValues(t, true, js.TasksStatus[0].SourcesStatus[0].Completed)
		require.EqualValues(t, 3, js.TasksStatus[0].SourcesStatus[0].Stats.In)
		require.EqualValues(t, 3, js.TasksStatus[0].SourcesStatus[0].Stats.Out)
		require.EqualValues(t, 0, js.TasksStatus[0].SourcesStatus[0].Stats.Failed)

		require.EqualValues(t, true, js.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Completed)
		require.EqualValues(t, 3, js.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.In)
		require.EqualValues(t, 2, js.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.Out)
		require.EqualValues(t, 1, js.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.Failed)
	})

	t.Run("Corrupted Destination Stats", func(t *testing.T) {
		js := rsources.JobStatus{
			ID: "job-id",
			TasksStatus: []rsources.TaskStatus{
				{
					ID: "task-id",
					SourcesStatus: []rsources.SourceStatus{
						{
							ID:        "source-id",
							Completed: true,
							Stats: rsources.Stats{
								In:     2,
								Out:    2,
								Failed: 0,
							},
							DestinationsStatus: []rsources.DestinationStatus{
								{
									ID:        "destination-id",
									Completed: false,
									Stats: rsources.Stats{
										In:     2,
										Out:    2,
										Failed: 1,
									},
								},
							},
						},
					},
				},
			},
		}

		js.FixCorruptedStats(logger.NOP)

		require.EqualValues(t, true, js.TasksStatus[0].SourcesStatus[0].Completed)
		require.EqualValues(t, 2, js.TasksStatus[0].SourcesStatus[0].Stats.In)
		require.EqualValues(t, 2, js.TasksStatus[0].SourcesStatus[0].Stats.Out)
		require.EqualValues(t, 0, js.TasksStatus[0].SourcesStatus[0].Stats.Failed)

		require.EqualValues(t, true, js.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Completed)
		require.EqualValues(t, 3, js.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.In)
		require.EqualValues(t, 2, js.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.Out)
		require.EqualValues(t, 1, js.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.Failed)
	})

	t.Run("Corrupted Source & Destination Stats", func(t *testing.T) {
		js := rsources.JobStatus{
			ID: "job-id",
			TasksStatus: []rsources.TaskStatus{
				{
					ID: "task-id",
					SourcesStatus: []rsources.SourceStatus{
						{
							ID:        "source-id",
							Completed: false,
							Stats: rsources.Stats{
								In:     1,
								Out:    2,
								Failed: 0,
							},
							DestinationsStatus: []rsources.DestinationStatus{
								{
									ID:        "destination-id",
									Completed: false,
									Stats: rsources.Stats{
										In:     2,
										Out:    2,
										Failed: 1,
									},
								},
							},
						},
					},
				},
			},
		}

		js.FixCorruptedStats(logger.NOP)
		require.EqualValues(t, true, js.TasksStatus[0].SourcesStatus[0].Completed)
		require.EqualValues(t, 2, js.TasksStatus[0].SourcesStatus[0].Stats.In)
		require.EqualValues(t, 2, js.TasksStatus[0].SourcesStatus[0].Stats.Out)
		require.EqualValues(t, 0, js.TasksStatus[0].SourcesStatus[0].Stats.Failed)

		require.EqualValues(t, true, js.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Completed)
		require.EqualValues(t, 3, js.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.In)
		require.EqualValues(t, 2, js.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.Out)
		require.EqualValues(t, 1, js.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.Failed)
	})

	t.Run("Nothing is corrupted", func(t *testing.T) {
		js := rsources.JobStatus{
			ID: "job-id",
			TasksStatus: []rsources.TaskStatus{
				{
					ID: "task-id",
					SourcesStatus: []rsources.SourceStatus{
						{
							ID:        "source-id",
							Completed: false,
							Stats: rsources.Stats{
								In:     3,
								Out:    2,
								Failed: 0,
							},
							DestinationsStatus: []rsources.DestinationStatus{
								{
									ID:        "destination-id",
									Completed: false,
									Stats: rsources.Stats{
										In:     2,
										Out:    1,
										Failed: 0,
									},
								},
							},
						},
					},
				},
			},
		}

		js.FixCorruptedStats(logger.NOP)

		require.EqualValues(t, false, js.TasksStatus[0].SourcesStatus[0].Completed)
		require.EqualValues(t, 3, js.TasksStatus[0].SourcesStatus[0].Stats.In)
		require.EqualValues(t, 2, js.TasksStatus[0].SourcesStatus[0].Stats.Out)
		require.EqualValues(t, 0, js.TasksStatus[0].SourcesStatus[0].Stats.Failed)

		require.EqualValues(t, false, js.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Completed)
		require.EqualValues(t, 2, js.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.In)
		require.EqualValues(t, 1, js.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.Out)
		require.EqualValues(t, 0, js.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.Failed)
	})
}
