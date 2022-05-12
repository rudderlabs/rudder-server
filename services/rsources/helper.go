package rsources

func statusFromQueryResult(jobRunId string, statMap map[JobTargetKey]Stats) JobStatus {
	status := JobStatus{
		ID: jobRunId,
	}

	taskRunIdIndex := make(map[string]int)           // task run id -> index
	sourceIdIndex := make(map[string]map[string]int) // sourceID -> taskRunId -> index

	for key, stat := range statMap {

		var idx int
		if _, ok := taskRunIdIndex[key.TaskRunId]; !ok {
			idx = len(taskRunIdIndex)
			status.TasksStatus = append(status.TasksStatus, TaskStatus{
				ID: key.TaskRunId,
			})
			taskRunIdIndex[key.TaskRunId] = idx
		} else {
			idx = taskRunIdIndex[key.TaskRunId]
		}

		var source_idx int
		if _, ok := sourceIdIndex[key.SourceId]; !ok {
			source_idx = len(status.TasksStatus[idx].SourcesStatus)
			status.TasksStatus[idx].SourcesStatus = append(
				status.TasksStatus[idx].SourcesStatus, SourceStatus{
					ID: key.SourceId,
				})
			sourceIdIndex[key.SourceId] = make(map[string]int)
			sourceIdIndex[key.SourceId][key.TaskRunId] = source_idx
		} else {
			if _, ok := sourceIdIndex[key.SourceId][key.TaskRunId]; !ok {
				source_idx = len(status.TasksStatus[idx].SourcesStatus)
				status.TasksStatus[idx].SourcesStatus = append(
					status.TasksStatus[idx].SourcesStatus, SourceStatus{
						ID: key.SourceId,
					})
				sourceIdIndex[key.SourceId][key.TaskRunId] = source_idx
			} else {
				source_idx = sourceIdIndex[key.SourceId][key.TaskRunId]
			}
		}

		if key.DestinationId == "" {
			status.TasksStatus[idx].SourcesStatus[source_idx].Stats = stat
		} else {
			status.TasksStatus[idx].SourcesStatus[source_idx].DestinationsStatus = append(
				status.TasksStatus[idx].SourcesStatus[source_idx].DestinationsStatus, DestinationStatus{
					ID:        key.DestinationId,
					Stats:     stat,
					Completed: stat.completed(),
				})
		}
	}

	for _, taskStatus := range status.TasksStatus {
		for i := range taskStatus.SourcesStatus {
			sourcestat := &taskStatus.SourcesStatus[i]
			sourcestat.calculateCompleted()
		}
	}

	return status
}
