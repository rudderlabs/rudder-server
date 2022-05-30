package rsources

func statusFromQueryResult(jobRunId string, statMap map[JobTargetKey]Stats) JobStatus {
	status := JobStatus{
		ID: jobRunId,
	}

	taskRunIdIndex := make(map[string]int)           // task run id -> index
	sourceIdIndex := make(map[string]map[string]int) // sourceID -> taskRunId -> index

	for key, stat := range statMap {

		var idx int
		if _, ok := taskRunIdIndex[key.TaskRunID]; !ok {
			idx = len(taskRunIdIndex)
			status.TasksStatus = append(status.TasksStatus, TaskStatus{
				ID: key.TaskRunID,
			})
			taskRunIdIndex[key.TaskRunID] = idx
		} else {
			idx = taskRunIdIndex[key.TaskRunID]
		}

		var sourceIdx int
		if _, ok := sourceIdIndex[key.SourceID]; !ok {
			sourceIdx = len(status.TasksStatus[idx].SourcesStatus)
			status.TasksStatus[idx].SourcesStatus = append(
				status.TasksStatus[idx].SourcesStatus, SourceStatus{
					ID: key.SourceID,
				})
			sourceIdIndex[key.SourceID] = make(map[string]int)
			sourceIdIndex[key.SourceID][key.TaskRunID] = sourceIdx
		} else {
			if _, ok := sourceIdIndex[key.SourceID][key.TaskRunID]; !ok {
				sourceIdx = len(status.TasksStatus[idx].SourcesStatus)
				status.TasksStatus[idx].SourcesStatus = append(
					status.TasksStatus[idx].SourcesStatus, SourceStatus{
						ID: key.SourceID,
					})
				sourceIdIndex[key.SourceID][key.TaskRunID] = sourceIdx
			} else {
				sourceIdx = sourceIdIndex[key.SourceID][key.TaskRunID]
			}
		}

		if key.DestinationID == "" {
			status.TasksStatus[idx].SourcesStatus[sourceIdx].Stats = stat
		} else {
			status.TasksStatus[idx].SourcesStatus[sourceIdx].DestinationsStatus = append(
				status.TasksStatus[idx].SourcesStatus[sourceIdx].DestinationsStatus, DestinationStatus{
					ID:        key.DestinationID,
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
