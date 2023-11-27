package rsources

import (
	"sort"
)

func statusFromQueryResult(jobRunId string, statMap map[JobTargetKey]Stats) JobStatus {
	status := JobStatus{
		ID: jobRunId,
	}

	taskRunIdIndex := make(map[string]int)           // task run id -> index
	sourceIdIndex := make(map[string]map[string]int) // task run id -> source id -> index
	sortedKeys := make([]JobTargetKey, 0, len(statMap))
	for key := range statMap {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Slice(sortedKeys, func(i, j int) bool {
		return sortedKeys[i].String() < sortedKeys[j].String()
	})
	for _, key := range sortedKeys {
		stat := statMap[key]
		var idx int
		var ok bool
		if idx, ok = taskRunIdIndex[key.TaskRunID]; !ok {
			idx = len(taskRunIdIndex)
			status.TasksStatus = append(status.TasksStatus, TaskStatus{
				ID: key.TaskRunID,
			})
			taskRunIdIndex[key.TaskRunID] = idx
			sourceIdIndex[key.TaskRunID] = make(map[string]int)
		}

		var sourceIdx int
		if sourceIdx, ok = sourceIdIndex[key.TaskRunID][key.SourceID]; !ok {
			sourceIdx = len(status.TasksStatus[idx].SourcesStatus)
			status.TasksStatus[idx].SourcesStatus = append(
				status.TasksStatus[idx].SourcesStatus, SourceStatus{
					ID: key.SourceID,
				})
			sourceIdIndex[key.TaskRunID][key.SourceID] = sourceIdx
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

func failedRecordsFromQueryResult[T any](jobRunId string, recordsMap map[JobTargetKey][]T) JobFailedRecords[T] {
	result := JobFailedRecords[T]{
		ID: jobRunId,
	}

	taskRunIdIndex := make(map[string]int)           // task run id -> index
	sourceIdIndex := make(map[string]map[string]int) // task run id -> source id -> index
	sortedKeys := make([]JobTargetKey, 0, len(recordsMap))
	for key := range recordsMap {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Slice(sortedKeys, func(i, j int) bool {
		return sortedKeys[i].String() < sortedKeys[j].String()
	})
	for _, key := range sortedKeys {
		records := recordsMap[key]
		var taskIdx, sourceIdx int
		var ok bool
		if taskIdx, ok = taskRunIdIndex[key.TaskRunID]; !ok {
			taskIdx = len(taskRunIdIndex)
			result.Tasks = append(result.Tasks, TaskFailedRecords[T]{
				ID: key.TaskRunID,
			})
			taskRunIdIndex[key.TaskRunID] = taskIdx
			sourceIdIndex[key.TaskRunID] = make(map[string]int)
		}

		if sourceIdx, ok = sourceIdIndex[key.TaskRunID][key.SourceID]; !ok {
			sourceIdx = len(result.Tasks[taskIdx].Sources)
			sourceIdIndex[key.TaskRunID][key.SourceID] = sourceIdx
			result.Tasks[taskIdx].Sources = append(result.Tasks[taskIdx].Sources, SourceFailedRecords[T]{ID: key.SourceID})
		}

		if key.DestinationID == "" {
			result.Tasks[taskIdx].Sources[sourceIdx].Records = append(result.Tasks[taskIdx].Sources[sourceIdx].Records, records...)
		} else {
			result.Tasks[taskIdx].Sources[sourceIdx].Destinations = append(result.Tasks[taskIdx].Sources[sourceIdx].Destinations, DestinationFailedRecords[T]{ID: key.DestinationID, Records: records})
		}
	}

	return result
}
