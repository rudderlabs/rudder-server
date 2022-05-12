package model

type JobFilter struct {
	TaskRunId     *string
	SourceId      *string
	DestinationId *string
}

type FailedRecords struct {
}

type JobStatus struct {
	ID          string
	TasksStatus []TaskStatus
}

type TaskStatus struct {
	ID            string
	SourcesStatus []SourceStatus
}

type SourceStatus struct {
	ID                 string
	Completed          bool
	Stats              Stats
	DestinationsStatus []DestinationStatus
}

type DestinationStatus struct {
	ID        string
	Completed bool
	Stats     Stats
}

type Stats struct {
	In     uint
	Out    uint
	Failed uint
}
