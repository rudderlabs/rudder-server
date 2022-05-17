package model

type JobFilter struct {
	TaskRunId *[]string
	SourceId  *[]string
}

type FailedRecords struct {
}

type JobStatus struct {
	ID          string       `json:"id"`
	TasksStatus []TaskStatus `json:"tasks"`
}

type TaskStatus struct {
	ID            string         `json:"id"`
	SourcesStatus []SourceStatus `json:"sources"`
}

type SourceStatus struct {
	ID                 string              `json:"id"`
	Completed          bool                `json:"completed"`
	Stats              Stats               `json:"stats"`
	DestinationsStatus []DestinationStatus `json:"destinations"`
}

type DestinationStatus struct {
	ID        string `json:"id"`
	Completed bool   `json:"completed"`
	Stats     Stats  `json:"stats"`
}
type Stats struct {
	In     uint `json:"in"`
	Out    uint `json:"out"`
	Failed uint `json:"failed"`
}
