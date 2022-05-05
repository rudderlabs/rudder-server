package api

type JobStatusSchema struct {
	ID          string             `json:"id"`
	TasksStatus []TaskStatusSchema `json:"tasks"`
}

type TaskStatusSchema struct {
	ID            string               `json:"id"`
	SourcesStatus []SourceStatusSchema `json:"sources"`
}

type SourceStatusSchema struct {
	ID                 string                    `json:"id"`
	Completed          bool                      `json:"completed"`
	Stats              StatsSchema               `json:"stats"`
	DestinationsStatus []DestinationStatusSchema `json:"destinations"`
}

type DestinationStatusSchema struct {
	ID        string      `json:"id"`
	Completed bool        `json:"completed"`
	Stats     StatsSchema `json:"stats"`
}

type StatsSchema struct {
	In     uint `json:"in"`
	Out    uint `json:"out"`
	Failed uint `json:"failed"`
}
