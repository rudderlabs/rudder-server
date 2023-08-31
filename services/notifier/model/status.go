package model

type JobStatus string

const (
	Waiting   JobStatus = "waiting"
	Executing JobStatus = "executing"
	Succeeded JobStatus = "succeeded"
	Failed    JobStatus = "failed"
	Aborted   JobStatus = "aborted"
)
