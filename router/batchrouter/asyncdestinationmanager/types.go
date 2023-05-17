package asyncdestinationmanager

type AsyncStatusResponse struct {
	Success        bool
	StatusCode     int
	HasFailed      bool
	HasWarning     bool
	FailedJobsURL  string
	WarningJobsURL string
}
