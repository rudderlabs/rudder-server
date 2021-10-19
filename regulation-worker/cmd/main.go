package main

import (
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/client"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/service"
)

//TODO: in case of os.Interrupt, syscall.SIGTERM make sure that all are notified about the termination of the service (like transformer, if a job is in progress, etc.)
//creates `loop` object and call getJobLoop and updateStatusLoop methods in different go routines.
func main() {
	svc := service.JobSvc{
		API: client.JobAPI{
			WorkspaceID: getWorkspaceID(),
		},
	}

	l := &service.Looper{Svc: svc}
	l.Loop()
}

//TODO: get workspaceID from env
func getWorkspaceID() string {
	workspaceID := "mockWorkspaceID"
	return workspaceID
}
