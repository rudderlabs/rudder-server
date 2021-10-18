package main

import (
	"time"

	client "github.com/rudderlabs/rudder-server/regulation-worker/internal/api-client"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/service"
)

var timeout time.Duration

//creates `loop` object and call getJobLoop and updateStatusLoop methods in different go routines.
func main() {
	timeout = time.Minute * 10
	l := &Loop{
		svc: &service.JobSvcImpl{
			Api: client.JobAPI{
				WorkspaceID: getWorkspaceID(),
			},
		},
	}
	go l.getJobLoop()
	go l.updateStatusLoop()
}

//TODO: get workspaceID from env
func getWorkspaceID() string {
	workspaceID := "mockWorkspaceID"
	return workspaceID
}

type Looper interface {
	getJobLoop()
	updateStatusLoop()
}

type Loop struct {
	svc service.JobSvc
}

//in each iteration it calls the getJob method of jobSvc, if it returns no job found error, it goes into exponential backoff sleep. If job was found
//then the iteration is blocked
//TODO: make sleep more sophisticated with exponential sleep time & max timeout.
//if we receive error other than ErrNoRunnableJob then we retry to getJob immediately.
func (l *Loop) getJobLoop() {
	for {
		err := l.svc.GetJob()
		if err == model.ErrNoRunnableJob {
			time.Sleep(10 * time.Minute)
		}
	}
}

//calls the updateStatus method of service and sleeps till next timeout.
func (l *Loop) updateStatusLoop() {
	for {
		err := l.svc.UpdateJobStatus()
		if err != nil {
			continue
		}
		time.Sleep(timeout - time.Minute)
	}
}
