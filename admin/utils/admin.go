/*
Package admin :
- has a rpc over http server listening on a unix socket
- support other packages to expose any admin functionality over the above server

Example for registering admin handler from another package

// Add this while initializing the package in setup or init etc.
admin.RegisterAdminHandler("PackageName", &PackageAdmin{})
admin.RegisterStatusHandler("PackageName", &PackageAdmin{})

// Convention is to keep the following code in admin.go in the respective package
type PackageAdmin struct {
}

// Status function is used for debug purposes by the admin interface
func (p *PackageAdmin) Status() map[string]interface{} {
	return map[string]interface{}{
		"parameter-1"  : value,
		"parameter-2"  : value,
	}
}

// The following function can be called from rudder-cli using getUDSClient().Call("PackageName.SomeAdminFunction", &arg, &reply)
func (p *PackageAdmin) SomeAdminFunction(arg *string, reply *string) error {
	*reply = "admin function output"
	return nil
}
*/
package admin

import (
	"fmt"
	"strings"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

type MiscHandler struct {
	jobsDB         jobsdb.JobsDB
	readOnlyJobsDB jobsdb.ReadonlyHandleT
}

var miscHandler *MiscHandler

func init() {
	miscHandler = &MiscHandler{}
	admin.RegisterAdminHandler("Misc", miscHandler)
}

func (handler *MiscHandler) RunSQLQuery(argString string, reply *string) error {
	var err error

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Internal Rudder Server Error. Error: %w", r)
		}
	}()
	args := strings.Split(argString, ":")
	var response string
	var readOnlyJobsDB jobsdb.ReadonlyHandleT
	if args[0] == "brt" {
		args[0] = "batch_rt"
	}
	readOnlyJobsDB.Setup(args[0])
	switch args[1] {
	case "Jobs between JobID's of a User":
		response, err = readOnlyJobsDB.GetJobIDsForUser(args)
	case "Error Code Count By Destination":
		response, err = readOnlyJobsDB.GetFailedStatusErrorCodeCountsByDestination(args)
	}
	*reply = string(response)
	return err
}
