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
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"

	"github.com/rudderlabs/rudder-server/services/db"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/spf13/viper"
)

// PackageStatusHandler to be implemented by the package objects that are registered as status handlers
// output of Status() is expected to be json encodeable by default
type PackageStatusHandler interface {
	Status() interface{}
}

// RegisterAdminHandler is used by other packages to
// expose admin functions over the unix socket based rpc interface
func RegisterAdminHandler(name string, handler interface{}) {
	instance.rpcServer.RegisterName(name, handler)
}

// RegisterStatusHandler expects object implementing PackageStatusHandler interface
func RegisterStatusHandler(name string, handler PackageStatusHandler) {
	instance.statushandlers[strings.ToLower(name)] = handler
}

type Admin struct {
	statushandlers map[string]PackageStatusHandler
	rpcServer      *rpc.Server
}

var instance Admin

func init() {
	instance = Admin{
		statushandlers: make(map[string]PackageStatusHandler),
		rpcServer:      rpc.NewServer(),
	}
	instance.rpcServer.Register(instance)
}

// Status reports overall server status by fetching status of all registered admin handlers
func (a Admin) Status(noArgs struct{}, reply *string) error {
	statusObj := make(map[string]interface{})
	statusObj["server-mode"] = db.CurrentMode

	for moduleName, handler := range a.statushandlers {
		statusObj[moduleName] = handler.Status()
	}
	formattedOutput, err := json.MarshalIndent(statusObj, "", "  ")
	*reply = string(formattedOutput)
	return err
}

// PrintStack fetches stack traces of all running goroutines
func (a Admin) PrintStack(noArgs struct{}, reply *string) error {
	byteArr := make([]byte, 2048*1024)
	n := runtime.Stack(byteArr, true)
	*reply = string(byteArr[:n])
	return nil
}

// HeapDump creates heap profile at given path using pprof
func (a Admin) HeapDump(path *string, reply *string) error {
	f, err := os.OpenFile(*path, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer f.Close()
	pprof.Lookup("heap").WriteTo(f, 1)
	*reply = "Heap profile written to " + *path
	return nil
}

// ServerConfig fetches current configuration as set in viper
func (a Admin) ServerConfig(noArgs struct{}, reply *string) error {
	config := make(map[string]interface{})
	for _, key := range viper.AllKeys() {
		config[key] = viper.Get(key)
	}
	formattedOutput, err := json.MarshalIndent(config, "", "  ")
	*reply = string(formattedOutput)
	return err
}

type LogLevel struct {
	Module string
	Level  string
}

func (a Admin) SetLogLevel(l LogLevel, reply *string) error {
	err := logger.SetModuleLevel(l.Module, l.Level)
	if err == nil {
		*reply = fmt.Sprintf("Module %s log level set to %s", l.Module, l.Level)
	}
	return err
}

// StartServer starts an http server listening on unix socket and serving rpc communication
func StartServer() {
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	sockAddr := filepath.Join(tmpDirPath, "rudder-server.sock")
	if err := os.RemoveAll(sockAddr); err != nil {
		log.Fatal(err)
	}
	l, e := net.Listen("unix", sockAddr)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	logger.Info("Serving on admin interface @ ", sockAddr)
	srvMux := http.NewServeMux()
	srvMux.Handle(rpc.DefaultRPCPath, instance.rpcServer)
	http.Serve(l, srvMux)
}
