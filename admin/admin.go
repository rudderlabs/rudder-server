package admin

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
)

const sockAddr = "/tmp/rudder-server.sock"

/*
Example for registering admin handler from another package

// Add this while initializing the package in setup or init etc.
admin.RegisterAdminHandler("PackageName", &PackageAdmin{})

// Convention is to keep the following code in admin.go in the respective package
type PackageAdmin struct {
}

// Status function is used for debug purposes by the admin interface
func (p *PackageAdmin) Status() string {
	return fmt.Sprintf(
		`Package:
---------
Parameter 1  : value
Parameter 2  : value
`)
}

// The following function can be called from rudder-cli using getUDSClient().Call("PackageName.SomeAdminFunction", &arg, &reply)
func (p *PackageAdmin) SomeAdminFunction(arg *string, reply *string) error {
	*reply = "admin function output"
	return nil
}
*/

// PackageAdminHandler to be implemented by other package admin objects
type PackageAdminHandler interface {
	Status() string
}

// RegisterAdminHandler is used by other packages to
// expose admin functions over the unix socket based rpc interface
func RegisterAdminHandler(name string, handler PackageAdminHandler) {
	instance.handlers[strings.ToLower(name)] = handler
	instance.rpcServer.RegisterName(name, handler)
}

type Admin struct {
	handlers  map[string]PackageAdminHandler
	rpcServer *rpc.Server
}

var instance Admin

func init() {
	instance = Admin{
		handlers:  make(map[string]PackageAdminHandler),
		rpcServer: rpc.NewServer(),
	}
	instance.rpcServer.Register(instance)
}

func (a Admin) Status(module *string, reply *string) error {

	if *module == "" {
		for _, handler := range a.handlers {
			*reply += handler.Status() + "\n"
		}
	} else if _, ok := a.handlers[*module]; ok {
		*reply = a.handlers[*module].Status()
	} else {
		statusEnabledModules := make([]string, 0, len(a.handlers))
		for k := range a.handlers {
			statusEnabledModules = append(statusEnabledModules, "\""+k+"\"")
		}
		return fmt.Errorf("valid module arguments are " + strings.Join(statusEnabledModules, ","))
	}

	return nil
}

func (s Admin) PrintStack(arg *string, reply *string) error {
	byteArr := make([]byte, 2048*1024)
	n := runtime.Stack(byteArr, true)
	*reply = string(byteArr[:n])
	return nil
}

func (s Admin) HeapDump(path *string, reply *string) error {
	f, err := os.OpenFile(*path, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer f.Close()
	pprof.Lookup("heap").WriteTo(f, 1)
	*reply = "Heap profile written to " + *path
	return nil
}

func StartServer() {
	if err := os.RemoveAll(sockAddr); err != nil {
		log.Fatal(err)
	}
	l, e := net.Listen("unix", sockAddr)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	fmt.Println("Serving on admin interface...")
	srvMux := http.NewServeMux()
	srvMux.Handle(rpc.DefaultRPCPath, instance.rpcServer)
	http.Serve(l, srvMux)
}
