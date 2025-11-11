/*
Package admin :
- has a rpc over http server listening on a unix socket
- support other packages to expose any admin functionality over the above server

# Example for registering admin handler from another package

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
	"context"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"time"

	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// RegisterAdminHandler is used by other packages to
// expose admin functions over the unix socket based rpc interface
func RegisterAdminHandler(name string, handler interface{}) {
	_ = instance.rpcServer.RegisterName(name, handler) // @TODO fix ignored error
}

type Admin struct {
	rpcServer *rpc.Server
}

var (
	instance  *Admin
	pkgLogger logger.Logger
)

func Init() {
	pkgLogger = logger.NewLogger().Child("admin")
	instance = &Admin{rpcServer: rpc.NewServer()}
	err := instance.rpcServer.Register(instance)
	if err != nil {
		pkgLogger.Errorn("Error registering admin handler", obskit.Error(err))
	}
}

type LogLevel struct {
	Module string
	Level  string
}

func (*Admin) SetLogLevel(l LogLevel, reply *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r) //nolint:forbidigo
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()
	err = logger.SetLogLevel(l.Module, l.Level)
	if err == nil {
		*reply = fmt.Sprintf("Module %s log level set to %s", l.Module, l.Level)
	}
	return err
}

// GetLoggingConfig returns the logging configuration
func (*Admin) GetLoggingConfig(_ struct{}, reply *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r) //nolint:forbidigo
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()
	loggingConfigMap := logger.GetLoggingConfig()
	formattedOutput, err := jsonrs.MarshalIndent(loggingConfigMap, "", "  ")
	*reply = string(formattedOutput)
	return err
}

// StartServer starts an HTTP server listening on unix socket and serving rpc communication
func StartServer(ctx context.Context) error {
	tmpDirPath, err := misc.GetTmpDir()
	if err != nil {
		panic(err)
	}
	sockAddr := filepath.Join(tmpDirPath, "rudder-server.sock")
	if err := os.RemoveAll(sockAddr); err != nil {
		pkgLogger.Fataln("Cannot remove socket file", obskit.Error(err))
		return err
	}
	defer func() {
		if err := os.RemoveAll(sockAddr); err != nil {
			pkgLogger.Warnn("Cannot remove socket file", obskit.Error(err))
		}
	}()

	l, err := net.Listen("unix", sockAddr)
	if err != nil {
		pkgLogger.Fataln("Listen error", obskit.Error(err))
		return err
	}
	defer func() {
		if l != nil {
			if err := l.Close(); err != nil {
				pkgLogger.Warnn("Cannot close listener", obskit.Error(err))
			}
		}
	}()

	pkgLogger.Infon("Serving on admin interface", logger.NewStringField("socket", sockAddr))
	srvMux := http.NewServeMux()
	srvMux.Handle(rpc.DefaultRPCPath, instance.rpcServer)

	srv := &http.Server{Handler: srvMux, ReadHeaderTimeout: 3 * time.Second}

	return kithttputil.Serve(ctx, srv, l, time.Second)
}
