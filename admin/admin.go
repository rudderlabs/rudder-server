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
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/viper"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// PackageStatusHandler to be implemented by the package objects that are registered as status handlers
// output of Status() is expected to be json encodable by default
type PackageStatusHandler interface {
	Status() interface{}
}

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
	instance = &Admin{
		rpcServer: rpc.NewServer(),
	}
	_ = instance.rpcServer.Register(instance) // @TODO fix ignored error
	pkgLogger = logger.NewLogger().Child("admin")
}

// ServerConfig fetches current configuration as set in viper
func (*Admin) ServerConfig(_ struct{}, reply *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()

	conf := make(map[string]interface{})
	for _, key := range viper.AllKeys() {
		conf[key] = viper.Get(key)
	}
	formattedOutput, err := json.MarshalIndent(conf, "", "  ")
	*reply = string(formattedOutput)
	return err
}

type LogLevel struct {
	Module string
	Level  string
}

func (*Admin) SetLogLevel(l LogLevel, reply *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
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
			pkgLogger.Error(r)
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()
	loggingConfigMap := logger.GetLoggingConfig()
	formattedOutput, err := json.MarshalIndent(loggingConfigMap, "", "  ")
	*reply = string(formattedOutput)
	return err
}

// GetFormattedEnv return the formatted env
func (*Admin) GetFormattedEnv(env string, reply *string) (err error) {
	*reply = config.ConfigKeyToEnv(config.DefaultEnvPrefix, env)
	return nil
}

// StartServer starts an HTTP server listening on unix socket and serving rpc communication
func StartServer(ctx context.Context) error {
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	sockAddr := filepath.Join(tmpDirPath, "rudder-server.sock")
	if err := os.RemoveAll(sockAddr); err != nil {
		pkgLogger.Fatal(err) // @TODO return?
	}
	defer func() {
		if err := os.RemoveAll(sockAddr); err != nil {
			pkgLogger.Warn(err)
		}
	}()

	l, e := net.Listen("unix", sockAddr)
	if e != nil {
		pkgLogger.Fatal("listen error:", e) // @TODO return?
	}
	defer func() {
		if l != nil {
			if err := l.Close(); err != nil {
				pkgLogger.Warn(err)
			}
		}
	}()

	pkgLogger.Info("Serving on admin interface @ ", sockAddr)
	srvMux := http.NewServeMux()
	srvMux.Handle(rpc.DefaultRPCPath, instance.rpcServer)

	srv := &http.Server{Handler: srvMux, ReadHeaderTimeout: 3 * time.Second}

	return kithttputil.Serve(ctx, srv, l, time.Second)
}
