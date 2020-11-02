package rruntime

import (
	"context"
	"fmt"
	"runtime"

	"github.com/bugsnag/bugsnag-go"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var pkgLogger logger.LoggerI

func init() {
	pkgLogger = logger.NewLogger().Child("rruntime")
}

//Go Starts the excution of the function passed as argument in a new Goroutine
//THING TO NOTE: If the function you are intending to run inside a goroutine takes any parameters,
//before calling this function, create local variable for every argument (so that evaluation of the argument happens immediately)
//and then pass those local variables as arguments
//Ex.
//    var worker *workerT
//    worker = &workerT{
//      	workerID: i,
//    }
//    rruntime.Go(func() {
//      	rt.workerProcess(worker)
//    })
func Go(function func()) {
	go func() {
		ctx := bugsnag.StartSession(context.Background())
		defer func() {
			if r := recover(); r != nil {
				defer bugsnag.AutoNotify(ctx, bugsnag.SeverityError, bugsnag.MetaData{
					"GoRoutines": {
						"Number": runtime.NumGoroutine(),
					}})

				misc.RecordAppError(fmt.Errorf("%v", r))
				pkgLogger.Fatal(r)
				panic(r)
			}
		}()
		function()
	}()
}
