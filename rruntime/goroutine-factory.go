package rruntime

import (
	"context"
	"fmt"
	"runtime"

	"github.com/bugsnag/bugsnag-go"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func Go(function func()) {
	GoHandleError(function, panicOnError)
}

//GoHandleError Starts the excution of the function passed as argument in a new Goroutine
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
//When the go routine panics with an error, and if a custom errorHandler is passed
func GoHandleError(function func(), errorHandler func(err error)) {
	go func() {
		ctx := bugsnag.StartSession(context.Background())
		defer func() {
			if r := recover(); r != nil {

				defer bugsnag.AutoNotify(ctx, bugsnag.SeverityError, bugsnag.MetaData{
					"GoRoutines": {
						"Number": runtime.NumGoroutine(),
					}})

				misc.RecordAppError(fmt.Errorf("%v", r))
				logger.Fatal(r)
				err, ok := r.(string)
				_ = err
				_ = ok
				errorHandler(fmt.Errorf("%v", r))
			}
		}()
		function()
	}()
}

func panicOnError(err error) {
	panic(err)
}
