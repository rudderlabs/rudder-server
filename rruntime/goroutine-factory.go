package rruntime

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"

	"github.com/bugsnag/bugsnag-go"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

//StartGoroutine Starts the excution of the function passed as argument in a new Goroutine
func StartGoroutine(function func()) {
	ctx := bugsnag.StartSession(context.Background())
	go func() {
		defer func() {
			if r := recover(); r != nil {
				debug.PrintStack()
				defer bugsnag.AutoNotify(ctx, bugsnag.SeverityError, bugsnag.MetaData{
					"GoRoutines": {
						"Number": runtime.NumGoroutine(),
					}})

				misc.RecordAppError(fmt.Errorf("%v", r))
				logger.Fatal(r)
				panic(r)
			}
		}()
		function()
	}()
}
