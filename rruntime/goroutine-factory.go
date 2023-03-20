package rruntime

import (
	"context"

	"github.com/bugsnag/bugsnag-go/v2"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// Go Starts the execution of the function passed as argument in a new Goroutine
// THING TO NOTE: If the function you are intending to run inside a goroutine takes any parameters,
// before calling this function, create local variable for every argument (so that evaluation of the argument happens immediately)
// and then pass those local variables as arguments
// Ex.
//
//	var worker *workerT
//	worker = &workerT{
//	  	workerID: i,
//	}
//	rruntime.Go(func() {
//	  	rt.workerProcess(worker)
//	})
func Go(function func()) {
	go func() {
		ctx := bugsnag.StartSession(context.Background())
		defer misc.BugsnagNotify(ctx, "Core")()
		function()
	}()
}

func GoForWarehouse(function func()) {
	go func() {
		ctx := bugsnag.StartSession(context.Background())
		defer misc.BugsnagNotify(ctx, "Warehouse")()
		function()
	}()
}

var GoRoutineFactory goRoutineFactory

type goRoutineFactory struct{}

func (goRoutineFactory) Go(function func()) {
	Go(function)
}
