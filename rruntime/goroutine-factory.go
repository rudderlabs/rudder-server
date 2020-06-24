package rruntime

import (
	"github.com/rudderlabs/rudder-server/app/crash"
)

//Go Starts the excution of the function passed as argument in a new Goroutine.
//The created goroutine will handle panics using the default crash handler and a new Bugsnag session.
//
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
		ctx := crash.StartBugsnagSession()
		defer crash.Default.DeferWithContext(ctx)
		function()
	}()
}
