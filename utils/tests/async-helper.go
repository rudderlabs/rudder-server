package testutils

import (
	"sync"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

/*
AsyncTestHelper provides synchronization methods to test goroutines.
Example:

	var (
		asyncHelper testutils.AsyncTestHelper`
	)

	BeforeEach(func() {
		mockMyInterface.EXPECT().MyMethodInGoroutine().Do(asyncHelper.ExpectAndNotifyCallback())
	})

	AfterEach(func() {
		asyncHelper.WaitWithTimeout(time.Second)
	})
*/
type AsyncTestHelper struct {
	wg sync.WaitGroup
}

/*
ExpectAndNotifyCallback Adds one to this helper's WaitGroup, and provides a callback that calls Done on it.
Should be used for gomock Do calls that trigger via mocked functions executed in a goroutine.
*/
func (helper *AsyncTestHelper) ExpectAndNotifyCallback() func(...interface{}) {
	helper.wg.Add(1)
	return func(...interface{}) {
		helper.wg.Done()
	}
}

/*
WaitWithTimeout waits for this helper's WaitGroup until provided timeout.
Should wait for all ExpectAndNotifyCallback callbacks, registered in asynchronous mocks calls
*/
func (helper *AsyncTestHelper) WaitWithTimeout(d time.Duration) {
	RunTestWithTimeout(func() {
		helper.wg.Wait()
	}, d)
}

/*
RunTestWithTimeout runs function f until provided timeout.
If the function times out, it will cause the ginkgo test to fail.
*/
func RunTestWithTimeout(f func(), d time.Duration) {
	misc.RunWithTimeout(func() {
		defer ginkgo.GinkgoRecover()
		f()
	}, func() {
		ginkgo.Fail("Async helper's wait group timed out")
	}, d)
}
