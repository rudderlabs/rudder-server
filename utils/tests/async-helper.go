package testutils

import (
	"fmt"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

// AsyncTestHelper provides synchronization methods to test goroutines.
// Example:
//
//	var (
//		asyncHelper testutils.AsyncTestHelper
//	)
//
//	BeforeEach(func() {
//		mockMyInterface.EXPECT().MyMethodInGoroutine().Do(asyncHelper.ExpectAndNotifyCallback())
//	})
//
//	AfterEach(func() {
//		asyncHelper.WaitWithTimeout(time.Second)
//	})
type AsyncTestHelper struct {
	wg             sync.WaitGroup
	waitingMap     map[string]int
	waitingMapLock sync.RWMutex
}

// ExpectAndNotifyCallback Adds one to this helper's WaitGroup, and provides a callback that calls Done on it.
// Should be used for gomock Do calls that trigger via mocked functions executed in a goroutine.
func (helper *AsyncTestHelper) ExpectAndNotifyCallback() func(...interface{}) {
	helper.wg.Add(1)
	return func(...interface{}) {
		helper.wg.Done()
	}
}

// ExpectAndNotifyCallback Adds one to this helper's WaitGroup, and provides a callback that calls Done on it.
// Should be used for gomock Do calls that trigger via mocked functions executed in a goroutine.
func (helper *AsyncTestHelper) ExpectAndNotifyCallbackWithName(name string) func(...interface{}) {
	helper.waitingMapLock.Lock()
	defer helper.waitingMapLock.Unlock()

	if _, ok := helper.waitingMap[name]; !ok {
		helper.waitingMap[name] = 0
	}
	helper.waitingMap[name]++

	helper.wg.Add(1)
	return func(...interface{}) {
		helper.waitingMapLock.Lock()
		defer helper.waitingMapLock.Unlock()

		helper.wg.Done()
		helper.waitingMap[name]--
	}
}

// ExpectAndNotifyCallback Adds one to this helper's WaitGroup, and provides a callback that calls Done on it.
// Should be used for gomock Do calls that trigger via mocked functions executed in a goroutine.
func (helper *AsyncTestHelper) ExpectAndNotifyCallbackWithNameOnce(name string) func() {
	var s sync.Once
	f := helper.ExpectAndNotifyCallbackWithName(name)
	return func() {
		s.Do(func() {
			f()
		})
	}
}

// WaitWithTimeout waits for this helper's WaitGroup until provided timeout.
// Should wait for all ExpectAndNotifyCallback callbacks, registered in asynchronous mocks calls
func (helper *AsyncTestHelper) WaitWithTimeout(d time.Duration) {
	helper.RunTestWithTimeout(func() {
		helper.wg.Wait()
	}, d)
}

// RegisterCalls registers a number of calls to this async helper, so that they are waited for.
func (helper *AsyncTestHelper) RegisterCalls(calls ...*gomock.Call) {
	for _, call := range calls {
		call.Do(helper.ExpectAndNotifyCallback())
	}
}

// RunTestWithTimeout runs function f until provided timeout.
// If the function times out, it will cause the ginkgo test to fail.
func (helper *AsyncTestHelper) RunTestWithTimeout(f func(), d time.Duration) {
	misc.RunWithTimeout(func() {
		defer ginkgo.GinkgoRecover()
		f()
	}, func() {
		helper.waitingMapLock.RLock()
		defer helper.waitingMapLock.RUnlock()
		for k, v := range helper.waitingMap {
			fmt.Println(k, "", v)
		}

		ginkgo.Fail("Async helper's wait group timed out")
	}, d)
}

// RunTestWithTimeout runs function f until provided timeout.
// If the function times out, it will cause the ginkgo test to fail.
func RunTestWithTimeout(f func(), d time.Duration) {
	misc.RunWithTimeout(func() {
		defer ginkgo.GinkgoRecover()
		f()
	}, func() {
		ginkgo.Fail("Async helper's wait group timed out")
	}, d)
}

func (helper *AsyncTestHelper) Setup() {
	helper.waitingMap = make(map[string]int)
}
