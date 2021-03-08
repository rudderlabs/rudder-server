package misc

import (
	"sync"
)

type WaitGroup struct {
	wg       sync.WaitGroup
	errChan  chan error
	doneChan chan bool
}

func NewWaitGroup() *WaitGroup {
	return &WaitGroup{
		wg:       sync.WaitGroup{},
		errChan:  make(chan error),
		doneChan: make(chan bool, 1),
	}
}

func (wg *WaitGroup) Add(delta int) {
	wg.wg.Add(delta)
}

func (wg *WaitGroup) Done() {
	wg.wg.Done()
}

func (wg *WaitGroup) Err(err error) {
	wg.errChan <- err
	wg.wg.Done()
}

func (wg *WaitGroup) Wait() error {
	go func() {
		wg.wg.Wait()
		wg.doneChan <- true
	}()
	select {
	case err := <-wg.errChan:
		return err
	case <-wg.doneChan:
		return nil
	}
}

func (wg *WaitGroup) WaitForAll() []error {
	var errList []error
	go func() {
		wg.wg.Wait()
		wg.doneChan <- true
	}()
	for {
		select {
		case err := <-wg.errChan:
			errList = append(errList, err)
		case <-wg.doneChan:
			return errList
		}
	}
}
