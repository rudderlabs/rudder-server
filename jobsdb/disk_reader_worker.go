package jobsdb

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"
)

func newDiskReadWorker(partition string, readChan chan readRequest) *worker {
	w := &worker{
		partition: partition,
		readChan:  readChan,
	}
	w.lifecycle.ctx, w.lifecycle.cancel = context.WithCancel(context.Background())
	w.start()
	return w

}

type worker struct {
	partition string
	file      *os.File

	lifecycle struct {
		ctx    context.Context
		cancel context.CancelFunc
		wg     sync.WaitGroup
	}

	readChan chan readRequest
}

func (w *worker) start() {
	fileName := w.partition
	file, err := os.Open(fileName)
	if err != nil {
		panic(fmt.Errorf("open file - %s: %w", fileName, err))
	}
	w.file = file
	w.lifecycle.wg.Add(1)
	go func() {
		defer w.lifecycle.wg.Done()
		<-w.lifecycle.ctx.Done()
		if err := file.Close(); err != nil {
			panic(fmt.Errorf("close file - %s: %w", fileName, err))
		}
	}()
}

func (w *worker) Work() bool {
	readReq := <-w.readChan
	offset, length := readReq.offset, readReq.length
	response := readReq.response
	payload := make([]byte, length)
	_, err := w.file.ReadAt(payload, int64(offset))
	if err != nil {
		response <- payloadOrError{err: fmt.Errorf("read file - %s - %d: %w", w.partition, offset, err)}
		return false
	}
	response <- payloadOrError{payload: payload}
	return true
}

func (w *worker) SleepDurations() (min, max time.Duration) {
	return 0, 0 // set workerpool idle timeout to 5 seconds - that is more relevant here
}

func (w *worker) Stop() {
	w.lifecycle.cancel()
	w.lifecycle.wg.Wait()
}
