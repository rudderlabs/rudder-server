package buffer

import (
	"bufio"
	"fmt"
	"os"
	"sync"

	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	ht "github.com/rudderlabs/rudder-server/helper/types"
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

type BufferHandle struct {
	logger                  logger.Logger
	dir                     string
	counter                 int
	file                    *os.File
	writer                  *bufio.Writer
	bufferCapacity          int
	maxBytesForFileRotation int
	bytesWritten            int
	IsInitialised           bool
	mu                      *sync.RWMutex
}

func WithOptsFromConfig(prefix string, conf *config.Config) func(*BufferHandle) {
	maxSizeinBKey := fmt.Sprintf("%s.DebugHelper.bufferCapacityInB", prefix)
	maxBytesFileRotKey := fmt.Sprintf("%s.DebugHelper.maxBytesForFileRotation", prefix)

	maxSizeInB := conf.GetReloadableInt64Var(int64(1), 1, maxSizeinBKey).Load()
	maxBytesForFileRotation := conf.GetReloadableIntVar(2*1024, 1, maxBytesFileRotKey).Load()
	return func(bh *BufferHandle) {
		bh.bufferCapacity = int(maxSizeInB)
		bh.maxBytesForFileRotation = maxBytesForFileRotation
	}
}

func New(dir string, opts ...func(*BufferHandle)) *BufferHandle {
	bh := &BufferHandle{
		dir:                     dir,
		logger:                  logger.NewLogger().Child("helper.buffer_file"),
		bufferCapacity:          1,
		maxBytesForFileRotation: 2 * 1024,
		mu:                      &sync.RWMutex{},
	}
	for _, opt := range opts {
		opt(bh)
	}
	ready := make(chan bool, 1)
	var once sync.Once
	once.Do(func() {
		// create the directory to store logs
		if _, err := os.Stat(bh.dir); os.IsNotExist(err) {
			// file does not exist
			_ = os.MkdirAll(bh.dir, os.ModePerm)
		}
		err := bh.createFile()
		if err != nil {
			ready <- false
			return
		}
		ready <- true
	})
	bh.IsInitialised = <-ready
	bh.logger.Info("handle is ready to send information: %v", bh.IsInitialised)
	defer close(ready)
	return bh
}

func (bh *BufferHandle) createFile() error {
	fname := fmt.Sprintf("buffer_file_debug_log_%d.jsonl", bh.counter)
	file, err := os.Create(bh.dir + fname)
	if err != nil {
		bh.logger.Warnf("problem in file creation: %s", err.Error())
		return err
	}
	bh.file = file
	bh.writer = bufio.NewWriterSize(bh.file, bh.bufferCapacity)
	return nil
}

func (bh *BufferHandle) Send(input, output any, metainfo ht.MetaInfo) {
	bh.logger.Debugf("marshalling debugFields")
	debugFields := ht.DebugFields{
		Input:    input,
		Output:   output,
		Metainfo: metainfo,
	}
	debugFieldsBytes, marshalErr := jsonfast.Marshal(debugFields)
	if marshalErr != nil {
		bh.logger.Warnf("marshal error:%s", marshalErr.Error())
		return
	}
	bh.logger.Debugf("write to buffer")

	bh.ReadSyncBlock(func() {
		debugFieldsBytes = append(debugFieldsBytes, '\n')
		bytesWritten, wErr := bh.writer.Write(debugFieldsBytes)
		if wErr != nil {
			bh.logger.Warn("write to buffer:%s", wErr.Error())
			return
		}
		bh.bytesWritten += bytesWritten
	})

	bh.logger.Infof("Bytes written to buffer: %d", bh.bytesWritten)
	bh.logger.Infof("Max bytes for file rotation: %d", bh.maxBytesForFileRotation)

	bh.WriteSyncBlock(bh.rotateFile)
}

func (bh *BufferHandle) rotateFile() {
	if bh.bytesWritten >= bh.maxBytesForFileRotation {
		// Rotate file
		flushErr := bh.writer.Flush()
		if flushErr != nil {
			bh.logger.Warnf("flush:%v", flushErr.Error())
			return
		}
		defer bh.file.Close()
		bh.bytesWritten = 0
		bh.counter += 1

		err := bh.createFile()
		if err != nil {
			// error will already get logged
			return
		}
	}
}

func (bh *BufferHandle) WriteSyncBlock(f func()) {
	bh.mu.Lock()
	f()
	defer bh.mu.Unlock()
}

func (bh *BufferHandle) ReadSyncBlock(f func()) {
	bh.mu.RLock()
	f()
	defer bh.mu.RUnlock()
}

func (h *BufferHandle) Shutdown() {
	if h.writer != nil {
		h.writer.Flush()
		defer h.file.Close()
	}
}
