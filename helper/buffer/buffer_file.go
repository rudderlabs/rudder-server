package buffer

import (
	"bufio"
	"fmt"
	"os"

	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	ht "github.com/rudderlabs/rudder-server/helper/types"
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

type BufferHandle struct {
	logger       logger.Logger
	dir          string
	counter      int
	file         *os.File
	writer       *bufio.Writer
	maxBatchSize int
}

func WithDirectory(dir string) func(*BufferHandle) {
	return func(bh *BufferHandle) {
		bh.dir = dir
	}
}

func New(parentMod string, opts ...func(*BufferHandle)) *BufferHandle {
	maxSizeinKBKey := fmt.Sprintf("%s.DebugHelper.maxBatchSizeInKB", parentMod)
	maxSizeInKBF := config.GetReloadableInt64Var(int64(1), 1, maxSizeinKBKey).Load()
	bh := &BufferHandle{
		dir:          "mydebuglogs/",
		logger:       logger.NewLogger().Child("helper.buffer_file"),
		maxBatchSize: int(maxSizeInKBF * bytesize.B),
	}
	for _, opt := range opts {
		opt(bh)
	}
	return bh
}

func (h *BufferHandle) Send(input, output any, metainfo ht.MetaInfo) {
	if _, err := os.Stat(h.dir); os.IsNotExist(err) {
		// file does not exist
		_ = os.MkdirAll(h.dir, os.ModePerm)
	}
	fname := fmt.Sprintf("buffer_file_debug_log_%d.json", h.counter)
	_, err := os.Stat(fname)
	switch {
	case os.IsNotExist(err):
		h.file, err = os.Create(h.dir + fname)
		if err != nil {
			h.logger.Warnf("problem in file creation: %s", err.Error())
			return
		}
		h.writer = bufio.NewWriterSize(h.file, h.maxBatchSize)
	case err != os.ErrExist && err != nil:
		h.logger.Warnf("some problem with file: %s", err.Error())
		return
	}

	h.logger.Infof("marshalling debugFields")
	debugFields := ht.DebugFields{
		Input:    input,
		Output:   output,
		Metainfo: metainfo,
	}
	debugFieldsBytes, marshalErr := jsonfast.Marshal(debugFields)
	if marshalErr != nil {
		h.logger.Warnf("marshal error:%s", marshalErr.Error())
		return
	}
	h.logger.Infof("write to buffer")

	_, wErr := h.writer.Write(debugFieldsBytes)
	if wErr != nil {
		h.logger.Warn("write to buffer:%s", wErr.Error())
		return
	}

	h.logger.Infof("write to buffer complete")

	if h.writer != nil && h.writer.Available() == 0 {
		h.logger.Infof("no available buffer space")
		flushErr := h.writer.Flush()
		if flushErr != nil {
			h.logger.Warnf("flusing: %s", flushErr.Error())
			return
		}
		h.counter += 1
		defer h.file.Close()
	}
}

func (h *BufferHandle) Shutdown() {
	if h.writer != nil {
		// h.writer.Available() == h.writer.Size() // there is no buffer to write
		if h.writer.Available() < h.writer.Size() {
			h.writer.Flush()
			defer h.file.Close()
		}
	}
}
