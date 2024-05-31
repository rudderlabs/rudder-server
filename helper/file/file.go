package file

import (
	"context"
	"fmt"
	"os"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	ht "github.com/rudderlabs/rudder-server/helper/types"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

type fields struct {
	Input    any         `json:"request"`
	Output   any         `json:"response"`
	Metainfo ht.MetaInfo `json:"metadata"`
}

type fileConfig struct {
	MaxBatchSize    int
	MaxBatchTimeout time.Duration
}

type FileHandle struct {
	dir                 string
	fileConfig          *fileConfig
	logger              logger.Logger
	fieldsChan          chan fields
	backgroundCtxCancel context.CancelFunc
	backgroundWait      func() error
}

func getFileConfig(parentMod string) *fileConfig {
	maxBatchSzConfPath := fmt.Sprintf("%s.DebugHelper.maxBatchSize", parentMod)
	maxBatchTimeoutConfPath := fmt.Sprintf("%s.DebugHelper.maxBatchTimeout", parentMod)

	return &fileConfig{
		MaxBatchTimeout: config.GetReloadableDurationVar(20, time.Second, maxBatchTimeoutConfPath).Load(),
		MaxBatchSize:    config.GetReloadableIntVar(200, 1, maxBatchSzConfPath).Load(),
	}
}

func WithDirectory(dir string) func(*FileHandle) {
	return func(fh *FileHandle) {
		fh.dir = dir
	}
}

func WithConfig(maxBatchSz int, maxBatchTimeout time.Duration) func(*FileHandle) {
	return func(fh *FileHandle) {
		fh.fileConfig = &fileConfig{
			MaxBatchSize:    maxBatchSz,
			MaxBatchTimeout: maxBatchTimeout,
		}
	}
}

func New(parentMod string, opts ...func(*FileHandle)) (*FileHandle, error) {
	fileConfig := getFileConfig(parentMod)
	fieldsChan := make(chan fields, fileConfig.MaxBatchSize)

	ctx, cancel := context.WithCancel(context.Background())
	g, _ := errgroup.WithContext(ctx)

	handle := &FileHandle{
		dir:                 "mydebuglogs/",
		fileConfig:          fileConfig,
		logger:              logger.NewLogger().Child("helper.file"),
		fieldsChan:          fieldsChan,
		backgroundCtxCancel: cancel,
		backgroundWait:      g.Wait,
	}
	for _, opt := range opts {
		opt(handle)
	}
	g.Go(misc.WithBugsnag(func() error {
		handle.writeToFile()
		return nil
	}))
	return handle, nil
}

func (h *FileHandle) Send(input, output any, metainfo ht.MetaInfo) {
	h.fieldsChan <- fields{
		Input:    input,
		Output:   output,
		Metainfo: metainfo,
	}
}

func (h *FileHandle) writeToFile() {
	for {
		fieldsBuffer, noOfFieldsBatched, _, isFieldsChanOpen := lo.BufferWithTimeout(h.fieldsChan, h.fileConfig.MaxBatchSize, h.fileConfig.MaxBatchTimeout)
		h.logger.Debug("BufferWithTimeout completed")
		if noOfFieldsBatched > 0 {
			if _, err := os.Stat(h.dir); os.IsNotExist(err) {
				// file does not exist
				_ = os.MkdirAll(h.dir, os.ModePerm)
			}
			h.logger.Debug("start writing to file")
			filename := fmt.Sprintf("debug_helper_%s.json", time.Now().UTC().Format(time.RFC3339))
			file, _ := os.OpenFile(fmt.Sprintf("%s/%s", h.dir, filename), os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
			defer file.Close()

			encoder := jsonfast.NewEncoder(file)
			err := encoder.Encode(fieldsBuffer)
			if err != nil {
				h.logger.Errorn("writing to file", obskit.Error(err))
				continue
			}
			h.logger.Debug("Completed Writing to file")
		}
		if !isFieldsChanOpen {
			// what does this mean ?
			return
		}
	}
}

func (h *FileHandle) Shutdown() {
	h.backgroundCtxCancel()
	close(h.fieldsChan)
	_ = h.backgroundWait()
}
