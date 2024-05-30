package file

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/helper"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

type fields struct {
	Input    any             `json:"request"`
	Output   any             `json:"response"`
	Metainfo helper.MetaInfo `json:"metadata"`
}

type FileHandle struct {
	dir                 string
	debugMeta           helper.DebuggableMetaInfo
	logger              logger.Logger
	fieldsChan          chan fields
	backgroundCtxCancel context.CancelFunc
	backgroundWait      func() error
}

func getDebugMeta(parentMod string) (*helper.DebuggableMetaInfo, error) {
	enabledConfPath := fmt.Sprintf("%s.DebugHelper.enabled", parentMod)
	enabled := config.GetReloadableBoolVar(false, enabledConfPath).Load()
	if !enabled {
		return nil, errors.New("debugger not enabled")
	}

	destIDsConfPath := fmt.Sprintf("%s.DebugHelper.destinationIDs", parentMod)
	workspaceIDsConfPath := fmt.Sprintf("%s.DebugHelper.workspaceIDs", parentMod)
	eventNamesConfPath := fmt.Sprintf("%s.DebugHelper.eventNames", parentMod)
	destTypesConfPath := fmt.Sprintf("%s.DebugHelper.destTypes", parentMod)
	startTimeConfPath := fmt.Sprintf("%s.DebugHelper.startTime", parentMod)
	endTimeConfPath := fmt.Sprintf("%s.DebugHelper.endTime", parentMod)

	maxBatchSzConfPath := fmt.Sprintf("%s.DebugHelper.maxBatchSize", parentMod)
	maxBatchTimeoutConfPath := fmt.Sprintf("%s.DebugHelper.maxBatchTimeout", parentMod)

	debugMetaInfo := &helper.DebuggableMetaInfo{
		Enabled:         enabled,
		DestTypes:       config.GetReloadableStringSliceVar([]string{}, destTypesConfPath).Load(),
		WorkspaceIDs:    config.GetReloadableStringSliceVar([]string{}, workspaceIDsConfPath).Load(),
		DestinationIDs:  config.GetReloadableStringSliceVar([]string{}, destIDsConfPath).Load(),
		EventNames:      config.GetReloadableStringSliceVar([]string{}, eventNamesConfPath).Load(),
		MaxBatchTimeout: config.GetReloadableDurationVar(20, time.Second, maxBatchTimeoutConfPath).Load(),
		MaxBatchSize:    config.GetReloadableIntVar(200, 1, maxBatchSzConfPath).Load(),
	}

	stTime := config.GetReloadableStringVar("", startTimeConfPath).Load()
	if stTime != "" {
		startTime, err := time.Parse(time.RFC3339, config.GetReloadableStringVar("", startTimeConfPath).Load())
		if err != nil {
			return nil, fmt.Errorf("start time parse:%w", err)
		}
		debugMetaInfo.StartTime = startTime
	}

	eTime := config.GetReloadableStringVar("", endTimeConfPath).Load()
	if eTime != "" {
		endTime, enderr := time.Parse(time.RFC3339, config.GetReloadableStringVar("", endTimeConfPath).Load())
		if enderr != nil {
			return nil, fmt.Errorf("start time parse:%w", enderr)
		}
		debugMetaInfo.EndTime = endTime
		if debugMetaInfo.EndTime.Compare(debugMetaInfo.StartTime) <= 0 {
			return nil, errors.New("endtime cannot be less than startTime")
		}
	}

	return debugMetaInfo, nil
}

func NewFileDebugger(directory, parentMod string) (*FileHandle, error) {
	debugMeta, err := getDebugMeta(parentMod)
	if err != nil {
		return nil, err
	}
	fieldsChan := make(chan fields, debugMeta.MaxBatchSize)

	ctx, cancel := context.WithCancel(context.Background())
	g, _ := errgroup.WithContext(ctx)

	handle := &FileHandle{
		dir:                 directory,
		debugMeta:           *debugMeta,
		logger:              logger.NewLogger().Child("helper.file"),
		fieldsChan:          fieldsChan,
		backgroundCtxCancel: cancel,
		backgroundWait:      g.Wait,
	}
	g.Go(misc.WithBugsnag(func() error {
		handle.writeToFile()
		return nil
	}))
	return handle, nil
}

func (h *FileHandle) isDebuggable(metainfo helper.MetaInfo) bool {
	h.logger.Debugn("isDebuggable",
		obskit.DestinationID(metainfo.DestinationID),
		obskit.DestinationType(metainfo.DestType),
	)
	return h.debugMeta.Enabled && ((lo.Contains(h.debugMeta.DestinationIDs, metainfo.DestinationID) ||
		lo.Contains(h.debugMeta.WorkspaceIDs, metainfo.WorkspaceID) ||
		lo.Contains(h.debugMeta.DestTypes, metainfo.DestType)) && lo.Contains(h.debugMeta.EventNames, metainfo.EventName))
}

func (h *FileHandle) Send(input, output any, metainfo helper.MetaInfo) {
	now := time.Now()
	h.logger.Debugn("Before timerange validation",
		obskit.DestinationID(metainfo.DestinationID),
		obskit.DestinationType(metainfo.DestType),
		logger.NewField("value", now.Before(h.debugMeta.StartTime) || now.After(h.debugMeta.EndTime)),
	)
	if (!h.debugMeta.StartTime.IsZero() && now.Before(h.debugMeta.StartTime)) || (!h.debugMeta.EndTime.IsZero() && now.After(h.debugMeta.EndTime)) {
		return
	}
	h.logger.Debugn("Valid timerange",
		obskit.DestinationID(metainfo.DestinationID),
		obskit.DestinationType(metainfo.DestType),
	)
	if h.isDebuggable(metainfo) {
		h.fieldsChan <- fields{
			Input:    input,
			Output:   output,
			Metainfo: metainfo,
		}
	}
}

func (h *FileHandle) writeToFile() {
	for {
		fieldsBuffer, noOfFieldsBatched, _, isFieldsChanOpen := lo.BufferWithTimeout(h.fieldsChan, h.debugMeta.MaxBatchSize, h.debugMeta.MaxBatchTimeout)
		h.logger.Debug("BufferWithTimeout completed")
		if noOfFieldsBatched > 0 {
			if _, err := os.Stat(h.dir); os.IsNotExist(err) {
				// your file does not exist
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
