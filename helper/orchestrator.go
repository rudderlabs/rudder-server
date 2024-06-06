package helper

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	buf "github.com/rudderlabs/rudder-server/helper/buffer"
	fh "github.com/rudderlabs/rudder-server/helper/file"
	"github.com/rudderlabs/rudder-server/helper/noop"
	ht "github.com/rudderlabs/rudder-server/helper/types"
)

type Debugger interface {
	Send(input, output any, meta ht.MetaInfo)
	Shutdown()
}

type DebugHandle struct {
	handler     Debugger
	debugConfig *ht.DebugConfig
	debugMeta   *ht.DebuggableMetaInfo
	logger      logger.Logger
}

func getDebugConfig(parentMod string, conf *config.Config) (*ht.DebugConfig, error) {
	startTimeConfPath := fmt.Sprintf("%s.DebugHelper.startTime", parentMod)
	endTimeConfPath := fmt.Sprintf("%s.DebugHelper.endTime", parentMod)

	debugConfig := &ht.DebugConfig{}

	stTime := conf.GetReloadableStringVar("", startTimeConfPath).Load()
	if stTime != "" {
		startTime, err := time.Parse(time.RFC3339, stTime)
		if err != nil {
			return nil, fmt.Errorf("start time parse:%w", err)
		}
		debugConfig.StartTime = startTime
	}

	eTime := conf.GetReloadableStringVar("", endTimeConfPath).Load()
	if eTime != "" {
		endTime, enderr := time.Parse(time.RFC3339, eTime)
		if enderr != nil {
			return nil, fmt.Errorf("start time parse:%w", enderr)
		}
		debugConfig.EndTime = endTime
		if debugConfig.EndTime.Compare(debugConfig.StartTime) <= 0 {
			return nil, errors.New("endtime cannot be less than startTime")
		}
	}

	return debugConfig, nil
}

func getDebugMeta(parentMod string, conf *config.Config) (*ht.DebuggableMetaInfo, error) {
	enabledConfPath := fmt.Sprintf("%s.DebugHelper.enabled", parentMod)
	enabled := conf.GetReloadableBoolVar(false, enabledConfPath).Load()
	if !enabled {
		return nil, errors.New("debugger not enabled")
	}

	destIDsConfPath := fmt.Sprintf("%s.DebugHelper.destinationIDs", parentMod)
	workspaceIDsConfPath := fmt.Sprintf("%s.DebugHelper.workspaceIDs", parentMod)
	eventNamesConfPath := fmt.Sprintf("%s.DebugHelper.eventNames", parentMod)
	destTypesConfPath := fmt.Sprintf("%s.DebugHelper.destTypes", parentMod)

	debugMetaInfo := &ht.DebuggableMetaInfo{
		Enabled:        enabled,
		DestTypes:      conf.GetReloadableStringSliceVar([]string{}, destTypesConfPath).Load(),
		WorkspaceIDs:   conf.GetReloadableStringSliceVar([]string{}, workspaceIDsConfPath).Load(),
		DestinationIDs: conf.GetReloadableStringSliceVar([]string{}, destIDsConfPath).Load(),
		EventNames:     conf.GetReloadableStringSliceVar([]string{}, eventNamesConfPath).Load(),
	}

	return debugMetaInfo, nil
}

func getDebugger(parentModule string, conf *config.Config) Debugger {
	debugType := conf.GetReloadableStringVar("noop", fmt.Sprintf("%s.DebugHelper.type", parentModule)).Load()
	var debugger Debugger
	var err error
	switch {
	case debugType == "file":
		debugger, err = fh.New("./router/debuglogs/", fh.WithOptsFromConfig(parentModule, conf))
		if err != nil {
			debugger, _ = noop.New()
		}
	case debugType == "buffer_file":
		debugger = buf.New("./router/debuglogs/", buf.WithOptsFromConfig(parentModule, conf))
	default:
		debugger, _ = noop.New()
	}
	return debugger
}

func New(parentModule string, conf *config.Config) Debugger {
	debugger := getDebugger(parentModule, conf)

	debugMetaInfo, err := getDebugMeta(parentModule, conf)
	if err != nil {
		debugger, _ = noop.New()
	}

	var debugConfig *ht.DebugConfig
	debugConfig, err = getDebugConfig(parentModule, conf)
	if err != nil {
		debugger, _ = noop.New()
	}

	return &DebugHandle{
		handler:     debugger,
		debugConfig: debugConfig,
		debugMeta:   debugMetaInfo,
		logger:      logger.NewLogger().Child("debughelper"),
	}
}

func (d *DebugHandle) isDebuggable(metainfo ht.MetaInfo) bool {
	fields := []logger.Field{
		obskit.DestinationID(metainfo.DestinationID),
		obskit.DestinationType(metainfo.DestType),
	}
	d.logger.Debugn("isDebuggable", fields...)
	if d.debugMeta == nil {
		d.logger.Debugn("debug meta is nil", fields...)
		return false
	}
	now := time.Now()
	// TODO: Think about clean-up activity when endTime > 0 && after-endTime
	if (!d.debugConfig.StartTime.IsZero() && now.Before(d.debugConfig.StartTime)) || (!d.debugConfig.EndTime.IsZero() && now.After(d.debugConfig.EndTime)) {
		d.logger.Debugn("before startTime or after endTime", fields...)
		return false
	}
	isDebuggable := d.debugMeta.Enabled && ((lo.Contains(d.debugMeta.DestinationIDs, metainfo.DestinationID) ||
		lo.Contains(d.debugMeta.WorkspaceIDs, metainfo.WorkspaceID) ||
		lo.Contains(d.debugMeta.DestTypes, metainfo.DestType)) && lo.Contains(d.debugMeta.EventNames, metainfo.EventName))
	d.logger.Debugn(fmt.Sprintf("evaluated isDebuggable to %s", strconv.FormatBool(isDebuggable)), fields...)
	return isDebuggable
}

func (d *DebugHandle) Send(input, output any, meta ht.MetaInfo) {
	if !d.isDebuggable(meta) {
		return
	}
	d.handler.Send(input, output, meta)
}

func (d *DebugHandle) Shutdown() {
	d.handler.Shutdown()
}
