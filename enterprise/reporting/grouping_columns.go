package reporting

import (
	"crypto/md5"
	"encoding/hex"
	"strconv"

	"github.com/rudderlabs/rudder-server/utils/types"
)

type GroupingColumns struct {
	WorkspaceID string
	// Namespace               string
	// InstanceID              string
	SourceDefinitionID      string
	SourceCategory          string
	SourceID                string
	DestinationDefinitionID string
	DestinationID           string
	SourceTaskRunID         string
	SourceJobID             string
	SourceJobRunID          string
	TransformationID        string
	TransformationVersionID string
	TrackingPlanID          string
	TrackingPlanVersion     int
	InPU                    string
	PU                      string
	Status                  string
	TerminalState           bool
	InitialState            bool
	StatusCode              int
	EventName               string
	EventType               string
	ErrorType               string
}

func NewGroupingColumns(metric types.PUReportedMetric) GroupingColumns {
	return GroupingColumns{
		WorkspaceID:             metric.ConnectionDetails.SourceID,
		SourceDefinitionID:      metric.ConnectionDetails.SourceDefinitionId,
		SourceCategory:          metric.ConnectionDetails.SourceCategory,
		SourceID:                metric.ConnectionDetails.SourceID,
		DestinationDefinitionID: metric.ConnectionDetails.DestinationDefinitionId,
		DestinationID:           metric.ConnectionDetails.DestinationID,
		SourceTaskRunID:         metric.ConnectionDetails.SourceTaskRunID,
		SourceJobID:             metric.ConnectionDetails.SourceJobID,
		SourceJobRunID:          metric.ConnectionDetails.SourceJobRunID,
		TransformationID:        metric.ConnectionDetails.TransformationID,
		TransformationVersionID: metric.ConnectionDetails.TransformationVersionID,
		TrackingPlanID:          metric.ConnectionDetails.TrackingPlanID,
		TrackingPlanVersion:     metric.ConnectionDetails.TrackingPlanVersion,
		InPU:                    metric.PUDetails.InPU,
		PU:                      metric.PUDetails.PU,
		Status:                  metric.StatusDetail.Status,
		TerminalState:           metric.PUDetails.TerminalPU,
		InitialState:            metric.PUDetails.InitialPU,
		StatusCode:              metric.StatusDetail.StatusCode,
		EventName:               metric.StatusDetail.EventName,
		EventType:               metric.StatusDetail.EventType,
		ErrorType:               metric.StatusDetail.ErrorType,
	}
}

func (groupingColumns GroupingColumns) generateHash() string {
	data := groupingColumns.WorkspaceID + groupingColumns.SourceDefinitionID + groupingColumns.SourceCategory + groupingColumns.SourceID + groupingColumns.DestinationDefinitionID + groupingColumns.DestinationID + groupingColumns.SourceTaskRunID + groupingColumns.SourceJobID + groupingColumns.SourceJobRunID + groupingColumns.TransformationID + groupingColumns.TransformationVersionID + groupingColumns.TrackingPlanID + strconv.Itoa(groupingColumns.TrackingPlanVersion) + groupingColumns.InPU + groupingColumns.PU + groupingColumns.Status + strconv.FormatBool(groupingColumns.TerminalState) + strconv.FormatBool(groupingColumns.InitialState) + strconv.Itoa(groupingColumns.StatusCode) + groupingColumns.EventName + groupingColumns.EventType + groupingColumns.ErrorType
	hash := md5.Sum([]byte(data))
	return hex.EncodeToString(hash[:])
}
