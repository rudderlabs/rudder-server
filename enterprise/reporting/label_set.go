package reporting

import (
	"encoding/hex"
	"strconv"

	"github.com/spaolacci/murmur3"

	"github.com/rudderlabs/rudder-server/utils/types"
)

type LabelSet struct {
	WorkspaceID             string
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
	ErrorCode               string
	ErrorMessage            string
	Bucket                  int64
}

func NewLabelSet(metric types.PUReportedMetric, bucket int64) LabelSet {
	return LabelSet{
		WorkspaceID:             metric.SourceID,
		SourceDefinitionID:      metric.SourceDefinitionID,
		SourceCategory:          metric.SourceCategory,
		SourceID:                metric.SourceID,
		DestinationDefinitionID: metric.DestinationDefinitionID,
		DestinationID:           metric.DestinationID,
		SourceTaskRunID:         metric.SourceTaskRunID,
		SourceJobID:             metric.SourceJobID,
		SourceJobRunID:          metric.SourceJobRunID,
		TransformationID:        metric.TransformationID,
		TransformationVersionID: metric.TransformationVersionID,
		TrackingPlanID:          metric.TrackingPlanID,
		TrackingPlanVersion:     metric.TrackingPlanVersion,
		InPU:                    metric.InPU,
		PU:                      metric.PU,
		Status:                  metric.StatusDetail.Status,
		TerminalState:           metric.TerminalPU,
		InitialState:            metric.InitialPU,
		StatusCode:              metric.StatusDetail.StatusCode,
		EventName:               metric.StatusDetail.EventName,
		EventType:               metric.StatusDetail.EventType,
		ErrorType:               metric.StatusDetail.ErrorType,
		Bucket:                  bucket,
		ErrorCode:               metric.StatusDetail.ErrorDetails.Code,
		ErrorMessage:            metric.StatusDetail.ErrorDetails.Message,
	}
}

func (labelSet LabelSet) generateHash() string {
	data := labelSet.WorkspaceID + labelSet.SourceDefinitionID + labelSet.SourceCategory + labelSet.SourceID + labelSet.DestinationDefinitionID + labelSet.DestinationID + labelSet.SourceTaskRunID + labelSet.SourceJobID + labelSet.SourceJobRunID + labelSet.TransformationID + labelSet.TransformationVersionID + labelSet.TrackingPlanID + strconv.Itoa(labelSet.TrackingPlanVersion) + labelSet.InPU + labelSet.PU + labelSet.Status + strconv.FormatBool(labelSet.TerminalState) + strconv.FormatBool(labelSet.InitialState) + strconv.Itoa(labelSet.StatusCode) + labelSet.EventName + labelSet.EventType + labelSet.ErrorType + strconv.FormatInt(labelSet.Bucket, 10) + labelSet.ErrorCode + labelSet.ErrorMessage
	hash := murmur3.Sum64([]byte(data))
	return hex.EncodeToString([]byte(strconv.FormatUint(hash, 16)))
}
