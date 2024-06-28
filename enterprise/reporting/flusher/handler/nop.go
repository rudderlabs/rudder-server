package handler

import (
	"errors"

	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/report"
)

type NOP struct{}

func (n *NOP) Decode(report report.RawReport) (report.DecodedReport, error) {
	ErrInvalidValue := errors.New("invalid value")
	return nil, ErrInvalidValue
}

func (n *NOP) Aggregate(aggReport, report report.DecodedReport) error {
	return nil
}
