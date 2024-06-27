//go:generate mockgen -destination=./handler_mock.go -package=handler -source=./handler.go Handler
package handler

import "github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/report"

type Handler interface {
	Decode(r report.RawReport) (report.DecodedReport, error)
	Aggregate(aggR report.DecodedReport, r report.DecodedReport) error
}
