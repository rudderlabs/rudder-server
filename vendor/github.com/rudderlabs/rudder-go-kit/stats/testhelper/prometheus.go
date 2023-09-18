package testhelper

import (
	"io"

	promClient "github.com/prometheus/client_model/go"
	promParser "github.com/prometheus/common/expfmt"
)

// ParsePrometheusMetrics parses the given Prometheus metrics and returns a map of metric name to metric family.
func ParsePrometheusMetrics(rdr io.Reader) (map[string]*promClient.MetricFamily, error) {
	var parser promParser.TextParser
	mf, err := parser.TextToMetricFamilies(rdr)
	if err != nil {
		return nil, err
	}
	return mf, nil
}
