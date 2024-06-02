package runner

import (
	"github.com/rudderlabs/rudder-go-kit/bytesize"
)

var (
	defaultHistogramBuckets = []float64{
		0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 60, 300, 600, // 2ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s, 30s, 1m
	}
	defaultWarehouseHistogramBuckets = []float64{
		0.1, 0.25, 0.5, 1, 2.5, 5, 10, 60, 300, 600, // 0.1s, 0.25s, 0.5s, 1s, 2.5s, 5s, 10s, 1m, 5m, 10m
	}

	customBucketsServer = map[string][]float64{
		"event_delivery_time": {
			0.5, 1, 2.5, 5, 10, 30, 60, 300, 600, 1800, 3600, 7200, 10800, 21600, 32400, 86400, // 0.5 seconds, 1 second, 2.5 seconds, 5 seconds, 10 seconds, 30 seconds, 1 minute, 5 minutes, 10 minutes, 30 minutes, 1 hour, 2 hours, 3 hours, 6 hours, 9 hours, 24 hours
		},
	}
	customBucketsWarehouse = map[string][]float64{
		"event_delivery_time": {
			60, 300, 600, 1800, 3600, 5400, 12600, 23400, 45000, 88200, // 1 minute, 5 minutes, 10 minutes, 30 minutes, 1 hour, 1.5 hours, 3.5 hours, 6.5 hours, 12.5 hours, 24.5 hours
		},
	}

	customBuckets = map[string][]float64{
		"exporting_data": {
			0.1, 0.25, 0.5, 1, 2.5, 5, 10, 60, 300, 600, 1800, 3600, 7200, 10800, 21600, 32400, 86400, // 0.1 seconds, 0.25 seconds, 0.5 seconds, 1 second, 2.5 seconds, 5 seconds, 10 seconds, 30 seconds, 1 minute, 5 minutes, 10 minutes, 30 minutes, 1 hour, 2 hours, 3 hours, 6 hours, 9 hours, 24 hours
		},

		"error_detail_reports_size": {
			1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000,
		},
		"error_detail_reports_aggregated_size": {
			1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000,
		},
		"reporting_client_get_reports_count": {
			1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000,
		},
		"reporting_client_get_aggregated_reports_count": {
			1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000,
		},
		"csv_file_size": {
			float64(10 * bytesize.B), float64(100 * bytesize.B),
			float64(1 * bytesize.KB), float64(10 * bytesize.KB), float64(100 * bytesize.KB),
			float64(1 * bytesize.MB), float64(3 * bytesize.MB), float64(5 * bytesize.MB), float64(10 * bytesize.MB),
			float64(1 * bytesize.GB),
		},
		"payload_size": {
			float64(10 * bytesize.B), float64(100 * bytesize.B),
			float64(1 * bytesize.KB), float64(10 * bytesize.KB), float64(100 * bytesize.KB),
			float64(1 * bytesize.MB), float64(3 * bytesize.MB), float64(5 * bytesize.MB), float64(10 * bytesize.MB),
			float64(1 * bytesize.GB),
		},
		"backend_config_http_response_size": {
			float64(10 * bytesize.B), float64(100 * bytesize.B),
			float64(1 * bytesize.KB), float64(10 * bytesize.KB), float64(100 * bytesize.KB),
			float64(1 * bytesize.MB), float64(5 * bytesize.MB), float64(10 * bytesize.MB), float64(100 * bytesize.MB),
			float64(1 * bytesize.GB), float64(10 * bytesize.GB),
		},
		"gateway.batch_size": {
			1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000,
		},
		"router.kafka.batch_size": {
			1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000,
		},
		"gateway.request_size": {
			float64(10 * bytesize.B), float64(100 * bytesize.B),
			float64(1 * bytesize.KB), float64(10 * bytesize.KB), float64(100 * bytesize.KB),
			float64(1 * bytesize.MB), float64(3 * bytesize.MB), float64(5 * bytesize.MB), float64(10 * bytesize.MB),
		},
		"gateway.user_suppression_age": {
			86400, 432000, 864000, 2592000, 5184000, 7776000, 15552000, 31104000, // 1 day, 5 days, 10 days, 30 days, 60 days, 90 days, 180 days, 360 days
		},
		"processor.transformer_request_batch_count": {
			1, 5, 10, 25, 50, 100, 250, 500, 1000,
		},
		"processor_db_write_events": {
			1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000,
		},
		"processor_db_write_payload_bytes": {
			float64(10 * bytesize.B), float64(100 * bytesize.B),
			float64(1 * bytesize.KB), float64(10 * bytesize.KB), float64(100 * bytesize.KB),
			float64(1 * bytesize.MB), float64(5 * bytesize.MB), float64(10 * bytesize.MB), float64(100 * bytesize.MB),
			float64(1 * bytesize.GB),
		},
		"processor_db_read_events": {
			1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000,
		},
		"processor_db_read_payload_bytes": {
			float64(10 * bytesize.B), float64(100 * bytesize.B),
			float64(1 * bytesize.KB), float64(10 * bytesize.KB), float64(100 * bytesize.KB),
			float64(1 * bytesize.MB), float64(5 * bytesize.MB), float64(10 * bytesize.MB), float64(100 * bytesize.MB),
			float64(1 * bytesize.GB),
		},
		"processor_db_read_requests": {
			1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000,
		},
	}
)
