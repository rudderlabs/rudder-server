package runner

import (
	"github.com/rudderlabs/rudder-go-kit/bytesize"
)

var (
	defaultHistogramBuckets = []float64{
		0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 60, 300, 600, // 2ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s, 30s, 1m, 5m
	}
	defaultWarehouseHistogramBuckets = []float64{
		0.1, 0.25, 0.5, 1, 2.5, 5, 10, 60, 300, 600, 1800, 3600, 10800, // 0.1s, 0.25s, 0.5s, 1s, 2.5s, 5s, 10s, 1m, 5m, 10m, 30m, 1h, 3h
	}

	customBucketsServer = map[string][]float64{
		"event_delivery_time": {
			0.5, 1, 2.5, 5, 10, 30, 60, 300, 600, 1800, 3600, 7200, 10800, 21600, 32400, 86400, // 0.5s, 1s, 2.5s, 5s, 10s, 30s, 1m, 5m, 10m, 30m, 1h, 2h, 3h, 6h, 9h, 24h
		},
	}
	customBucketsWarehouse = map[string][]float64{
		"event_delivery_time": {
			60, 300, 900, 1800, 2100, 2700, 3900, 4500, 5400, 9900, 11100, 12600, 21600, 23400, 43200, 45000, 82800, 86400, 88200, 108000, 129600, // 1m, 5m, 15m, 30m, 35m, 45m, 1h5m, 1h15m, 1h30m, 2h45m, 3h5m, 3h30m, 6h, 6h30m, 12h, 12h30m, 23h, 24h, 24h30m, 30h, 36h
		},
		"warehouse_schema_size": {
			float64(10 * bytesize.B), float64(100 * bytesize.B),
			float64(1 * bytesize.KB), float64(10 * bytesize.KB), float64(100 * bytesize.KB),
			float64(1 * bytesize.MB), float64(3 * bytesize.MB), float64(5 * bytesize.MB), float64(10 * bytesize.MB),
			float64(25 * bytesize.MB), float64(50 * bytesize.MB), float64(100 * bytesize.MB), float64(1 * bytesize.GB),
		},
	}

	customBuckets = map[string][]float64{
		"db_wait_duration": {
			0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 60, 300, 600, 1800, 3600, 7200, 10800, 21600, 32400, 86400, // 2ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s, 1m, 5m, 10m, 30m, 1h, 2h, 3h, 6h, 9h, 24h
		},

		"exporting_data": {
			0.1, 0.25, 0.5, 1, 2.5, 5, 10, 60, 300, 600, 1800, 3600, 7200, 10800, 21600, 32400, 86400, // 0.1s, 0.25s, 0.5s, 1s, 2.5s, 5s, 10s, 1m, 5m, 10m, 30m, 1h, 2h, 3h, 6h, 9h, 24h
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
		"reporting_event_sampler_request_duration_seconds": {
			// 1ms, 5ms, 10ms, 25ms, 50ms, 100ms
			0.001, 0.005, 0.01, 0.025, 0.05, 0.1,
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
		"router.kafka.batch_size": {
			1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000,
		},
		"processor.event_pickup_lag_seconds": {
			// 0.1s, 0.5s, 1s, 5s, 1m, 5m, 10m, 30m, 1h, 12h, 24h
			0.1, 0.5, 1, 5, 60, 300, 600, 1800, 3600, 12 * 3600, 24 * 3600,
		},
		"gateway.event_pickup_lag_seconds": {
			// 0.1s, 0.5s, 1s, 5s, 1m, 5m, 10m, 30m, 1h, 12h, 24h
			0.1, 0.5, 1, 5, 60, 300, 600, 1800, 3600, 12 * 3600, 24 * 3600,
		},
		"tracked_users_hll_bytes": {
			float64(10 * bytesize.B),  // for hll containing single id = 8B
			float64(100 * bytesize.B), // for hll containing 10 ids = 80B
			float64(400 * bytesize.B), // for hll containing 50 ids = 400B
			float64(1 * bytesize.KB),  // for hll containing 100 ids = 800B
			float64(3 * bytesize.KB),  // for hll containing 300 ids = 2400B
			float64(6 * bytesize.KB),  // for hll containing 700 ids = 5600B
			float64(10 * bytesize.KB), // max size for hll with log2m=14, regWidth=5 = 10KB
			float64(41 * bytesize.KB), // max size for hll with log2m=16, regWidth=5 = 40KB
		},
		"processor_tracked_users_report_gen_seconds": {
			// 1microsecond, 2.5microsecond, 5microsecond, 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s
			0.00001, 0.00025, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1,
		},
		"router_delivery_payload_size_bytes": {
			float64(1 * bytesize.KB), float64(10 * bytesize.KB), float64(100 * bytesize.KB),
			float64(1 * bytesize.MB), float64(3 * bytesize.MB), float64(5 * bytesize.MB), float64(10 * bytesize.MB),
		},
		"snowpipe_streaming_request_body_size": {
			float64(10 * bytesize.B), float64(100 * bytesize.B),
			float64(1 * bytesize.KB), float64(10 * bytesize.KB), float64(100 * bytesize.KB),
			float64(1 * bytesize.MB), float64(3 * bytesize.MB), float64(5 * bytesize.MB), float64(10 * bytesize.MB),
		},
		"throttling": {
			// 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s
			0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1,
		},

		"warehouse_dest_transform_mismatched_events": {
			0.1, 0.25, 0.5, 1, 2.5, 5, 7.5, 10, 12.5, 15, 30, 60, 120, 300, 600, 900, 1800, 3600,
		},
		"warehouse_dest_transform_input_events": {
			1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 12500, 15000, 30000, 50000, 75000, 100000,
		},
		"warehouse_dest_transform_output_events": {
			1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 12500, 15000, 30000, 50000, 75000, 100000,
		},
		"warehouse_dest_transform_output_failed_events": {
			1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 12500, 15000, 30000, 50000, 75000, 100000,
		},
	}
)
