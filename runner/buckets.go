package runner

var customBuckets = map[string][]float64{
	"gateway.request_size": {
		10,       // 10 bytes
		100,      // 100 bytes
		1000,     // 1kb
		10000,    // 10kb
		100000,   // 100kb
		1000000,  // 1mb
		10000000, // 10mb
	},
	"gateway.response_time": {
		0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 60,
	},
	"gateway.user_suppression_age": {
		86400, 432000, 864000, 2592000, 5184000, 7776000, 15552000, 31104000, // 1 day, 5 days, 10 days, 30 days, 60 days, 90 days, 180 days, 360 days
	},
}
