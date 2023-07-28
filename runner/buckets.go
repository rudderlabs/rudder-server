package runner

var customBuckets = map[string][]float64{
	"gateway.response_time": {
		0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 60,
	},
	"gateway.user_suppression_age": {
		86400, 2592000, 5184000, 7776000, // 1 day, 1 month, 2 months, 3 months
	},
}
