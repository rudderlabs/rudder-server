package runner

var customBuckets = map[string][]float64{
	"gateway.response_time": {
		0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 60,
		300, /* 5 mins */
	},
}
