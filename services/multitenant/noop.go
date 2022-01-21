package multitenant

var NOOP = noop{}

type noop struct{}

func (*noop) CalculateSuccessFailureCounts(customer string, destType string, isSuccess bool, isDrained bool) {
}

func (*noop) GenerateSuccessRateMap(destType string) (map[string]float64, map[string]float64) {
	return nil, nil
}

func (*noop) AddToInMemoryCount(customerID string, destinationType string, count int, tableType string) {
}
