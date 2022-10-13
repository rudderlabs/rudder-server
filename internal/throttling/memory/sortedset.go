package memory

type sortedSet struct {
	getterSetter
}

func (s *sortedSet) limit(key string, cost, rate, period int64) (
	members []string, err error,
) {
	return nil, nil
}
