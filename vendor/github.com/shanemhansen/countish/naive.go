package countish

type Counter interface {
	Observe(string)
	ItemsAboveThreshold(float64) []Entry
}

type naiveSampler struct {
	vals map[string]uint64
	N    uint64
}

func NewNaiveSampler() *naiveSampler {
	return &naiveSampler{
		vals: make(map[string]uint64),
	}
}

func (ns *naiveSampler) Observe(key string) {
	ns.vals[key]++
	ns.N++
}

func (ns *naiveSampler) ItemsAboveThreshold(val float64) []Entry {
	count := uint64(val * float64(ns.N))
	var entries []Entry
	for key, val := range ns.vals {
		if val >= count {
			entries = append(entries, Entry{Key: key, Frequency: float64(val) / float64(ns.N)})
		}
	}
	return entries
}
