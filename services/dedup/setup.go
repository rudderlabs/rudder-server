package dedup

import (
	"sync"
)

var (
	dedupManager DedupI
	once         sync.Once
)

// GetInstance returns an instance of DedupI
func GetInstance(clearDB *bool) DedupI {
	pkgLogger.Info("[[ Dedup ]] Setting up Dedup Manager")
	once.Do(func() {
		opts := []OptFn{FromConfig()}
		if clearDB != nil && *clearDB {
			opts = append(opts, WithClearDB())
		}

		dedupManager = New(DefaultRudderPath(), opts...)
	})

	return dedupManager
}
