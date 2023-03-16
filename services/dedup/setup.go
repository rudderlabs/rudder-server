package dedup

import (
	"sync"
)

var (
	dedupManager Dedup
	once         sync.Once
)

// GetInstance returns an instance of Dedup
func GetInstance(clearDB *bool) Dedup {
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
