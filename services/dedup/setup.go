package dedup

import (
	"fmt"
	"os"
	"sync"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	dedupManager DedupI
	once         sync.Once
)

// GetInstance returns an instance of DedupI
func GetInstance(clearDB *bool) DedupI {
	pkgLogger.Info("[[ Dedup ]] Setting up Dedup Manager")
	defer cleanUpV2Data()
	once.Do(func() {
		opts := []OptFn{FromConfig()}
		if clearDB != nil && *clearDB {
			opts = append(opts, WithClearDB())
		}

		dedupManager = New(DefaultRudderPath(), opts...)
	})

	return dedupManager
}

func cleanUpV2Data() {
	// clean up badgerDBv2 data
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return
	}
	_ = os.RemoveAll(fmt.Sprintf(`%v/badgerdbv2`, tmpDirPath))
}
