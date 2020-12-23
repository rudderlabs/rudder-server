package dedup

var (
	dedupManager DedupI
)

// GetInstance returns an instance of DedupI
func GetInstance(clearDB *bool) DedupI {
	pkgLogger.Info("[[ Dedup ]] Setting up Dedup Manager")
	if dedupManager == nil {
		handler := &DedupHandleT{}
		handler.setup(clearDB)
		dedupManager = handler
	}
	return dedupManager
}
