package model

import "sync"

type File struct {
	Path string
	Mu   *sync.RWMutex
}

var (
	MigrationFullExportSeqID   = "Migration.FullExport.seqID"
	MigrationSyncInProgress    = "Migration.FullExport.syncInProgress"
	MigrationSyncSeqIDOverride = "Migration.FullExport.syncSeqIDOverride"
)
