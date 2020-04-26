package jobsdb

import "sync"

//SequenceProvider maintains a sequenceId increments it based on request atomically
type SequenceProvider struct {
	sequenceAssignerLock sync.RWMutex
	sequenceID           int64
}

func (sequenceProvider *SequenceProvider) init(startValue int64) {
	sequenceProvider.sequenceID = startValue
}

//NewSequenceProvider initializes a sequenceprovider with a start value and returns the provider
func NewSequenceProvider(startValue int64) *SequenceProvider {
	sequenceProvider := SequenceProvider{}
	sequenceProvider.init(startValue)
	return &sequenceProvider
}

//ReserveIds returns a sequence number (seq) and the user can use ids from seq to seq + count -1
func (sequenceProvider *SequenceProvider) ReserveIds(count int) int64 {
	sequenceProvider.sequenceAssignerLock.Lock()
	defer sequenceProvider.sequenceAssignerLock.Unlock()
	sequenceIDToReturn := sequenceProvider.sequenceID
	sequenceProvider.sequenceID = sequenceProvider.sequenceID + int64(count)
	return sequenceIDToReturn
}

//IsInitialized tells if the sequence provider is initialized or not
func (sequenceProvider *SequenceProvider) IsInitialized() bool {
	return sequenceProvider.sequenceID != 0
}
