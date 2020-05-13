package jobsdb

import "sync"

//SequenceProviderT maintains a sequenceId increments it based on request atomically
type SequenceProviderT struct {
	sequenceAssignerLock sync.RWMutex
	sequenceID           int64
}

func (sequenceProvider *SequenceProviderT) init(startValue int64) {
	sequenceProvider.sequenceID = startValue
}

//NewSequenceProvider initializes a sequenceprovider with a start value and returns the provider
func NewSequenceProvider(startValue int64) *SequenceProviderT {
	sequenceProvider := SequenceProviderT{}
	sequenceProvider.init(startValue)
	return &sequenceProvider
}

//ReserveIdsAndProvideStartSequence returns a sequence number (seq) and the user can use ids from seq to seq + count -1
func (sequenceProvider *SequenceProviderT) ReserveIdsAndProvideStartSequence(count int) int64 {
	sequenceProvider.sequenceAssignerLock.Lock()
	defer sequenceProvider.sequenceAssignerLock.Unlock()
	sequenceIDToReturn := sequenceProvider.sequenceID
	sequenceProvider.sequenceID = sequenceProvider.sequenceID + int64(count)
	return sequenceIDToReturn
}

//IsInitialized tells if the sequence provider is initialized or not
func (sequenceProvider *SequenceProviderT) IsInitialized() bool {
	return sequenceProvider.sequenceID != 0
}
