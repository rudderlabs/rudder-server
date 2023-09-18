// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pulsar

import (
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/bits-and-blooms/bitset"
)

type ackGroupingTracker interface {
	add(id MessageID)

	addCumulative(id MessageID)

	isDuplicate(id MessageID) bool

	flush()

	flushAndClean()

	close()
}

func newAckGroupingTracker(options *AckGroupingOptions,
	ackIndividual func(id MessageID),
	ackCumulative func(id MessageID),
	ackList func(ids []*pb.MessageIdData)) ackGroupingTracker {
	if options == nil {
		options = &AckGroupingOptions{
			MaxSize: 1000,
			MaxTime: 100 * time.Millisecond,
		}
	}

	if options.MaxSize <= 1 {
		return &immediateAckGroupingTracker{
			ackIndividual: ackIndividual,
			ackCumulative: ackCumulative,
		}
	}

	t := &timedAckGroupingTracker{
		maxNumAcks:        int(options.MaxSize),
		ackCumulative:     ackCumulative,
		ackList:           ackList,
		pendingAcks:       make(map[[2]uint64]*bitset.BitSet),
		lastCumulativeAck: EarliestMessageID(),
	}

	if options.MaxTime > 0 {
		t.ticker = time.NewTicker(options.MaxTime)
		t.exitCh = make(chan struct{})
		go func() {
			for {
				select {
				case <-t.exitCh:
					return
				case <-t.ticker.C:
					t.flush()
				}
			}
		}()
	}

	return t
}

type immediateAckGroupingTracker struct {
	ackIndividual func(id MessageID)
	ackCumulative func(id MessageID)
}

func (i *immediateAckGroupingTracker) add(id MessageID) {
	i.ackIndividual(id)
}

func (i *immediateAckGroupingTracker) addCumulative(id MessageID) {
	i.ackCumulative(id)
}

func (i *immediateAckGroupingTracker) isDuplicate(id MessageID) bool {
	return false
}

func (i *immediateAckGroupingTracker) flush() {
}

func (i *immediateAckGroupingTracker) flushAndClean() {
}

func (i *immediateAckGroupingTracker) close() {
}

type timedAckGroupingTracker struct {
	sync.RWMutex

	maxNumAcks    int
	ackCumulative func(id MessageID)
	ackList       func(ids []*pb.MessageIdData)
	ticker        *time.Ticker

	// Key is the pair of the ledger id and the entry id,
	// Value is the bit set that represents which messages are acknowledged if the entry stores a batch.
	// The bit 1 represents the message has been acknowledged, i.e. the bits "111" represents all messages
	// in the batch whose batch size is 3 are not acknowledged.
	// After the 1st message (i.e. batch index is 0) is acknowledged, the bits will become "011".
	// Value is nil if the entry represents a single message.
	pendingAcks map[[2]uint64]*bitset.BitSet

	lastCumulativeAck     MessageID
	cumulativeAckRequired int32

	exitCh chan struct{}
}

func (t *timedAckGroupingTracker) add(id MessageID) {
	if acks := t.tryAddIndividual(id); acks != nil {
		t.flushIndividual(acks)
	}
}

func (t *timedAckGroupingTracker) tryAddIndividual(id MessageID) map[[2]uint64]*bitset.BitSet {
	t.Lock()
	defer t.Unlock()
	key := [2]uint64{uint64(id.LedgerID()), uint64(id.EntryID())}

	batchIdx := id.BatchIdx()
	batchSize := id.BatchSize()

	if batchIdx >= 0 && batchSize > 0 {
		bs, found := t.pendingAcks[key]
		if !found {
			if batchSize > 1 {
				bs = bitset.New(uint(batchSize))
				for i := uint(0); i < uint(batchSize); i++ {
					bs.Set(i)
				}
			}
			t.pendingAcks[key] = bs
		}
		if bs != nil {
			bs.Clear(uint(batchIdx))
		}
	} else {
		t.pendingAcks[key] = nil
	}

	if len(t.pendingAcks) >= t.maxNumAcks {
		pendingAcks := t.pendingAcks
		t.pendingAcks = make(map[[2]uint64]*bitset.BitSet)
		return pendingAcks
	}
	return nil
}

func (t *timedAckGroupingTracker) addCumulative(id MessageID) {
	if t.tryUpdateCumulative(id) && t.ticker == nil {
		t.ackCumulative(id)
	}
}

func (t *timedAckGroupingTracker) tryUpdateCumulative(id MessageID) bool {
	t.Lock()
	defer t.Unlock()
	if messageIDCompare(t.lastCumulativeAck, id) < 0 {
		t.lastCumulativeAck = id
		atomic.StoreInt32(&t.cumulativeAckRequired, 1)
		return true
	}
	return false
}

func (t *timedAckGroupingTracker) isDuplicate(id MessageID) bool {
	t.RLock()
	defer t.RUnlock()
	if messageIDCompare(t.lastCumulativeAck, id) >= 0 {
		return true
	}
	key := [2]uint64{uint64(id.LedgerID()), uint64(id.EntryID())}
	if bs, found := t.pendingAcks[key]; found {
		if bs == nil {
			return true
		}
		if !bs.Test(uint(id.BatchIdx())) {
			return true
		}
	}
	return false
}

func (t *timedAckGroupingTracker) flush() {
	if acks := t.clearPendingAcks(); len(acks) > 0 {
		t.flushIndividual(acks)
	}
	if atomic.CompareAndSwapInt32(&t.cumulativeAckRequired, 1, 0) {
		t.RLock()
		id := t.lastCumulativeAck
		t.RUnlock()
		t.ackCumulative(id)
	}
}

func (t *timedAckGroupingTracker) flushAndClean() {
	if acks := t.clearPendingAcks(); len(acks) > 0 {
		t.flushIndividual(acks)
	}
	if atomic.CompareAndSwapInt32(&t.cumulativeAckRequired, 1, 0) {
		t.Lock()
		id := t.lastCumulativeAck
		t.lastCumulativeAck = EarliestMessageID()
		t.Unlock()
		t.ackCumulative(id)
	}
}

func (t *timedAckGroupingTracker) clearPendingAcks() map[[2]uint64]*bitset.BitSet {
	t.Lock()
	defer t.Unlock()
	pendingAcks := t.pendingAcks
	t.pendingAcks = make(map[[2]uint64]*bitset.BitSet)
	return pendingAcks
}

func (t *timedAckGroupingTracker) close() {
	t.flushAndClean()
	if t.exitCh != nil {
		close(t.exitCh)
	}
}

func (t *timedAckGroupingTracker) flushIndividual(pendingAcks map[[2]uint64]*bitset.BitSet) {
	msgIDs := make([]*pb.MessageIdData, 0, len(pendingAcks))
	for k, v := range pendingAcks {
		ledgerID := k[0]
		entryID := k[1]
		msgID := &pb.MessageIdData{LedgerId: &ledgerID, EntryId: &entryID}
		if v != nil && !v.None() {
			bytes := v.Bytes()
			msgID.AckSet = make([]int64, len(bytes))
			for i := 0; i < len(bytes); i++ {
				msgID.AckSet[i] = int64(bytes[i])
			}
		}
		msgIDs = append(msgIDs, msgID)
	}
	t.ackList(msgIDs)
}
