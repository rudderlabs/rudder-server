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
	"time"

	log "github.com/apache/pulsar-client-go/pulsar/log"
)

type redeliveryConsumer interface {
	Redeliver(msgIds []messageID)
}

type negativeAcksTracker struct {
	sync.Mutex

	doneCh       chan interface{}
	doneOnce     sync.Once
	negativeAcks map[messageID]time.Time
	rc           redeliveryConsumer
	nackBackoff  NackBackoffPolicy
	tick         *time.Ticker
	delay        time.Duration
	log          log.Logger
}

func newNegativeAcksTracker(rc redeliveryConsumer, delay time.Duration,
	nackBackoffPolicy NackBackoffPolicy, logger log.Logger) *negativeAcksTracker {

	t := &negativeAcksTracker{
		doneCh:       make(chan interface{}),
		negativeAcks: make(map[messageID]time.Time),
		rc:           rc,
		nackBackoff:  nackBackoffPolicy,
		log:          logger,
	}

	if nackBackoffPolicy != nil {
		firstDelayForNackBackoff := nackBackoffPolicy.Next(1)
		t.delay = firstDelayForNackBackoff
	} else {
		t.delay = delay
	}

	t.tick = time.NewTicker(t.delay / 3)

	go t.track()
	return t
}

func (t *negativeAcksTracker) Add(msgID *messageID) {
	// Always clear up the batch index since we want to track the nack
	// for the entire batch
	batchMsgID := messageID{
		ledgerID: msgID.ledgerID,
		entryID:  msgID.entryID,
		batchIdx: 0,
	}

	t.Lock()
	defer t.Unlock()

	_, present := t.negativeAcks[batchMsgID]
	if present {
		// The batch is already being tracked
		return
	}

	targetTime := time.Now().Add(t.delay)
	t.negativeAcks[batchMsgID] = targetTime
}

func (t *negativeAcksTracker) AddMessage(msg Message) {
	nackBackoffDelay := t.nackBackoff.Next(msg.RedeliveryCount())

	msgID := msg.ID()

	// Always clear up the batch index since we want to track the nack
	// for the entire batch
	batchMsgID := messageID{
		ledgerID: msgID.LedgerID(),
		entryID:  msgID.EntryID(),
		batchIdx: 0,
	}

	t.Lock()
	defer t.Unlock()

	_, present := t.negativeAcks[batchMsgID]
	if present {
		// The batch is already being tracked
		return
	}

	targetTime := time.Now().Add(nackBackoffDelay)
	t.negativeAcks[batchMsgID] = targetTime
}

func (t *negativeAcksTracker) track() {
	for {
		select {
		case <-t.doneCh:
			t.log.Debug("Closing nack tracker")
			return

		case <-t.tick.C:
			{
				now := time.Now()
				msgIds := make([]messageID, 0)

				t.Lock()

				for msgID, targetTime := range t.negativeAcks {
					t.log.Debugf("MsgId: %v -- targetTime: %v -- now: %v", msgID, targetTime, now)
					if targetTime.Before(now) {
						t.log.Debugf("Adding MsgId: %v", msgID)
						msgIds = append(msgIds, msgID)
						delete(t.negativeAcks, msgID)
					}
				}

				t.Unlock()

				if len(msgIds) > 0 {
					t.rc.Redeliver(msgIds)
				}
			}
		}
	}
}

func (t *negativeAcksTracker) Close() {
	// allow Close() to be invoked multiple times by consumer_partition to avoid panic
	t.doneOnce.Do(func() {
		t.tick.Stop()
		t.doneCh <- nil
	})
}
