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

package internal

import (
	"sync"
)

// BlockingQueue is a interface of block queue
type BlockingQueue interface {
	// Put enqueue one item, block if the queue is full
	Put(item interface{})

	// Take dequeue one item, block until it's available
	Take() interface{}

	// Poll dequeue one item, return nil if queue is empty
	Poll() interface{}

	// CompareAndPoll compare the first item and poll it if meet the conditions
	CompareAndPoll(compare func(item interface{}) bool) interface{}

	// Peek return the first item without dequeing, return nil if queue is empty
	Peek() interface{}

	// PeekLast return last item in queue without dequeing, return nil if queue is empty
	PeekLast() interface{}

	// Size return the current size of the queue
	Size() int

	// ReadableSlice returns a new view of the readable items in the queue
	ReadableSlice() []interface{}
}

type blockingQueue struct {
	items   []interface{}
	headIdx int
	tailIdx int
	size    int
	maxSize int

	mutex      sync.Mutex
	isNotEmpty *sync.Cond
	isNotFull  *sync.Cond
}

// NewBlockingQueue init block queue and returns a BlockingQueue
func NewBlockingQueue(maxSize int) BlockingQueue {
	bq := &blockingQueue{
		items:   make([]interface{}, maxSize),
		headIdx: 0,
		tailIdx: 0,
		size:    0,
		maxSize: maxSize,
	}

	bq.isNotEmpty = sync.NewCond(&bq.mutex)
	bq.isNotFull = sync.NewCond(&bq.mutex)
	return bq
}

func (bq *blockingQueue) Put(item interface{}) {
	bq.mutex.Lock()
	defer bq.mutex.Unlock()

	for bq.size == bq.maxSize {
		bq.isNotFull.Wait()
	}

	wasEmpty := bq.size == 0

	bq.items[bq.tailIdx] = item
	bq.size++
	bq.tailIdx++
	if bq.tailIdx >= bq.maxSize {
		bq.tailIdx = 0
	}

	if wasEmpty {
		// Wake up eventual reader waiting for next item
		bq.isNotEmpty.Signal()
	}
}

func (bq *blockingQueue) Take() interface{} {
	bq.mutex.Lock()
	defer bq.mutex.Unlock()

	for bq.size == 0 {
		bq.isNotEmpty.Wait()
	}

	return bq.dequeue()
}

func (bq *blockingQueue) Poll() interface{} {
	bq.mutex.Lock()
	defer bq.mutex.Unlock()

	if bq.size == 0 {
		return nil
	}

	return bq.dequeue()
}

func (bq *blockingQueue) CompareAndPoll(compare func(interface{}) bool) interface{} {
	bq.mutex.Lock()
	defer bq.mutex.Unlock()

	if bq.size == 0 {
		return nil
	}

	if compare(bq.items[bq.headIdx]) {
		return bq.dequeue()
	}
	return nil
}

func (bq *blockingQueue) Peek() interface{} {
	bq.mutex.Lock()
	defer bq.mutex.Unlock()

	if bq.size == 0 {
		return nil
	}
	return bq.items[bq.headIdx]
}

func (bq *blockingQueue) PeekLast() interface{} {
	bq.mutex.Lock()
	defer bq.mutex.Unlock()

	if bq.size == 0 {
		return nil
	}
	idx := (bq.headIdx + bq.size - 1) % bq.maxSize
	return bq.items[idx]
}

func (bq *blockingQueue) dequeue() interface{} {
	item := bq.items[bq.headIdx]
	bq.items[bq.headIdx] = nil

	bq.headIdx++
	if bq.headIdx == len(bq.items) {
		bq.headIdx = 0
	}

	bq.size--
	bq.isNotFull.Signal()
	return item
}

func (bq *blockingQueue) Size() int {
	bq.mutex.Lock()
	defer bq.mutex.Unlock()

	return bq.size
}

func (bq *blockingQueue) ReadableSlice() []interface{} {
	bq.mutex.Lock()
	defer bq.mutex.Unlock()

	res := make([]interface{}, bq.size)
	readIdx := bq.headIdx
	for i := 0; i < bq.size; i++ {
		res[i] = bq.items[readIdx]
		readIdx++
		if readIdx == bq.maxSize {
			readIdx = 0
		}
	}

	return res
}
