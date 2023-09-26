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
	"context"
	"sync"
	"sync/atomic"
	"unsafe"
)

type chCond struct {
	L sync.Locker
	// The pointer to the channel, the channel pointed to may change,
	// because we will use the channel's close mechanism to implement broadcast notifications.
	notifyChPtr unsafe.Pointer
}

func newCond(l sync.Locker) *chCond {
	c := &chCond{L: l}
	n := make(chan struct{})
	c.notifyChPtr = unsafe.Pointer(&n)
	return c
}

// wait for broadcast calls. Similar to regular sync.Cond
func (c *chCond) wait() {
	n := c.notifyChan()
	c.L.Unlock()
	<-n
	c.L.Lock()
}

// waitWithContext Same as wait() call, but the end condition can also be controlled through the context.
func (c *chCond) waitWithContext(ctx context.Context) bool {
	n := c.notifyChan()
	c.L.Unlock()
	defer c.L.Lock()
	select {
	case <-n:
		return true
	case <-ctx.Done():
		return false
	default:
		return true
	}
}

// broadcast wakes all goroutines waiting on c.
// It is not required for the caller to hold c.L during the call.
func (c *chCond) broadcast() {
	n := make(chan struct{})
	ptrOld := atomic.SwapPointer(&c.notifyChPtr, unsafe.Pointer(&n))
	// close old channels to trigger broadcast.
	close(*(*chan struct{})(ptrOld))
}

func (c *chCond) notifyChan() <-chan struct{} {
	ptr := atomic.LoadPointer(&c.notifyChPtr)
	return *((*chan struct{})(ptr))
}
