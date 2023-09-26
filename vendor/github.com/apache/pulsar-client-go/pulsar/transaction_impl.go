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
	"context"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

type subscription struct {
	topic        string
	subscription string
}

type transaction struct {
	sync.Mutex
	txnID                    TxnID
	state                    TxnState
	tcClient                 *transactionCoordinatorClient
	registerPartitions       map[string]bool
	registerAckSubscriptions map[subscription]bool
	// opsFlow It has two effects:
	// 1. Wait all the operations of sending and acking messages with the transaction complete
	// by reading msg from the chan.
	// 2. Prevent sending or acking messages with a committed or aborted transaction.
	// opsCount is record the number of the uncompleted operations.
	// opsFlow
	//   Write:
	//     1. When the transaction is created, a bool will be written to opsFlow chan.
	//     2. When the opsCount decrement from 1 to 0, a new bool will be written to opsFlow chan.
	//     3. When get a retryable error after committing or aborting the transaction,
	//        a bool will be written to opsFlow chan.
	//   Read:
	//     1. When the transaction is committed or aborted, a bool will be read from opsFlow chan.
	//     2. When the opsCount increment from 0 to 1, a bool will be read from opsFlow chan.
	opsFlow   chan bool
	opsCount  int32
	opTimeout time.Duration
	log       log.Logger
}

func newTransaction(id TxnID, tcClient *transactionCoordinatorClient, timeout time.Duration) *transaction {
	transaction := &transaction{
		txnID:                    id,
		state:                    TxnOpen,
		registerPartitions:       make(map[string]bool),
		registerAckSubscriptions: make(map[subscription]bool),
		opsFlow:                  make(chan bool, 1),
		opTimeout:                5 * time.Second,
		tcClient:                 tcClient,
	}
	//This means there are not pending requests with this transaction. The transaction can be committed or aborted.
	transaction.opsFlow <- true
	go func() {
		//Set the state of the transaction to timeout after timeout
		<-time.After(timeout)
		atomic.CompareAndSwapInt32((*int32)(&transaction.state), int32(TxnOpen), int32(TxnTimeout))
	}()
	transaction.log = tcClient.log.SubLogger(log.Fields{})
	return transaction
}

func (txn *transaction) GetState() TxnState {
	return txn.state
}

func (txn *transaction) Commit(ctx context.Context) error {
	if !(atomic.CompareAndSwapInt32((*int32)(&txn.state), int32(TxnOpen), int32(TxnCommitting)) ||
		txn.state == TxnCommitting) {
		return newError(InvalidStatus, "Expect transaction state is TxnOpen but "+txn.state.string())
	}

	//Wait for all operations to complete
	select {
	case <-txn.opsFlow:
	case <-time.After(txn.opTimeout):
		return newError(TimeoutError, "There are some operations that are not completed after the timeout.")
	}
	//Send commit transaction command to transaction coordinator
	err := txn.tcClient.endTxn(&txn.txnID, pb.TxnAction_COMMIT)
	if err == nil {
		atomic.StoreInt32((*int32)(&txn.state), int32(TxnCommitted))
	} else {
		if err.(*Error).Result() == TransactionNoFoundError || err.(*Error).Result() == InvalidStatus {
			atomic.StoreInt32((*int32)(&txn.state), int32(TxnError))
			return err
		}
		txn.opsFlow <- true
	}
	return err
}

func (txn *transaction) Abort(ctx context.Context) error {
	if !(atomic.CompareAndSwapInt32((*int32)(&txn.state), int32(TxnOpen), int32(TxnAborting)) ||
		txn.state == TxnAborting) {
		return newError(InvalidStatus, "Expect transaction state is TxnOpen but "+txn.state.string())
	}

	//Wait for all operations to complete
	select {
	case <-txn.opsFlow:
	case <-time.After(txn.opTimeout):
		return newError(TimeoutError, "There are some operations that are not completed after the timeout.")
	}
	//Send abort transaction command to transaction coordinator
	err := txn.tcClient.endTxn(&txn.txnID, pb.TxnAction_ABORT)
	if err == nil {
		atomic.StoreInt32((*int32)(&txn.state), int32(TxnAborted))
	} else {
		if err.(*Error).Result() == TransactionNoFoundError || err.(*Error).Result() == InvalidStatus {
			atomic.StoreInt32((*int32)(&txn.state), int32(TxnError))
		} else {
			txn.opsFlow <- true
		}
	}
	return err
}

func (txn *transaction) registerSendOrAckOp() error {
	if atomic.AddInt32(&txn.opsCount, 1) == 1 {
		//There are new operations that not completed
		select {
		case <-txn.opsFlow:
			return nil
		case <-time.After(txn.opTimeout):
			if _, err := txn.checkIfOpen(); err != nil {
				return err
			}
			return newError(TimeoutError, "Failed to get the semaphore to register the send/ack operation")
		}
	}
	return nil
}

func (txn *transaction) endSendOrAckOp(err error) {
	if err != nil {
		atomic.StoreInt32((*int32)(&txn.state), int32(TxnError))
	}
	if atomic.AddInt32(&txn.opsCount, -1) == 0 {
		//This means there are not pending send/ack requests
		txn.opsFlow <- true
	}
}

func (txn *transaction) registerProducerTopic(topic string) error {
	isOpen, err := txn.checkIfOpen()
	if !isOpen {
		return err
	}
	_, ok := txn.registerPartitions[topic]
	if !ok {
		txn.Lock()
		defer txn.Unlock()
		if _, ok = txn.registerPartitions[topic]; !ok {
			err := txn.tcClient.addPublishPartitionToTxn(&txn.txnID, []string{topic})
			if err != nil {
				return err
			}
			txn.registerPartitions[topic] = true
		}
	}
	return nil
}

func (txn *transaction) registerAckTopic(topic string, subName string) error {
	isOpen, err := txn.checkIfOpen()
	if !isOpen {
		return err
	}
	sub := subscription{
		topic:        topic,
		subscription: subName,
	}
	_, ok := txn.registerAckSubscriptions[sub]
	if !ok {
		txn.Lock()
		defer txn.Unlock()
		if _, ok = txn.registerAckSubscriptions[sub]; !ok {
			err := txn.tcClient.addSubscriptionToTxn(&txn.txnID, topic, subName)
			if err != nil {
				return err
			}
			txn.registerAckSubscriptions[sub] = true
		}
	}
	return nil
}

func (txn *transaction) GetTxnID() TxnID {
	return txn.txnID
}

func (txn *transaction) checkIfOpen() (bool, error) {
	if txn.state == TxnOpen {
		return true, nil
	}
	return false, newError(InvalidStatus, "Expect transaction state is TxnOpen but "+txn.state.string())
}

func (state TxnState) string() string {
	switch state {
	case TxnOpen:
		return "TxnOpen"
	case TxnCommitting:
		return "TxnCommitting"
	case TxnAborting:
		return "TxnAborting"
	case TxnCommitted:
		return "TxnCommitted"
	case TxnAborted:
		return "TxnAborted"
	case TxnTimeout:
		return "TxnTimeout"
	case TxnError:
		return "TxnError"
	default:
		return "Unknown"
	}
}
