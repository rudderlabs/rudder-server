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
)

// TxnState The state of the transaction. Check the state of the transaction before executing some operation
// with the transaction is necessary.
type TxnState int32

const (
	_ TxnState = iota
	// TxnOpen The transaction in TxnOpen state can be used to send/ack messages.
	TxnOpen
	// TxnCommitting The state of the transaction will be TxnCommitting after the commit method is called.
	// The transaction in TxnCommitting state can be committed again.
	TxnCommitting
	// TxnAborting The state of the transaction will be TxnAborting after the abort method is called.
	// The transaction in TxnAborting state can be aborted again.
	TxnAborting
	// TxnCommitted The state of the transaction will be TxnCommitted after the commit method is executed success.
	// This means that all the operations with the transaction are success.
	TxnCommitted
	// TxnAborted The state of the transaction will be TxnAborted after the abort method is executed success.
	// This means that all the operations with the transaction are aborted.
	TxnAborted
	// TxnError The state of the transaction will be TxnError after the operation of transaction get a non-retryable error.
	TxnError
	// TxnTimeout The state of the transaction will be TxnTimeout after the transaction timeout.
	TxnTimeout
)

// TxnID An identifier for representing a transaction.
type TxnID struct {
	// MostSigBits The most significant 64 bits of this TxnID.
	MostSigBits uint64
	// LeastSigBits The least significant 64 bits of this TxnID.
	LeastSigBits uint64
}

// Transaction used to guarantee exactly-once
type Transaction interface {
	//Commit You can commit the transaction after all the sending/acknowledging operations with the transaction success.
	Commit(context.Context) error
	//Abort You can abort the transaction when you want to abort all the sending/acknowledging operations
	// with the transaction.
	Abort(context.Context) error
	//GetState Get the state of the transaction.
	GetState() TxnState
	//GetTxnID Get the identified ID of the transaction.
	GetTxnID() TxnID
}
