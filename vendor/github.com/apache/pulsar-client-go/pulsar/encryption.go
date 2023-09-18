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

import "github.com/apache/pulsar-client-go/pulsar/crypto"

// ProducerEncryptionInfo encryption related fields required by the producer
type ProducerEncryptionInfo struct {
	// KeyReader read RSA public/private key pairs
	KeyReader crypto.KeyReader

	// MessageCrypto used to encrypt and decrypt the data and session keys
	MessageCrypto crypto.MessageCrypto

	// Keys list of encryption key names to encrypt session key
	Keys []string

	// ProducerCryptoFailureAction action to be taken on failure of message encryption
	// default is ProducerCryptoFailureActionFail
	ProducerCryptoFailureAction int
}

// MessageDecryptionInfo encryption related fields required by the consumer to decrypt the message
type MessageDecryptionInfo struct {
	// KeyReader read RSA public/private key pairs
	KeyReader crypto.KeyReader

	// MessageCrypto used to encrypt and decrypt the data and session keys
	MessageCrypto crypto.MessageCrypto

	// ConsumerCryptoFailureAction action to be taken on failure of message decryption
	ConsumerCryptoFailureAction int
}
