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

package crypto

import (
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar/crypto"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

type producerEncryptor struct {
	keys                        []string
	keyReader                   crypto.KeyReader
	messageCrypto               crypto.MessageCrypto
	logger                      log.Logger
	producerCryptoFailureAction int
}

func NewProducerEncryptor(keys []string,
	keyReader crypto.KeyReader,
	messageCrypto crypto.MessageCrypto,
	producerCryptoFailureAction int,
	logger log.Logger) Encryptor {
	return &producerEncryptor{
		keys:                        keys,
		keyReader:                   keyReader,
		messageCrypto:               messageCrypto,
		logger:                      logger,
		producerCryptoFailureAction: producerCryptoFailureAction,
	}
}

// Encrypt producer encryptor
func (e *producerEncryptor) Encrypt(payload []byte, msgMetadata *pb.MessageMetadata) ([]byte, error) {
	// encrypt payload
	encryptedPayload, err := e.messageCrypto.Encrypt(e.keys,
		e.keyReader,
		crypto.NewMessageMetadataSupplier(msgMetadata),
		payload)

	// error encryping the payload
	if err != nil {
		// error occurred in encrypting the payload
		// crypto ProducerCryptoFailureAction is set to send
		// send unencrypted message
		if e.producerCryptoFailureAction == crypto.ProducerCryptoFailureActionSend {
			e.logger.
				WithError(err).
				Warnf("Encryption failed for payload sending unencrypted message ProducerCryptoFailureAction is set to send")
			return payload, nil
		}

		return nil, fmt.Errorf("ProducerCryptoFailureAction is set to Fail and error occurred in encrypting payload :%v", err)
	}
	return encryptedPayload, nil
}
