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
	gocrypto "crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar/log"
)

// DefaultMessageCrypto implementation of the interface MessageCryto
type DefaultMessageCrypto struct {
	// data key which is used to encrypt/decrypt messages
	dataKey []byte

	// LoadingCache used by the consumer to cache already decrypted key
	loadingCache sync.Map // map[string][]byte

	encryptedDataKeyMap sync.Map // map[string]EncryptionKeyInfo

	logCtx string

	logger log.Logger

	cipherLock sync.Mutex

	encryptLock sync.Mutex
}

// NewDefaultMessageCrypto get the instance of message crypto
func NewDefaultMessageCrypto(logCtx string, keyGenNeeded bool, logger log.Logger) (*DefaultMessageCrypto, error) {

	d := &DefaultMessageCrypto{
		logCtx:              logCtx,
		loadingCache:        sync.Map{},
		encryptedDataKeyMap: sync.Map{},
		logger:              logger,
	}

	if keyGenNeeded {
		key, err := generateDataKey()
		if err != nil {
			return d, err
		}
		d.dataKey = key
	}

	return d, nil
}

// AddPublicKeyCipher encrypt data key using keyCrypto and cache
func (d *DefaultMessageCrypto) AddPublicKeyCipher(keyNames []string, keyReader KeyReader) error {
	key, err := generateDataKey()
	if err != nil {
		return err
	}

	d.dataKey = key
	for _, keyName := range keyNames {
		err := d.addPublicKeyCipher(keyName, keyReader)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DefaultMessageCrypto) addPublicKeyCipher(keyName string, keyReader KeyReader) error {
	d.cipherLock.Lock()
	defer d.cipherLock.Unlock()
	if keyName == "" || keyReader == nil {
		return fmt.Errorf("keyname or keyreader is null")
	}

	// read the public key and its info using keyReader
	keyInfo, err := keyReader.PublicKey(keyName, nil)
	if err != nil {
		return err
	}

	parsedKey, err := d.loadPublicKey(keyInfo.Key())
	if err != nil {
		return err
	}

	// try to cast to RSA key
	rsaPubKey, ok := parsedKey.(*rsa.PublicKey)
	if !ok {
		return fmt.Errorf("only RSA keys are supported")
	}

	encryptedDataKey, err := rsa.EncryptOAEP(sha1.New(), rand.Reader, rsaPubKey, d.dataKey, nil)
	if err != nil {
		return err
	}

	d.encryptedDataKeyMap.Store(keyName, NewEncryptionKeyInfo(keyName, encryptedDataKey, keyInfo.Metadata()))

	return nil
}

// RemoveKeyCipher remove encrypted data key from cache
func (d *DefaultMessageCrypto) RemoveKeyCipher(keyName string) bool {
	if keyName == "" {
		return false
	}
	d.encryptedDataKeyMap.Delete(keyName)
	return true
}

// Encrypt payload using encryption keys and add encrypted data key
// to message metadata. Here data key is encrypted
// using public key
func (d *DefaultMessageCrypto) Encrypt(encKeys []string,
	keyReader KeyReader,
	msgMetadata MessageMetadataSupplier,
	payload []byte) ([]byte, error) {
	d.encryptLock.Lock()
	defer d.encryptLock.Unlock()

	if len(encKeys) == 0 {
		return payload, nil
	}

	for _, keyName := range encKeys {
		// if key is not already loaded, load it
		if _, ok := d.encryptedDataKeyMap.Load(keyName); !ok {
			if err := d.addPublicKeyCipher(keyName, keyReader); err != nil {
				d.logger.Error(err)
			}
		}

		// add key to the message metadata
		if k, ok := d.encryptedDataKeyMap.Load(keyName); ok {
			keyInfo, keyInfoOk := k.(*EncryptionKeyInfo)

			if keyInfoOk {
				msgMetadata.UpsertEncryptionKey(*keyInfo)
			} else {
				d.logger.Error("failed to get EncryptionKeyInfo for key %v", keyName)
			}
		} else {
			// we should never reach here
			msg := fmt.Sprintf("%v Failed to find encrypted Data key for key %v", d.logCtx, keyName)
			d.logger.Errorf(msg)
			return nil, fmt.Errorf(msg)
		}

	}

	// generate a new AES cipher with data key
	c, err := aes.NewCipher(d.dataKey)

	if err != nil {
		d.logger.Error("failed to create AES cipher")
		return nil, err
	}

	// gcm
	gcm, err := cipher.NewGCM(c)

	if err != nil {
		d.logger.Error("failed to create gcm")
		return nil, err
	}

	// create gcm param
	nonce := make([]byte, gcm.NonceSize())
	_, err = rand.Read(nonce)

	if err != nil {
		d.logger.Error(err)
		return nil, err
	}

	// Update message metadata with encryption param
	msgMetadata.SetEncryptionParam(nonce)

	// encrypt payload using seal function
	return gcm.Seal(nil, nonce, payload, nil), nil
}

// Decrypt the payload using decrypted data key.
// Here data key is read from the message
// metadata and  decrypted using private key.
func (d *DefaultMessageCrypto) Decrypt(msgMetadata MessageMetadataSupplier,
	payload []byte,
	keyReader KeyReader) ([]byte, error) {
	// if data key is present, attempt to derypt using the existing key
	if d.dataKey != nil {
		decryptedData, err := d.getKeyAndDecryptData(msgMetadata, payload)
		if err != nil {
			d.logger.Error(err)
		}

		if decryptedData != nil {
			return decryptedData, nil
		}
	}

	// data key is null or decryption failed. Attempt to regenerate data key
	encKeys := msgMetadata.EncryptionKeys()
	var ecKeyInfo *EncryptionKeyInfo

	for _, encKey := range encKeys {
		if d.decryptDataKey(encKey.Name(), encKey.Key(), encKey.Metadata(), keyReader) {
			ecKeyInfo = &encKey
		}
	}

	if ecKeyInfo == nil || d.dataKey == nil {
		// unable to decrypt data key
		return nil, errors.New("unable to decrypt data key")
	}

	return d.getKeyAndDecryptData(msgMetadata, payload)
}

func (d *DefaultMessageCrypto) decryptData(dataKeySecret []byte,
	msgMetadata MessageMetadataSupplier,
	payload []byte) ([]byte, error) {
	// get nonce from message metadata
	nonce := msgMetadata.EncryptionParam()

	c, err := aes.NewCipher(dataKeySecret)

	if err != nil {
		d.logger.Error(err)
		return nil, err
	}

	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, err
	}

	decryptedData, err := gcm.Open(nil, nonce, payload, nil)

	if err != nil {
		d.logger.Error(err)
	}

	return decryptedData, err
}

func (d *DefaultMessageCrypto) getKeyAndDecryptData(msgMetadata MessageMetadataSupplier,
	payload []byte) ([]byte, error) {
	// go through all keys to retrieve data key from cache
	for _, k := range msgMetadata.EncryptionKeys() {
		msgDataKey := k.Key()
		keyDigest := fmt.Sprintf("%x", md5.Sum(msgDataKey))
		if storedSecretKey, ok := d.loadingCache.Load(keyDigest); ok {
			decryptedData, err := d.decryptData(storedSecretKey.([]byte), msgMetadata, payload)
			if err != nil {
				d.logger.Error(err)
			}

			if decryptedData != nil {
				return decryptedData, nil
			}
		} else {
			// First time, entry won't be present in cache
			d.logger.Debugf("%s Failed to decrypt data or data key is not in cache. Will attempt to refresh", d.logCtx)
		}
	}
	return nil, nil
}

func (d *DefaultMessageCrypto) decryptDataKey(keyName string,
	encDatakey []byte,
	keyMeta map[string]string,
	keyReader KeyReader) bool {

	keyInfo, err := keyReader.PrivateKey(keyName, keyMeta)
	if err != nil {
		d.logger.Error(err)
		return false
	}

	parsedKey, err := d.loadPrivateKey(keyInfo.Key())
	if err != nil {
		d.logger.Error(err)
		return false
	}

	rsaPriKey, ok := parsedKey.(*rsa.PrivateKey)
	if !ok {
		d.logger.Error("only RSA keys are supported")
		return false
	}

	decryptedDataKey, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, rsaPriKey, encDatakey, nil)
	if err != nil {
		d.logger.Error(err)
		return false
	}
	d.dataKey = decryptedDataKey
	d.loadingCache.Store(fmt.Sprintf("%x", md5.Sum(encDatakey)), d.dataKey)

	return true
}

func (d *DefaultMessageCrypto) loadPrivateKey(key []byte) (gocrypto.PrivateKey, error) {
	var privateKey gocrypto.PrivateKey
	priPem, _ := pem.Decode(key)
	if priPem == nil {
		return privateKey, fmt.Errorf("failed to decode private key")
	}
	genericPrivateKey, err := x509.ParsePKCS1PrivateKey(priPem.Bytes)
	if err != nil {
		return privateKey, err
	}
	privateKey = genericPrivateKey
	return privateKey, nil
}

// read the public key into RSA key
func (d *DefaultMessageCrypto) loadPublicKey(key []byte) (gocrypto.PublicKey, error) {
	var publickKey gocrypto.PublicKey

	pubPem, _ := pem.Decode(key)
	if pubPem == nil {
		return publickKey, fmt.Errorf("failed to decode public key")
	}

	genericPublicKey, err := x509.ParsePKIXPublicKey(pubPem.Bytes)
	if err != nil {
		return publickKey, err
	}
	publickKey = genericPublicKey

	return publickKey, nil
}

func generateDataKey() ([]byte, error) {
	key := make([]byte, 32)  // generate key of length 256 bits
	_, err := rand.Read(key) // cryptographically secure random number
	return key, err
}
