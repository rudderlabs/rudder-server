// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache version 2.0 license. See LICENSE file for terms.

package zmssvctoken

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

type keySource struct {
	domain     string
	name       string
	keyVersion string
}

type validatorMeta struct {
	pubKey    []byte
	validator TokenValidator
	expiry    time.Time
}

func (k keySource) String() string {
	return fmt.Sprintf("[Domain: '%s', Name: '%s', Key version: '%s']", k.domain, k.name, k.keyVersion)
}

type keyStore struct {
	sync.RWMutex
	cache  map[keySource]*validatorMeta
	config *ValidationConfig
}

func newKeyStore(cfg *ValidationConfig) *keyStore {
	return &keyStore{
		config: cfg,
		cache:  make(map[keySource]*validatorMeta),
	}
}

func (k *keyStore) loadKey(src keySource) ([]byte, error) {

	client := &http.Client{
		Timeout: k.config.PublicKeyFetchTimeout,
	}

	url := fmt.Sprintf("%s/domain/%s/service/%s/publickey/%s", k.config.ZTSBaseUrl, src.domain, src.name, src.keyVersion)
	res, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("ZTS returned status %d", res.StatusCode)
	}

	b, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, err
	}

	var data struct {
		Key string
	}

	err = json.Unmarshal(b, &data)
	if err != nil {
		return nil, err
	}

	s, err := new(YBase64).DecodeString(data.Key)
	if err != nil {
		return nil, err
	}

	return s, nil

}

func (k *keyStore) getValidator(src keySource) (TokenValidator, error) {

	var (
		oldKey       []byte         // caches the previous seen key to avoid reloading
		oldValidator TokenValidator // caches the previous seen validator, ditto
	)

	k.RLock()
	meta, ok := k.cache[src]
	if ok {
		oldKey = meta.pubKey
		oldValidator = meta.validator
		if meta.expiry.Before(time.Now()) { // dead
			meta = nil
		}
	}
	k.RUnlock()

	// return from cache if valid entry
	if meta != nil {
		return meta.validator, nil
	}

	// get remote key, if not
	key, err := k.loadKey(src)
	if err != nil {
		return nil, fmt.Errorf("Unable to get public key from ZTS for %v, err: %v", src, err)
	}

	var v TokenValidator
	if oldKey != nil && bytes.Equal(key, oldKey) { // no changes to key, use old validator
		v = oldValidator
	} else {
		v, err = NewPubKeyTokenValidator(key)
		if err != nil {
			return nil, err
		}
	}

	meta = &validatorMeta{
		pubKey:    key,
		validator: v,
		expiry:    time.Now().Add(k.config.CacheTTL),
	}

	// add to cache
	k.Lock()
	k.cache[src] = meta
	k.Unlock()

	return v, nil
}
