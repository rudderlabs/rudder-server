package rand

import (
	"math/rand"
	"sync"
	"time"
	"unsafe"
)

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var (
	src                   = rand.NewSource(time.Now().UnixNano())
	uniqueRandomStrings   = make(map[string]struct{})
	uniqueRandomStringsMu sync.Mutex
)

// String returns a random string
// Credits to https://stackoverflow.com/a/31832326/828366
func String(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return *(*string)(unsafe.Pointer(&b)) // skipcq: GSC-G103
}

// UniqueString returns a random string that is unique
func UniqueString(n int) string {
	str := String(n)

	uniqueRandomStringsMu.Lock()
	defer uniqueRandomStringsMu.Unlock()

	for {
		if _, ok := uniqueRandomStrings[str]; !ok {
			uniqueRandomStrings[str] = struct{}{}
			return str
		}
		str = String(n)
	}
}
