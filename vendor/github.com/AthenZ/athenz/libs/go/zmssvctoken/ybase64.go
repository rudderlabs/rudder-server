// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache version 2.0 license. See LICENSE file for terms.

package zmssvctoken

import (
	"encoding/base64"
)

const (
	encodeChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._"
)

func getEncoding() *base64.Encoding {
	return base64.NewEncoding(encodeChars).WithPadding('-')
}

// YBase64 is a variant of the std base64 encoding with URL safe
// characters, used by Yahoo circa web 1.0. It uses '.' and '_' as replacements
// for '+' and '/' and uses '-' instead of '=' as the padding character.
type YBase64 struct {
}

// EncodeToString encodes an array of bytes to a string.
func (lb *YBase64) EncodeToString(b []byte) string {
	return getEncoding().EncodeToString(b)
}

// DecodeString decodes a string encoded using EncodeToString.
func (lb *YBase64) DecodeString(s string) ([]byte, error) {
	return getEncoding().DecodeString(s)
}
