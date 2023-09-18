// Copyright 2015 Yahoo Inc.
// Licensed under the terms of the Apache version 2.0 license. See LICENSE file for terms.

package rdl

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

//
// UUID - a wrapper around a 16 byte slice representing a UUID.
//
type UUID []byte

//
// Equal - return true if another UUID is equal to this one
//
func (u UUID) Equal(another UUID) bool {
	for i, b := range u {
		if b != another[i] {
			return false
		}
	}
	return true
}

//
// ParseUUID - parse the string to produce a UUID
//
func ParseUUID(s string) UUID {
	if len(s) == 36+9 {
		if strings.ToLower(s[:9]) != "urn:uuid:" {
			return nil
		}
		s = s[9:]
	} else if len(s) != 36 {
		return nil
	}
	if s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return nil
	}
	uuid := make([]byte, 16)
	for i, x := range []int{
		0, 2, 4, 6,
		9, 11,
		14, 16,
		19, 21,
		24, 26, 28, 30, 32, 34} {
		var v int64
		var err error
		if v, err = strconv.ParseInt(s[x:x+2], 16, 16); err != nil {
			return nil
		}
		uuid[i] = byte(v)
	}
	return uuid
}

//
// NewUUID - wrap an existing byte array as a UUID. Returns nil if not 16 bytes in length
//
func NewUUID(b []byte) UUID {
	if len(b) != 16 {
		return nil
	}
	return b
}

//
// String produces the standard "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" representaion of a UUID.
//
func (u UUID) String() string {
	if u == nil || len(u) != 16 {
		return "00000000-0000-0000-0000-000000000000"
	}
	b := []byte(u)
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}

//
// MarshalJSON produces the standard format for a UUID as a JSON string.
//
func (u UUID) MarshalJSON() ([]byte, error) {
	return []byte("\"" + u.String() + "\""), nil
}

//
// UnmarshalJSON parses a JSON string in standard UUID format.
//
func (u *UUID) UnmarshalJSON(b []byte) error {
	var j string
	err := json.Unmarshal(b, &j)
	if err == nil {
		v := ParseUUID(string(j))
		if v != nil {
			*u = v
		} else {
			err = fmt.Errorf("Bad UUID: %v", string(j))
		}
	}
	return err
}
