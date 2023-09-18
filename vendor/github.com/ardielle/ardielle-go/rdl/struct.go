// Copyright 2015 Yahoo Inc.
// Licensed under the terms of the Apache version 2.0 license. See LICENSE file for terms.

package rdl

import (
	"bytes"
	"fmt"
)

//
// Struct - A typedef for a "naked" struct with no field definitions.
//          Structs can be tested for equality, along with all RDL types.
//
type Struct map[Symbol]interface{}

func (s Struct) Equal(another Struct) bool {
	if len(s) != len(another) {
		return false
	}
	for k, v1 := range s {
		v2, ok := another[k]
		if !ok {
			return false
		}
		if !Equal(v1, v2) {
			return false
		}
	}
	return true
}

func (s Struct) String() string {
	var buf bytes.Buffer
	buf.WriteString("{")
	first := true
	for k, v := range s {
		if first {
			first = false
		} else {
			buf.WriteString(",")
		}
		buf.WriteString(string(k))
		buf.WriteString(":")
		buf.WriteString(fmt.Sprint(v))
	}
	buf.WriteString("}")
	return buf.String()
}
