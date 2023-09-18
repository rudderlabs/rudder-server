// Copyright 2015 Yahoo Inc.
// Licensed under the terms of the Apache version 2.0 license. See LICENSE file for terms.

package rdl

import (
	"bytes"
	"fmt"
)

type Any interface{}

//
//Equal - Return true if the two values are equivalent, in the RDL sense.
//
func Equal(val1 interface{}, val2 interface{}) bool {
	if val1 == nil || val2 == nil {
		return false
	}
	switch v1 := val1.(type) {
	case UUID:
		if v2, ok := val2.(UUID); ok {
			return v1.Equal(v2)
		}
		return false
	case Timestamp:
		if v2, ok := val2.(Timestamp); ok {
			return v1.Equal(v2)
		}
		return false
	case Array:
		if v2, ok := val2.(Array); ok {
			return v1.Equal(v2)
		}
		return false
	case Struct:
		if v2, ok := val2.(Struct); ok {
			return v1.Equal(v2)
		}
		return false
	case Symbol:
		if v2, ok := val2.(Symbol); ok {
			return v1 == v2
		}
		return false
	case int, int32, int8, int16, int64, float32, float64, uint, uint8, uint16, uint32, uint64:
		return val1 == val2
	case bool:
		return val1 == val2
	case string:
		if v2, ok := val2.(string); ok {
			return v1 == v2
		}
		return false
	case []byte:
		if v2, ok := val2.([]byte); ok {
			return bytes.Compare(v1, v2) == 0
		}
		return false
	default:
		fmt.Println(val1)
		fmt.Println(val2)
		fmt.Println("fix Equal of these two types")
		panic("fix Equal of these two types")
	}
}
