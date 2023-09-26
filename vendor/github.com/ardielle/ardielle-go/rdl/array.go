// Copyright 2015 Yahoo Inc.
// Licensed under the terms of the Apache version 2.0 license. See LICENSE file for terms.

package rdl

//
// Array - a typedef for a "naked" array with no field definitions
//
type Array []interface{}

//
// Equal - in RDL, arrays and structs can be compared. This Equal method does that.
//
func (a Array) Equal(another Array) bool {
	if another == nil {
		return false
	}
	if len(a) != len(another) {
		return false
	}
	for i, v1 := range a {
		v2 := another[i]
		if !Equal(v1, v2) {
			return false
		}
	}
	return true
}
