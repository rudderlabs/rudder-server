// Copyright 2015 Yahoo Inc.
// Licensed under the terms of the Apache version 2.0 license. See LICENSE file for terms.

package rdl

import (
//	"encoding/json"
//	"fmt"
)

//
// Symbol - a wrapper for string that marshals according to the RDL Symbol spec
//          These symbols are not interned (no global symbtable), this is just for encoding.
//
type Symbol string
