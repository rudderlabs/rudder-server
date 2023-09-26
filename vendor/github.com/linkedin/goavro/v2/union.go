// Copyright [2019] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

// codecInfo is a set of quick lookups it holds all the lookup info for the
// all the schemas we need to handle the list of types for this union
type codecInfo struct {
	allowedTypes   []string
	codecFromIndex []*Codec
	codecFromName  map[string]*Codec
	indexFromName  map[string]int
}

// Union wraps a datum value in a map for encoding as a Union, as required by
// Union encoder.
//
// When providing a value for an Avro union, the encoder will accept `nil` for a
// `null` value. If the value is non-`nil`, it must be a
// `map[string]interface{}` with a single key-value pair, where the key is the
// Avro type name and the value is the datum's value. As a convenience, the
// `Union` function wraps any datum value in a map as specified above.
//
//	func ExampleUnion() {
//	   codec, err := goavro.NewCodec(`["null","string","int"]`)
//	   if err != nil {
//	       fmt.Println(err)
//	   }
//	   buf, err := codec.TextualFromNative(nil, goavro.Union("string", "some string"))
//	   if err != nil {
//	       fmt.Println(err)
//	   }
//	   fmt.Println(string(buf))
//	   // Output: {"string":"some string"}
//	}
func Union(name string, datum interface{}) interface{} {
	if datum == nil && name == "null" {
		return nil
	}
	return map[string]interface{}{name: datum}
}

// makeCodecInfo takes the schema array
// and builds some lookup indices
// returning a codecInfo
func makeCodecInfo(st map[string]*Codec, enclosingNamespace string, schemaArray []interface{}, cb *codecBuilder) (codecInfo, error) {
	allowedTypes := make([]string, len(schemaArray)) // used for error reporting when encoder receives invalid datum type
	codecFromIndex := make([]*Codec, len(schemaArray))
	codecFromName := make(map[string]*Codec, len(schemaArray))
	indexFromName := make(map[string]int, len(schemaArray))

	for i, unionMemberSchema := range schemaArray {
		unionMemberCodec, err := buildCodec(st, enclosingNamespace, unionMemberSchema, cb)
		if err != nil {
			return codecInfo{}, fmt.Errorf("Union item %d ought to be valid Avro type: %s", i+1, err)
		}
		fullName := unionMemberCodec.typeName.fullName
		if _, ok := indexFromName[fullName]; ok {
			return codecInfo{}, fmt.Errorf("Union item %d ought to be unique type: %s", i+1, unionMemberCodec.typeName)
		}
		allowedTypes[i] = fullName
		codecFromIndex[i] = unionMemberCodec
		codecFromName[fullName] = unionMemberCodec
		indexFromName[fullName] = i
	}

	return codecInfo{
		allowedTypes:   allowedTypes,
		codecFromIndex: codecFromIndex,
		codecFromName:  codecFromName,
		indexFromName:  indexFromName,
	}, nil

}

func unionNativeFromBinary(cr *codecInfo) func(buf []byte) (interface{}, []byte, error) {

	return func(buf []byte) (interface{}, []byte, error) {
		var decoded interface{}
		var err error

		decoded, buf, err = longNativeFromBinary(buf)
		if err != nil {
			return nil, nil, err
		}
		index := decoded.(int64) // longDecoder always returns int64, so elide error checking
		if index < 0 || index >= int64(len(cr.codecFromIndex)) {
			return nil, nil, fmt.Errorf("cannot decode binary union: index ought to be between 0 and %d; read index: %d", len(cr.codecFromIndex)-1, index)
		}
		c := cr.codecFromIndex[index]
		decoded, buf, err = c.nativeFromBinary(buf)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot decode binary union item %d: %s", index+1, err)
		}
		if decoded == nil {
			// do not wrap a nil value in a map
			return nil, buf, nil
		}
		// Non-nil values are wrapped in a map with single key set to type name of value
		return Union(cr.allowedTypes[index], decoded), buf, nil
	}
}
func unionBinaryFromNative(cr *codecInfo) func(buf []byte, datum interface{}) ([]byte, error) {
	return func(buf []byte, datum interface{}) ([]byte, error) {
		switch v := datum.(type) {
		case nil:
			index, ok := cr.indexFromName["null"]
			if !ok {
				return nil, fmt.Errorf("cannot encode binary union: no member schema types support datum: allowed types: %v; received: %T", cr.allowedTypes, datum)
			}
			return longBinaryFromNative(buf, index)
		case map[string]interface{}:
			if len(v) != 1 {
				return nil, fmt.Errorf("cannot encode binary union: non-nil Union values ought to be specified with Go map[string]interface{}, with single key equal to type name, and value equal to datum value: %v; received: %T", cr.allowedTypes, datum)
			}
			// will execute exactly once
			for key, value := range v {
				index, ok := cr.indexFromName[key]
				if !ok {
					return nil, fmt.Errorf("cannot encode binary union: no member schema types support datum: allowed types: %v; received: %T", cr.allowedTypes, datum)
				}
				c := cr.codecFromIndex[index]
				buf, _ = longBinaryFromNative(buf, index)
				return c.binaryFromNative(buf, value)
			}
		}
		return nil, fmt.Errorf("cannot encode binary union: non-nil Union values ought to be specified with Go map[string]interface{}, with single key equal to type name, and value equal to datum value: %v; received: %T", cr.allowedTypes, datum)
	}
}
func unionNativeFromTextual(cr *codecInfo) func(buf []byte) (interface{}, []byte, error) {
	return func(buf []byte) (interface{}, []byte, error) {
		if len(buf) >= 4 && bytes.Equal(buf[:4], []byte("null")) {
			if _, ok := cr.indexFromName["null"]; ok {
				return nil, buf[4:], nil
			}
		}

		var datum interface{}
		var err error
		datum, buf, err = genericMapTextDecoder(buf, nil, cr.codecFromName)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot decode textual union: %s", err)
		}

		return datum, buf, nil
	}
}
func unionTextualFromNative(cr *codecInfo) func(buf []byte, datum interface{}) ([]byte, error) {
	return func(buf []byte, datum interface{}) ([]byte, error) {
		switch v := datum.(type) {
		case nil:
			_, ok := cr.indexFromName["null"]
			if !ok {
				return nil, fmt.Errorf("cannot encode textual union: no member schema types support datum: allowed types: %v; received: %T", cr.allowedTypes, datum)
			}
			return append(buf, "null"...), nil
		case map[string]interface{}:
			if len(v) != 1 {
				return nil, fmt.Errorf("cannot encode textual union: non-nil Union values ought to be specified with Go map[string]interface{}, with single key equal to type name, and value equal to datum value: %v; received: %T", cr.allowedTypes, datum)
			}
			// will execute exactly once
			for key, value := range v {
				index, ok := cr.indexFromName[key]
				if !ok {
					return nil, fmt.Errorf("cannot encode textual union: no member schema types support datum: allowed types: %v; received: %T", cr.allowedTypes, datum)
				}
				buf = append(buf, '{')
				var err error
				buf, err = stringTextualFromNative(buf, key)
				if err != nil {
					return nil, fmt.Errorf("cannot encode textual union: %s", err)
				}
				buf = append(buf, ':')
				c := cr.codecFromIndex[index]
				buf, err = c.textualFromNative(buf, value)
				if err != nil {
					return nil, fmt.Errorf("cannot encode textual union: %s", err)
				}
				return append(buf, '}'), nil
			}
		}
		return nil, fmt.Errorf("cannot encode textual union: non-nil values ought to be specified with Go map[string]interface{}, with single key equal to type name, and value equal to datum value: %v; received: %T", cr.allowedTypes, datum)
	}
}
func textualJSONFromNativeAvro(cr *codecInfo) func(buf []byte, datum interface{}) ([]byte, error) {
	return func(buf []byte, datum interface{}) ([]byte, error) {
		switch v := datum.(type) {
		case nil:
			_, ok := cr.indexFromName["null"]
			if !ok {
				return nil, fmt.Errorf("cannot encode textual union: no member schema types support datum: allowed types: %v; received: %T", cr.allowedTypes, datum)
			}
			return append(buf, "null"...), nil
		case map[string]interface{}:
			if len(v) != 1 {
				return nil, fmt.Errorf("cannot encode textual union: non-nil Union values ought to be specified with Go map[string]interface{}, with single key equal to type name, and value equal to datum value: %v; received: %T", cr.allowedTypes, datum)
			}
			// will execute exactly once
			for key, value := range v {
				index, ok := cr.indexFromName[key]
				if !ok {
					return nil, fmt.Errorf("cannot encode textual union: no member schema types support datum: allowed types: %v; received: %T", cr.allowedTypes, datum)
				}
				var err error
				c := cr.codecFromIndex[index]
				buf, err = c.textualFromNative(buf, value)
				if err != nil {
					return nil, fmt.Errorf("cannot encode textual union: %s", err)
				}
				return buf, nil
			}
		}
		return nil, fmt.Errorf("cannot encode textual union: non-nil values ought to be specified with Go map[string]interface{}, with single key equal to type name, and value equal to datum value: %v; received: %T", cr.allowedTypes, datum)
	}
}
func buildCodecForTypeDescribedBySlice(st map[string]*Codec, enclosingNamespace string, schemaArray []interface{}, cb *codecBuilder) (*Codec, error) {
	if len(schemaArray) == 0 {
		return nil, errors.New("Union ought to have one or more members")
	}

	cr, err := makeCodecInfo(st, enclosingNamespace, schemaArray, cb)
	if err != nil {
		return nil, err
	}

	rv := &Codec{
		// NOTE: To support record field default values, union schema set to the
		// type name of first member
		// TODO: add/change to schemaCanonical below
		schemaOriginal: cr.codecFromIndex[0].typeName.fullName,

		typeName:          &name{"union", nullNamespace},
		nativeFromBinary:  unionNativeFromBinary(&cr),
		binaryFromNative:  unionBinaryFromNative(&cr),
		nativeFromTextual: unionNativeFromTextual(&cr),
		textualFromNative: unionTextualFromNative(&cr),
	}
	return rv, nil
}

// Standard JSON
//
// The default avro library supports a json that would result from your data into json
// instead of serializing it into binary
//
// JSON in the wild differs from that in one critical way - unions
// the avro spec requires unions to have their type indicated
// which means every value that is of a union type
// is actually sent as a small map {"string", "some string"}
// instead of simply as the value itself, which is the way of wild JSON
// https://avro.apache.org/docs/current/spec.html#json_encoding
//
// In order to use this to avro encode standard json the unions have to be rewritten
// so the can encode into unions as expected by the avro schema
//
// so the technique is to read in the json in the usual way
// when a union type is found, read the next json object
// try to figure out if it fits into any of the types
// that are specified for the union per the supplied schema
// if so, then wrap the value into a map and return the expected Union
//
// the json is morphed on the read side
// and then it will remain avro-json object
// avro data is not serialized back into standard json
// the data goes to avro-json and stays that way
func buildCodecForTypeDescribedBySliceOneWayJSON(st map[string]*Codec, enclosingNamespace string, schemaArray []interface{}, cb *codecBuilder) (*Codec, error) {
	if len(schemaArray) == 0 {
		return nil, errors.New("Union ought to have one or more members")
	}

	cr, err := makeCodecInfo(st, enclosingNamespace, schemaArray, cb)
	if err != nil {
		return nil, err
	}

	rv := &Codec{
		// NOTE: To support record field default values, union schema set to the
		// type name of first member
		// TODO: add/change to schemaCanonical below
		schemaOriginal: cr.codecFromIndex[0].typeName.fullName,

		typeName:          &name{"union", nullNamespace},
		nativeFromBinary:  unionNativeFromBinary(&cr),
		binaryFromNative:  unionBinaryFromNative(&cr),
		nativeFromTextual: nativeAvroFromTextualJSON(&cr),
		textualFromNative: unionTextualFromNative(&cr),
	}
	return rv, nil
}
func buildCodecForTypeDescribedBySliceTwoWayJSON(st map[string]*Codec, enclosingNamespace string, schemaArray []interface{}, cb *codecBuilder) (*Codec, error) {
	if len(schemaArray) == 0 {
		return nil, errors.New("Union ought to have one or more members")
	}

	cr, err := makeCodecInfo(st, enclosingNamespace, schemaArray, cb)
	if err != nil {
		return nil, err
	}

	rv := &Codec{
		// NOTE: To support record field default values, union schema set to the
		// type name of first member
		// TODO: add/change to schemaCanonical below
		schemaOriginal: cr.codecFromIndex[0].typeName.fullName,

		typeName:          &name{"union", nullNamespace},
		nativeFromBinary:  unionNativeFromBinary(&cr),
		binaryFromNative:  unionBinaryFromNative(&cr),
		nativeFromTextual: nativeAvroFromTextualJSON(&cr),
		textualFromNative: textualJSONFromNativeAvro(&cr),
	}
	return rv, nil
}

func checkAll(allowedTypes []string, cr *codecInfo, buf []byte) (interface{}, []byte, error) {
	for _, name := range cr.allowedTypes {
		if name == "null" {
			// skip null since we know we already got type float64
			continue
		}
		theCodec, ok := cr.codecFromName[name]
		if !ok {
			continue
		}
		rv, rb, err := theCodec.NativeFromTextual(buf)
		if err != nil {
			continue
		}
		return map[string]interface{}{name: rv}, rb, nil
	}
	return nil, buf, fmt.Errorf("could not decode any json data in input %v", string(buf))
}
func nativeAvroFromTextualJSON(cr *codecInfo) func(buf []byte) (interface{}, []byte, error) {
	return func(buf []byte) (interface{}, []byte, error) {

		reader := bytes.NewReader(buf)
		dec := json.NewDecoder(reader)
		var m interface{}

		// i should be able to grab the next json "value" with decoder.Decode()
		// https://pkg.go.dev/encoding/json#Decoder.Decode
		// that dec.More() loop will give the next
		// whatever then dec.Decode(&m)
		// if m is interface{}
		// it goes one legit json object at a time like this
		// json.Delim: [
		// Q:map[string]interface {}: map[Name:Ed Text:Knock knock.]
		// Q:map[string]interface {}: map[Name:Sam Text:Who's there?]
		// Q:map[string]interface {}: map[Name:Ed Text:Go fmt.]
		// Q:map[string]interface {}: map[Name:Sam Text:Go fmt who?]
		// Q:map[string]interface {}: map[Name:Ed Text:Go fmt yourself!]
		// string: eewew
		// bottom:json.Delim: ]
		//
		// so right here, grab whatever this object is
		// grab the object specified as the value
		// and try to figure out what it is and handle it
		err := dec.Decode(&m)
		if err != nil {
			return nil, buf, err
		}

		allowedTypes := cr.allowedTypes

		switch m.(type) {
		case nil:
			if len(buf) >= 4 && bytes.Equal(buf[:4], []byte("null")) {
				if _, ok := cr.codecFromName["null"]; ok {
					return nil, buf[4:], nil
				}
			}
		case float64:
			// dec.Decode turns them all into float64
			// avro spec knows about int, long (variable length zig-zag)
			// and then float and double (32 bits, 64 bits)
			// https://avro.apache.org/docs/current/spec.html#binary_encode_primitive
			//

			// double
			// doubleNativeFromTextual
			// float
			// floatNativeFromTextual
			// long
			// longNativeFromTextual
			// int
			// intNativeFromTextual

			// sorted so it would be
			// double, float, int, long
			// that makes the priorities right by chance
			sort.Strings(cr.allowedTypes)

		case map[string]interface{}:

			// try to decode it as a map
			// because a map should fail faster than a record
			// if that fails assume record and return it
			sort.Strings(cr.allowedTypes)
		}

		return checkAll(allowedTypes, cr, buf)

	}
}
