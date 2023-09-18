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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
)

var (
	// MaxBlockCount is the maximum number of data items allowed in a single
	// block that will be decoded from a binary stream, whether when reading
	// blocks to decode an array or a map, or when reading blocks from an OCF
	// stream. This check is to ensure decoding binary data will not cause the
	// library to over allocate RAM, potentially creating a denial of service on
	// the system.
	//
	// If a particular application needs to decode binary Avro data that
	// potentially has more data items in a single block, then this variable may
	// be modified at your discretion.
	MaxBlockCount = int64(math.MaxInt32)

	// MaxBlockSize is the maximum number of bytes that will be allocated for a
	// single block of data items when decoding from a binary stream. This check
	// is to ensure decoding binary data will not cause the library to over
	// allocate RAM, potentially creating a denial of service on the system.
	//
	// If a particular application needs to decode binary Avro data that
	// potentially has more bytes in a single block, then this variable may be
	// modified at your discretion.
	MaxBlockSize = int64(math.MaxInt32)
)

// Codec supports decoding binary and text Avro data to Go native data types,
// and conversely encoding Go native data types to binary or text Avro data. A
// Codec is created as a stateless structure that can be safely used in multiple
// go routines simultaneously.
type Codec struct {
	soeHeader       []byte // single-object-encoding header
	schemaOriginal  string
	schemaCanonical string
	typeName        *name

	nativeFromTextual func([]byte) (interface{}, []byte, error)
	binaryFromNative  func([]byte, interface{}) ([]byte, error)
	nativeFromBinary  func([]byte) (interface{}, []byte, error)
	textualFromNative func([]byte, interface{}) ([]byte, error)

	Rabin uint64
}

// codecBuilder holds the 3 kinds of codec builders so they can be
// replaced if needed
// and so they can be passed down the call stack during codec building
type codecBuilder struct {
	mapBuilder    func(st map[string]*Codec, enclosingNamespace string, schemaMap map[string]interface{}, cb *codecBuilder) (*Codec, error)
	stringBuilder func(st map[string]*Codec, enclosingNamespace string, typeName string, schemaMap map[string]interface{}, cb *codecBuilder) (*Codec, error)
	sliceBuilder  func(st map[string]*Codec, enclosingNamespace string, schemaArray []interface{}, cb *codecBuilder) (*Codec, error)
}

// NewCodec returns a Codec used to translate between a byte slice of either
// binary or textual Avro data and native Go data.
//
// Creating a `Codec` is fast, but ought to be performed exactly once per Avro
// schema to process. Once a `Codec` is created, it may be used multiple times
// to convert data between native form and binary Avro representation, or
// between native form and textual Avro representation.
//
// A particular `Codec` can work with only one Avro schema. However,
// there is no practical limit to how many `Codec`s may be created and
// used in a program. Internally a `Codec` is merely a named tuple of
// four function pointers, and maintains no runtime state that is mutated
// after instantiation. In other words, `Codec`s may be safely used by
// many go routines simultaneously, as your program requires.
//
//	codec, err := goavro.NewCodec(`
//	    {
//	      "type": "record",
//	      "name": "LongList",
//	      "fields" : [
//	        {"name": "next", "type": ["null", "LongList"], "default": null}
//	      ]
//	    }`)
//	if err != nil {
//	        fmt.Println(err)
//	}
func NewCodec(schemaSpecification string) (*Codec, error) {
	return NewCodecFrom(schemaSpecification, &codecBuilder{
		buildCodecForTypeDescribedByMap,
		buildCodecForTypeDescribedByString,
		buildCodecForTypeDescribedBySlice,
	})
}

// NewCodecForStandardJSON returns a codec that uses a special union
// processing code that allows normal json to be ingested via an
// avro schema, by inferring the "type" intended for union types.
//
// This is the one-way code to get such json into the avro system
// and the deserialization is not supported in this codec - its
// json into avro-json one-way and one-way only for this codec.
//
// The "type" inference is done by using the types specified as
// potentially acceptable types for the union, and trying to
// unpack the incomin json into each of the specified types for
// the union type. See union.go +/Standard JSON/ for a general
// description of the problem and details of the solution
// are in union.go +/nativeAvroFromTextualJson/
//
// For a general description of a codex seen the comment for NewCodec
// above.
//
// The following is the exact same schema used in the above
// code for NewCodec:
//
//	codec, err := goavro.NewCodecForStandardJSON(`
//	    {
//	      "type": "record",
//	      "name": "LongList",
//	      "fields" : [
//	        {"name": "next", "type": ["null", "LongList"], "default": null}
//	      ]
//	    }`)
//	if err != nil {
//	        fmt.Println(err)
//	}
//
// The above will take json of this sort:
//
// {"next": null}
//
// {"next":{"next":null}}
//
// {"next":{"next":{"next":null}}}
//
// For more examples see the test cases in union_test.go
func NewCodecForStandardJSON(schemaSpecification string) (*Codec, error) {
	return NewCodecFrom(schemaSpecification, &codecBuilder{
		buildCodecForTypeDescribedByMap,
		buildCodecForTypeDescribedByString,
		buildCodecForTypeDescribedBySliceOneWayJSON,
	})
}

// NewCodecForStandardJSONOneWay is an alias for NewCodecForStandardJSON
// added to make the transition to two-way json handling more smooth
//
// This will unambiguously provide OneWay avro encoding for standard
// internet json. This takes in internet json, and brings it into
// the avro world, but the deserialization retains the unique
// form of normal avro-friendly json where unions have their
// types types specified in stream like this example from
// the official docs // https://avro.apache.org/docs/1.11.1/api/c/
//
// `{"string": "Follow your bliss."}`
//
// To be clear this means the incoming json string:
//
// "Follow your bliss."
//
// would deserialize according to the avro-json expectations to:
//
// `{"string": "Follow your bliss."}`
//
// To get full two-way support see the below NewCodecForStandardJSONFull
func NewCodecForStandardJSONOneWay(schemaSpecification string) (*Codec, error) {
	return NewCodecForStandardJSON(schemaSpecification)
}

// NewCodecForStandardJSONFull provides full serialization/deserialization
// for json that meets the expectations of regular internet json, viewed as
// something distinct from avro-json which has special handling for union
// types.  For details see the above comments.
//
// With this `codec` you can expect to see a json string like this:
//
// "Follow your bliss."
//
// to deserialize into the same json structure
//
// "Follow your bliss."
func NewCodecForStandardJSONFull(schemaSpecification string) (*Codec, error) {
	return NewCodecFrom(schemaSpecification, &codecBuilder{
		buildCodecForTypeDescribedByMap,
		buildCodecForTypeDescribedByString,
		buildCodecForTypeDescribedBySliceTwoWayJSON,
	})
}

func NewCodecFrom(schemaSpecification string, cb *codecBuilder) (*Codec, error) {
	var schema interface{}

	if err := json.Unmarshal([]byte(schemaSpecification), &schema); err != nil {
		return nil, fmt.Errorf("cannot unmarshal schema JSON: %s", err)
	}

	// bootstrap a symbol table with primitive type codecs for the new codec
	st := newSymbolTable()

	c, err := buildCodec(st, nullNamespace, schema, cb)
	if err != nil {
		return nil, err
	}
	c.schemaCanonical, err = parsingCanonicalForm(schema, "", make(map[string]string))
	if err != nil {
		return nil, err // should not get here because schema was validated above
	}

	c.Rabin = rabin([]byte(c.schemaCanonical))
	c.soeHeader = []byte{0xC3, 0x01, 0, 0, 0, 0, 0, 0, 0, 0}
	binary.LittleEndian.PutUint64(c.soeHeader[2:], c.Rabin)

	c.schemaOriginal = schemaSpecification
	return c, nil
}

func newSymbolTable() map[string]*Codec {
	return map[string]*Codec{
		"boolean": {
			typeName:          &name{"boolean", nullNamespace},
			schemaOriginal:    "boolean",
			schemaCanonical:   "boolean",
			binaryFromNative:  booleanBinaryFromNative,
			nativeFromBinary:  booleanNativeFromBinary,
			nativeFromTextual: booleanNativeFromTextual,
			textualFromNative: booleanTextualFromNative,
		},
		"bytes": {
			typeName:          &name{"bytes", nullNamespace},
			schemaOriginal:    "bytes",
			schemaCanonical:   "bytes",
			binaryFromNative:  bytesBinaryFromNative,
			nativeFromBinary:  bytesNativeFromBinary,
			nativeFromTextual: bytesNativeFromTextual,
			textualFromNative: bytesTextualFromNative,
		},
		"double": {
			typeName:          &name{"double", nullNamespace},
			schemaOriginal:    "double",
			schemaCanonical:   "double",
			binaryFromNative:  doubleBinaryFromNative,
			nativeFromBinary:  doubleNativeFromBinary,
			nativeFromTextual: doubleNativeFromTextual,
			textualFromNative: doubleTextualFromNative,
		},
		"float": {
			typeName:          &name{"float", nullNamespace},
			schemaOriginal:    "float",
			schemaCanonical:   "float",
			binaryFromNative:  floatBinaryFromNative,
			nativeFromBinary:  floatNativeFromBinary,
			nativeFromTextual: floatNativeFromTextual,
			textualFromNative: floatTextualFromNative,
		},
		"int": {
			typeName:          &name{"int", nullNamespace},
			schemaOriginal:    "int",
			schemaCanonical:   "int",
			binaryFromNative:  intBinaryFromNative,
			nativeFromBinary:  intNativeFromBinary,
			nativeFromTextual: intNativeFromTextual,
			textualFromNative: intTextualFromNative,
		},
		"long": {
			typeName:          &name{"long", nullNamespace},
			schemaOriginal:    "long",
			schemaCanonical:   "long",
			binaryFromNative:  longBinaryFromNative,
			nativeFromBinary:  longNativeFromBinary,
			nativeFromTextual: longNativeFromTextual,
			textualFromNative: longTextualFromNative,
		},
		"null": {
			typeName:          &name{"null", nullNamespace},
			schemaOriginal:    "null",
			schemaCanonical:   "null",
			binaryFromNative:  nullBinaryFromNative,
			nativeFromBinary:  nullNativeFromBinary,
			nativeFromTextual: nullNativeFromTextual,
			textualFromNative: nullTextualFromNative,
		},
		"string": {
			typeName:          &name{"string", nullNamespace},
			schemaOriginal:    "string",
			schemaCanonical:   "string",
			binaryFromNative:  stringBinaryFromNative,
			nativeFromBinary:  stringNativeFromBinary,
			nativeFromTextual: stringNativeFromTextual,
			textualFromNative: stringTextualFromNative,
		},
		// Start of compiled logical types using format typeName.logicalType where there is
		// no dependence on schema.
		"long.timestamp-millis": {
			typeName:          &name{"long.timestamp-millis", nullNamespace},
			schemaOriginal:    "long",
			schemaCanonical:   "long",
			nativeFromTextual: nativeFromTimeStampMillis(longNativeFromTextual),
			binaryFromNative:  timeStampMillisFromNative(longBinaryFromNative),
			nativeFromBinary:  nativeFromTimeStampMillis(longNativeFromBinary),
			textualFromNative: timeStampMillisFromNative(longTextualFromNative),
		},
		"long.timestamp-micros": {
			typeName:          &name{"long.timestamp-micros", nullNamespace},
			schemaOriginal:    "long",
			schemaCanonical:   "long",
			nativeFromTextual: nativeFromTimeStampMicros(longNativeFromTextual),
			binaryFromNative:  timeStampMicrosFromNative(longBinaryFromNative),
			nativeFromBinary:  nativeFromTimeStampMicros(longNativeFromBinary),
			textualFromNative: timeStampMicrosFromNative(longTextualFromNative),
		},
		"int.time-millis": {
			typeName:          &name{"int.time-millis", nullNamespace},
			schemaOriginal:    "int",
			schemaCanonical:   "int",
			nativeFromTextual: nativeFromTimeMillis(intNativeFromTextual),
			binaryFromNative:  timeMillisFromNative(intBinaryFromNative),
			nativeFromBinary:  nativeFromTimeMillis(intNativeFromBinary),
			textualFromNative: timeMillisFromNative(intTextualFromNative),
		},
		"long.time-micros": {
			typeName:          &name{"long.time-micros", nullNamespace},
			schemaOriginal:    "long",
			schemaCanonical:   "long",
			nativeFromTextual: nativeFromTimeMicros(longNativeFromTextual),
			binaryFromNative:  timeMicrosFromNative(longBinaryFromNative),
			nativeFromBinary:  nativeFromTimeMicros(longNativeFromBinary),
			textualFromNative: timeMicrosFromNative(longTextualFromNative),
		},
		"int.date": {
			typeName:          &name{"int.date", nullNamespace},
			schemaOriginal:    "int",
			schemaCanonical:   "int",
			nativeFromTextual: nativeFromDate(intNativeFromTextual),
			binaryFromNative:  dateFromNative(intBinaryFromNative),
			nativeFromBinary:  nativeFromDate(intNativeFromBinary),
			textualFromNative: dateFromNative(intTextualFromNative),
		},
	}
}

// BinaryFromNative appends the binary encoded byte slice representation of the
// provided native datum value to the provided byte slice in accordance with the
// Avro schema supplied when creating the Codec.  It is supplied a byte slice to
// which to append the binary encoded data along with the actual data to encode.
// On success, it returns a new byte slice with the encoded bytes appended, and
// a nil error value.  On error, it returns the original byte slice, and the
// error message.
//
//	func ExampleBinaryFromNative() {
//	    codec, err := goavro.NewCodec(`
//	        {
//	          "type": "record",
//	          "name": "LongList",
//	          "fields" : [
//	            {"name": "next", "type": ["null", "LongList"], "default": null}
//	          ]
//	        }`)
//	    if err != nil {
//	        fmt.Println(err)
//	    }
//
//	    // Convert native Go form to binary Avro data
//	    binary, err := codec.BinaryFromNative(nil, map[string]interface{}{
//	        "next": map[string]interface{}{
//	            "LongList": map[string]interface{}{
//	                "next": map[string]interface{}{
//	                    "LongList": map[string]interface{}{
//	                    // NOTE: May omit fields when using default value
//	                    },
//	                },
//	            },
//	        },
//	    })
//	    if err != nil {
//	        fmt.Println(err)
//	    }
//
//	    fmt.Printf("%#v", binary)
//	    // Output: []byte{0x2, 0x2, 0x0}
//	}
func (c *Codec) BinaryFromNative(buf []byte, datum interface{}) ([]byte, error) {
	newBuf, err := c.binaryFromNative(buf, datum)
	if err != nil {
		return buf, err // if error, return original byte slice
	}
	return newBuf, nil
}

// NativeFromBinary returns a native datum value from the binary encoded byte
// slice in accordance with the Avro schema supplied when creating the Codec. On
// success, it returns the decoded datum, a byte slice containing the remaining
// undecoded bytes, and a nil error value. On error, it returns nil for
// the datum value, the original byte slice, and the error message.
//
//	func ExampleNativeFromBinary() {
//	    codec, err := goavro.NewCodec(`
//	        {
//	          "type": "record",
//	          "name": "LongList",
//	          "fields" : [
//	            {"name": "next", "type": ["null", "LongList"], "default": null}
//	          ]
//	        }`)
//	    if err != nil {
//	        fmt.Println(err)
//	    }
//
//	    // Convert native Go form to binary Avro data
//	    binary := []byte{0x2, 0x2, 0x0}
//
//	    native, _, err := codec.NativeFromBinary(binary)
//	    if err != nil {
//	        fmt.Println(err)
//	    }
//
//	    fmt.Printf("%v", native)
//	    // Output: map[next:map[LongList:map[next:map[LongList:map[next:<nil>]]]]]
//	}
func (c *Codec) NativeFromBinary(buf []byte) (interface{}, []byte, error) {
	value, newBuf, err := c.nativeFromBinary(buf)
	if err != nil {
		return nil, buf, err // if error, return original byte slice
	}
	return value, newBuf, nil
}

// NativeFromSingle converts Avro data from Single-Object-Encoded format from
// the provided byte slice to Go native data types in accordance with the Avro
// schema supplied when creating the Codec.  On success, it returns the decoded
// datum, along with a new byte slice with the decoded bytes consumed, and a nil
// error value.  On error, it returns nil for the datum value, the original byte
// slice, and the error message.
//
//	func decode(codec *goavro.Codec, buf []byte) error {
//	    datum, _, err := codec.NativeFromSingle(buf)
//	    if err != nil {
//	        return err
//	    }
//	    _, err = fmt.Println(datum)
//	    return err
//	}
func (c *Codec) NativeFromSingle(buf []byte) (interface{}, []byte, error) {
	fingerprint, newBuf, err := FingerprintFromSOE(buf)
	if err != nil {
		return nil, buf, err
	}
	if !bytes.Equal(buf[:len(c.soeHeader)], c.soeHeader) {
		return nil, buf, ErrWrongCodec(fingerprint)
	}
	value, newBuf, err := c.nativeFromBinary(newBuf)
	if err != nil {
		return nil, buf, err // if error, return original byte slice
	}
	return value, newBuf, nil
}

// NativeFromTextual converts Avro data in JSON text format from the provided byte
// slice to Go native data types in accordance with the Avro schema supplied
// when creating the Codec. On success, it returns the decoded datum, along with
// a new byte slice with the decoded bytes consumed, and a nil error value. On
// error, it returns nil for the datum value, the original byte slice, and the
// error message.
//
//	func ExampleNativeFromTextual() {
//	    codec, err := goavro.NewCodec(`
//	        {
//	          "type": "record",
//	          "name": "LongList",
//	          "fields" : [
//	            {"name": "next", "type": ["null", "LongList"], "default": null}
//	          ]
//	        }`)
//	    if err != nil {
//	        fmt.Println(err)
//	    }
//
//	    // Convert native Go form to text Avro data
//	    text := []byte(`{"next":{"LongList":{"next":{"LongList":{"next":null}}}}}`)
//
//	    native, _, err := codec.NativeFromTextual(text)
//	    if err != nil {
//	        fmt.Println(err)
//	    }
//
//	    fmt.Printf("%v", native)
//	    // Output: map[next:map[LongList:map[next:map[LongList:map[next:<nil>]]]]]
//	}
func (c *Codec) NativeFromTextual(buf []byte) (interface{}, []byte, error) {
	value, newBuf, err := c.nativeFromTextual(buf)
	if err != nil {
		return nil, buf, err // if error, return original byte slice
	}
	return value, newBuf, nil
}

// SingleFromNative appends the single-object-encoding byte slice representation
// of the provided native datum value to the provided byte slice in accordance
// with the Avro schema supplied when creating the Codec.  It is supplied a byte
// slice to which to append the header and binary encoded data, along with the
// actual data to encode.  On success, it returns a new byte slice with the
// encoded bytes appended, and a nil error value.  On error, it returns the
// original byte slice, and the error message.
//
//	func ExampleSingleItemEncoding() {
//	    codec, err := goavro.NewCodec(`"int"`)
//	    if err != nil {
//	        fmt.Fprintf(os.Stderr, "%s\n", err)
//	        return
//	    }
//
//	    buf, err := codec.SingleFromNative(nil, 3)
//	    if err != nil {
//	        fmt.Fprintf(os.Stderr, "%s\n", err)
//	        return
//	    }
//
//	    fmt.Println(buf)
//	    // Output: [195 1 143 92 57 63 26 213 117 114 6]
//	}
func (c *Codec) SingleFromNative(buf []byte, datum interface{}) ([]byte, error) {
	newBuf, err := c.binaryFromNative(append(buf, c.soeHeader...), datum)
	if err != nil {
		return buf, err
	}
	return newBuf, nil
}

// TextualFromNative converts Go native data types to Avro data in JSON text format in
// accordance with the Avro schema supplied when creating the Codec. It is
// supplied a byte slice to which to append the encoded data and the actual data
// to encode. On success, it returns a new byte slice with the encoded bytes
// appended, and a nil error value. On error, it returns the original byte
// slice, and the error message.
//
//	func ExampleTextualFromNative() {
//	    codec, err := goavro.NewCodec(`
//	        {
//	          "type": "record",
//	          "name": "LongList",
//	          "fields" : [
//	            {"name": "next", "type": ["null", "LongList"], "default": null}
//	          ]
//	        }`)
//	    if err != nil {
//	        fmt.Println(err)
//	    }
//
//	    // Convert native Go form to text Avro data
//	    text, err := codec.TextualFromNative(nil, map[string]interface{}{
//	        "next": map[string]interface{}{
//	            "LongList": map[string]interface{}{
//	                "next": map[string]interface{}{
//	                    "LongList": map[string]interface{}{
//	                    // NOTE: May omit fields when using default value
//	                    },
//	                },
//	            },
//	        },
//	    })
//	    if err != nil {
//	        fmt.Println(err)
//	    }
//
//	    fmt.Printf("%s", text)
//	    // Output: {"next":{"LongList":{"next":{"LongList":{"next":null}}}}}
//	}
func (c *Codec) TextualFromNative(buf []byte, datum interface{}) ([]byte, error) {
	newBuf, err := c.textualFromNative(buf, datum)
	if err != nil {
		return buf, err // if error, return original byte slice
	}
	return newBuf, nil
}

// Schema returns the original schema used to create the Codec.
func (c *Codec) Schema() string {
	return c.schemaOriginal
}

// CanonicalSchema returns the Parsing Canonical Form of the schema according to
// the Avro specification.
func (c *Codec) CanonicalSchema() string {
	return c.schemaCanonical
}

// SchemaCRC64Avro returns a signed 64-bit integer Rabin fingerprint for the
// canonical schema.  This method returns the signed 64-bit cast of the unsigned
// 64-bit schema Rabin fingerprint.
//
// Deprecated: This method has been replaced by the Rabin structure Codec field
// and is provided for backward compatibility only.
func (c *Codec) SchemaCRC64Avro() int64 {
	return int64(c.Rabin)
}

// convert a schema data structure to a codec, prefixing with specified
// namespace
func buildCodec(st map[string]*Codec, enclosingNamespace string, schema interface{}, cb *codecBuilder) (*Codec, error) {
	switch schemaType := schema.(type) {
	case map[string]interface{}:
		return cb.mapBuilder(st, enclosingNamespace, schemaType, cb)
	case string:
		return cb.stringBuilder(st, enclosingNamespace, schemaType, nil, cb)
	case []interface{}:
		return cb.sliceBuilder(st, enclosingNamespace, schemaType, cb)
	default:
		return nil, fmt.Errorf("unknown schema type: %T", schema)
	}
}

// Reach into the map, grabbing its "type". Use that to create the codec.
func buildCodecForTypeDescribedByMap(st map[string]*Codec, enclosingNamespace string, schemaMap map[string]interface{}, cb *codecBuilder) (*Codec, error) {
	t, ok := schemaMap["type"]
	if !ok {
		return nil, fmt.Errorf("missing type: %v", schemaMap)
	}

	switch v := t.(type) {
	case string:
		// Already defined types may be abbreviated with its string name.
		// EXAMPLE: "type":"array"
		// EXAMPLE: "type":"enum"
		// EXAMPLE: "type":"fixed"
		// EXAMPLE: "type":"int"
		// EXAMPLE: "type":"record"
		// EXAMPLE: "type":"somePreviouslyDefinedCustomTypeString"
		return cb.stringBuilder(st, enclosingNamespace, v, schemaMap, cb)
	case map[string]interface{}:
		return cb.mapBuilder(st, enclosingNamespace, v, cb)
	case []interface{}:
		return cb.sliceBuilder(st, enclosingNamespace, v, cb)
	default:
		return nil, fmt.Errorf("type ought to be either string, map[string]interface{}, or []interface{}; received: %T", t)
	}
}

func buildCodecForTypeDescribedByString(st map[string]*Codec, enclosingNamespace string, typeName string, schemaMap map[string]interface{}, cb *codecBuilder) (*Codec, error) {
	isLogicalType := false
	searchType := typeName
	// logicalType will be non-nil for those fields without a logicalType property set
	if lt := schemaMap["logicalType"]; lt != nil {
		isLogicalType = true
		searchType = fmt.Sprintf("%s.%s", typeName, lt)
	}

	// NOTE: When codec already exists, return it. This includes both primitive and
	// logicalType codecs added in NewCodec, and user-defined types, added while
	// building the codec.
	if cd, ok := st[searchType]; ok {

		// For "bytes.decimal" types verify that the scale and precision in this schema map match a cached codec before
		// using the cached codec in favor of creating a new codec.
		if searchType == "bytes.decimal" {

			// Search the cached codecs for a "bytes.decimal" codec  with a "precision" and "scale" specified in the key,
			// only if that matches return the cached codec. Otherwise, create a new codec for this "bytes.decimal".
			decimalSearchType := fmt.Sprintf("bytes.decimal.%d.%d", int(schemaMap["precision"].(float64)), int(schemaMap["scale"].(float64)))
			if cd2, ok := st[decimalSearchType]; ok {
				return cd2, nil
			}

		} else {
			return cd, nil
		}
	}

	// Avro specification allows abbreviation of type name inside a namespace.
	if enclosingNamespace != "" {
		if cd, ok := st[enclosingNamespace+"."+typeName]; ok {
			return cd, nil
		}
	}

	// There are only a small handful of complex Avro data types.
	switch searchType {
	case "array":
		return makeArrayCodec(st, enclosingNamespace, schemaMap, cb)
	case "enum":
		return makeEnumCodec(st, enclosingNamespace, schemaMap)
	case "fixed":
		return makeFixedCodec(st, enclosingNamespace, schemaMap)
	case "map":
		return makeMapCodec(st, enclosingNamespace, schemaMap, cb)
	case "record":
		return makeRecordCodec(st, enclosingNamespace, schemaMap, cb)
	case "bytes.decimal":
		return makeDecimalBytesCodec(st, enclosingNamespace, schemaMap)
	case "fixed.decimal":
		return makeDecimalFixedCodec(st, enclosingNamespace, schemaMap)
	case "string.validated-string":
		return makeValidatedStringCodec(st, enclosingNamespace, schemaMap)
	default:
		if isLogicalType {
			delete(schemaMap, "logicalType")
			return buildCodecForTypeDescribedByString(st, enclosingNamespace, typeName, schemaMap, cb)
		}
		return nil, fmt.Errorf("unknown type name: %q", searchType)
	}
}

// notion of enclosing namespace changes when record, enum, or fixed create a
// new namespace, for child objects.
func registerNewCodec(st map[string]*Codec, schemaMap map[string]interface{}, enclosingNamespace string) (*Codec, error) {
	n, err := newNameFromSchemaMap(enclosingNamespace, schemaMap)
	if err != nil {
		return nil, err
	}
	c := &Codec{typeName: n}
	st[n.fullName] = c
	return c, nil
}

// ErrWrongCodec is returned when an attempt is made to decode a single-object
// encoded value using the wrong codec.
type ErrWrongCodec uint64

func (e ErrWrongCodec) Error() string { return "wrong codec: " + strconv.FormatUint(uint64(e), 10) }

// ErrNotSingleObjectEncoded is returned when an attempt is made to decode a
// single-object encoded value from a buffer that does not have the correct
// magic prefix.
type ErrNotSingleObjectEncoded string

func (e ErrNotSingleObjectEncoded) Error() string {
	return "cannot decode buffer as single-object encoding: " + string(e)
}
