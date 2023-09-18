# go-reflect

![Go](https://github.com/goccy/go-reflect/workflows/Go/badge.svg)
[![GoDoc](https://godoc.org/github.com/goccy/go-reflect?status.svg)](https://pkg.go.dev/github.com/goccy/go-reflect?tab=doc)
[![codecov](https://codecov.io/gh/goccy/go-reflect/branch/master/graph/badge.svg)](https://codecov.io/gh/goccy/go-reflect)
[![Go Report Card](https://goreportcard.com/badge/github.com/goccy/go-reflect)](https://goreportcard.com/report/github.com/goccy/go-reflect)

Zero-allocation reflection library for Go

# Features

- 100% Compatibility APIs with `reflect` library
- No allocation occurs when using the reflect.Type features
- You can choose to escape ( `reflect.ValueOf` ) or noescape ( `reflect.ValueNoEscapeOf` ) when creating reflect.Value

# Status

All the tests in the reflect library have been passed
except the tests that use some private functions.

# Installation

```bash
go get github.com/goccy/go-reflect
```

# How to use

Replace import statement from `reflect` to `github.com/goccy/go-reflect`

```bash
-import "reflect"
+import "github.com/goccy/go-reflect"
```

# Benchmarks

Source https://github.com/goccy/go-reflect/blob/master/benchmark_test.go

## Benchmark about reflect.Type

```
$ go test -bench TypeOf
```

```
goos: darwin
goarch: amd64
pkg: github.com/goccy/go-reflect
Benchmark_TypeOf_Reflect-12             100000000               13.8 ns/op             8 B/op          1 allocs/op
Benchmark_TypeOf_GoReflect-12           2000000000               1.70 ns/op            0 B/op          0 allocs/op
PASS
ok      github.com/goccy/go-reflect     5.369s
```

## Benchmark about reflect.Value

```
$ go test -bench ValueOf
```

```
goos: darwin
goarch: amd64
pkg: github.com/goccy/go-reflect
Benchmark_ValueOf_Reflect-12            100000000               13.0 ns/op             8 B/op          1 allocs/op
Benchmark_ValueOf_GoReflect-12          300000000                4.64 ns/op            0 B/op          0 allocs/op
PASS
ok      github.com/goccy/go-reflect     3.578s
```

# Real World Example

## Implements Fast Marshaler

I would like to introduce the technique I use for [github.com/goccy/go-json](https://github.com/goccy/go-json).  
Using this technique, allocation can be suppressed to once for any marshaler.  

Original Source is https://github.com/goccy/go-reflect/blob/master/benchmark_marshaler_test.go

```go
package reflect_test

import (
    "errors"
    "strconv"
    "sync"
    "testing"
    "unsafe"

    "github.com/goccy/go-reflect"
)

var (
    typeToEncoderMap sync.Map
    bufpool          = sync.Pool{
        New: func() interface{} {
            return &buffer{
                b: make([]byte, 0, 1024),
            }
        },
    }
)

type buffer struct {
    b []byte
}

type encoder func(*buffer, unsafe.Pointer) error

func Marshal(v interface{}) ([]byte, error) {

    // Technique 1.
    // Get type information and pointer from interface{} value without allocation.
    typ, ptr := reflect.TypeAndPtrOf(v)
    typeID := reflect.TypeID(typ)

    // Technique 2.
    // Reuse the buffer once allocated using sync.Pool
    buf := bufpool.Get().(*buffer)
    buf.b = buf.b[:0]
    defer bufpool.Put(buf)

    // Technique 3.
    // builds a optimized path by typeID and caches it
    if enc, ok := typeToEncoderMap.Load(typeID); ok {
        if err := enc.(encoder)(buf, ptr); err != nil {
            return nil, err
        }

        // allocate a new buffer required length only
        b := make([]byte, len(buf.b))
        copy(b, buf.b)
        return b, nil
    }

    // First time,
    // builds a optimized path by type and caches it with typeID.
    enc, err := compile(typ)
    if err != nil {
        return nil, err
    }
    typeToEncoderMap.Store(typeID, enc)
    if err := enc(buf, ptr); err != nil {
        return nil, err
    }

    // allocate a new buffer required length only
    b := make([]byte, len(buf.b))
    copy(b, buf.b)
    return b, nil
}

func compile(typ reflect.Type) (encoder, error) {
    switch typ.Kind() {
    case reflect.Struct:
        return compileStruct(typ)
    case reflect.Int:
        return compileInt(typ)
    }
    return nil, errors.New("unsupported type")
}

func compileStruct(typ reflect.Type) (encoder, error) {

    encoders := []encoder{}

    for i := 0; i < typ.NumField(); i++ {
        field := typ.Field(i)
        enc, err := compile(field.Type)
        if err != nil {
            return nil, err
        }
        offset := field.Offset
        encoders = append(encoders, func(buf *buffer, p unsafe.Pointer) error {
            return enc(buf, unsafe.Pointer(uintptr(p)+offset))
        })
    }
    return func(buf *buffer, p unsafe.Pointer) error {
        buf.b = append(buf.b, '{')
        for _, enc := range encoders {
            if err := enc(buf, p); err != nil {
                return err
            }
        }
        buf.b = append(buf.b, '}')
        return nil
    }, nil
}

func compileInt(typ reflect.Type) (encoder, error) {
    return func(buf *buffer, p unsafe.Pointer) error {
        value := *(*int)(p)
        buf.b = strconv.AppendInt(buf.b, int64(value), 10)
        return nil
    }, nil
}

func Benchmark_Marshal(b *testing.B) {
    b.ReportAllocs()
    for n := 0; n < b.N; n++ {
        bytes, err := Marshal(struct{ I int }{10})
        if err != nil {
            b.Fatal(err)
        }
        if string(bytes) != "{10}" {
            b.Fatal("unexpected error")
        }
    }
}
```

The benchmark result is as follows.  

```bash

$ go test -bench Benchmark_Marshal
goos: darwin
goarch: amd64
pkg: github.com/goccy/go-reflect
Benchmark_Marshal-16            16586372                71.0 ns/op             4 B/op          1 allocs/op
PASS
```

