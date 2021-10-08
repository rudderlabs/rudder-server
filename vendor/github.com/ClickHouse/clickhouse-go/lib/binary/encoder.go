package binary

import (
	"encoding/binary"
	"io"
	"math"
	"reflect"
	"unsafe"
)

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		output: w,
	}
}

func NewEncoderWithCompress(w io.Writer) *Encoder {
	return &Encoder{
		output:         w,
		compressOutput: NewCompressWriter(w),
	}
}

type Encoder struct {
	compress       bool
	output         io.Writer
	compressOutput io.Writer
	scratch        [binary.MaxVarintLen64]byte
}

func (enc *Encoder) SelectCompress(compress bool) {
	if enc.compressOutput == nil {
		return
	}
	if enc.compress && !compress {
		enc.Flush()
	}
	enc.compress = compress
}

func (enc *Encoder) Get() io.Writer {
	if enc.compress && enc.compressOutput != nil {
		return enc.compressOutput
	}
	return enc.output
}

func (enc *Encoder) Nullable(isNull bool) error {
	nullablePrefix := uint8(0)
	if !isNull {
		nullablePrefix = uint8(1)
	}
	if _, err := enc.Get().Write([]byte{nullablePrefix}); err != nil {
		return err
	}
	return nil
}

func (enc *Encoder) Uvarint(v uint64) error {
	ln := binary.PutUvarint(enc.scratch[:binary.MaxVarintLen64], v)
	if _, err := enc.Get().Write(enc.scratch[0:ln]); err != nil {
		return err
	}
	return nil
}

func (enc *Encoder) UvarintNullable(v *uint64) error {
	isNil := v == nil
	if err := enc.Nullable(isNil); err != nil {
		return err
	}
	if isNil {
		return enc.Uvarint(0)
	}
	return enc.Uvarint(*v)
}

func (enc *Encoder) Bool(v bool) error {
	if v {
		return enc.UInt8(1)
	}
	return enc.UInt8(0)
}

func (enc *Encoder) BoolNullable(v *bool) error {
	isNil := v == nil
	if err := enc.Nullable(isNil); err != nil {
		return err
	}
	if isNil {
		return enc.Bool(false)
	}
	return enc.Bool(*v)
}

func (enc *Encoder) Int8(v int8) error {
	return enc.UInt8(uint8(v))
}

func (enc *Encoder) Int8Nullable(v *int8) error {
	isNil := v == nil
	if err := enc.Nullable(isNil); err != nil {
		return err
	}
	if isNil {
		return enc.Int8(0)
	}
	return enc.Int8(*v)
}

func (enc *Encoder) Int16(v int16) error {
	return enc.UInt16(uint16(v))
}

func (enc *Encoder) Int16Nullable(v *int16) error {
	isNil := v == nil
	if err := enc.Nullable(isNil); err != nil {
		return err
	}
	if isNil {
		return enc.Int16(0)
	}
	return enc.Int16(*v)
}

func (enc *Encoder) Int32(v int32) error {
	return enc.UInt32(uint32(v))
}

func (enc *Encoder) Int32Nullable(v *int32) error {
	isNil := v == nil
	if err := enc.Nullable(isNil); err != nil {
		return err
	}
	if isNil {
		return enc.Int32(0)
	}
	return enc.Int32(*v)
}

func (enc *Encoder) Int64(v int64) error {
	return enc.UInt64(uint64(v))
}

func (enc *Encoder) Int64Nullable(v *int64) error {
	isNil := v == nil
	if err := enc.Nullable(isNil); err != nil {
		return err
	}
	if isNil {
		return enc.Int64(0)
	}
	return enc.Int64(*v)
}

func (enc *Encoder) UInt8(v uint8) error {
	enc.scratch[0] = v
	if _, err := enc.Get().Write(enc.scratch[:1]); err != nil {
		return err
	}
	return nil
}

func (enc *Encoder) UInt8Nullable(v *uint8) error {
	isNil := v == nil
	if err := enc.Nullable(isNil); err != nil {
		return err
	}
	if isNil {
		return enc.UInt8(0)
	}
	return enc.UInt8(*v)
}

func (enc *Encoder) UInt16(v uint16) error {
	enc.scratch[0] = byte(v)
	enc.scratch[1] = byte(v >> 8)
	if _, err := enc.Get().Write(enc.scratch[:2]); err != nil {
		return err
	}
	return nil
}

func (enc *Encoder) UInt16Nullable(v *uint16) error {
	isNil := v == nil
	if err := enc.Nullable(isNil); err != nil {
		return err
	}
	if isNil {
		return enc.UInt16(0)
	}
	return enc.UInt16(*v)
}

func (enc *Encoder) UInt32(v uint32) error {
	enc.scratch[0] = byte(v)
	enc.scratch[1] = byte(v >> 8)
	enc.scratch[2] = byte(v >> 16)
	enc.scratch[3] = byte(v >> 24)
	if _, err := enc.Get().Write(enc.scratch[:4]); err != nil {
		return err
	}
	return nil
}

func (enc *Encoder) UInt32Nullable(v *uint32) error {
	isNil := v == nil
	if err := enc.Nullable(isNil); err != nil {
		return err
	}
	if isNil {
		return enc.UInt32(0)
	}
	return enc.UInt32(*v)
}

func (enc *Encoder) UInt64(v uint64) error {
	enc.scratch[0] = byte(v)
	enc.scratch[1] = byte(v >> 8)
	enc.scratch[2] = byte(v >> 16)
	enc.scratch[3] = byte(v >> 24)
	enc.scratch[4] = byte(v >> 32)
	enc.scratch[5] = byte(v >> 40)
	enc.scratch[6] = byte(v >> 48)
	enc.scratch[7] = byte(v >> 56)
	if _, err := enc.Get().Write(enc.scratch[:8]); err != nil {
		return err
	}
	return nil
}

func (enc *Encoder) UInt64Nullable(v *uint64) error {
	isNil := v == nil
	if err := enc.Nullable(isNil); err != nil {
		return err
	}
	if isNil {
		return enc.UInt64(0)
	}
	return enc.UInt64(*v)
}

func (enc *Encoder) Float32(v float32) error {
	return enc.UInt32(math.Float32bits(v))
}

func (enc *Encoder) Float32Nullable(v *float32) error {
	isNil := v == nil
	if err := enc.Nullable(isNil); err != nil {
		return err
	}
	if isNil {
		return enc.Float32(0)
	}
	return enc.Float32(*v)
}

func (enc *Encoder) Float64(v float64) error {
	return enc.UInt64(math.Float64bits(v))
}

func (enc *Encoder) Float64Nullable(v *float64) error {
	isNil := v == nil
	if err := enc.Nullable(isNil); err != nil {
		return err
	}
	if isNil {
		return enc.Float64(0)
	}
	return enc.Float64(*v)
}

func (enc *Encoder) String(v string) error {
	str := Str2Bytes(v)
	if err := enc.Uvarint(uint64(len(str))); err != nil {
		return err
	}
	if _, err := enc.Get().Write(str); err != nil {
		return err
	}
	return nil
}

func (enc *Encoder) StringNullable(v *string) error {
	isNil := v == nil
	if err := enc.Nullable(isNil); err != nil {
		return err
	}
	if isNil {
		return enc.String("")
	}
	return enc.String(*v)
}

func (enc *Encoder) RawString(str []byte) error {
	if err := enc.Uvarint(uint64(len(str))); err != nil {
		return err
	}
	if _, err := enc.Get().Write(str); err != nil {
		return err
	}
	return nil
}

func (enc *Encoder) RawStringNullable(str *[]byte) error {
	isNil := str == nil
	if err := enc.Nullable(isNil); err != nil {
		return err
	}
	if isNil {
		return enc.RawString([]byte{})
	}
	return enc.RawString(*str)
}

func (enc *Encoder) Decimal128(bytes []byte) error {
	_, err := enc.Get().Write(bytes)
	return err
}

func (enc *Encoder) Write(b []byte) (int, error) {
	return enc.Get().Write(b)
}

func (enc *Encoder) Flush() error {
	if w, ok := enc.Get().(WriteFlusher); ok {
		return w.Flush()
	}
	return nil
}

type WriteFlusher interface {
	Flush() error
}

func Str2Bytes(str string) []byte {
	// Copied from https://github.com/m3db/m3/blob/master/src/x/unsafe/string.go#L62
	if len(str) == 0 {
		return nil
	}

	// We need to declare a real byte slice so internally the compiler
	// knows to use an unsafe.Pointer to keep track of the underlying memory so that
	// once the slice's array pointer is updated with the pointer to the string's
	// underlying bytes, the compiler won't prematurely GC the memory when the string
	// goes out of scope.
	var b []byte
	byteHeader := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	// This makes sure that even if GC relocates the string's underlying
	// memory after this assignment, the corresponding unsafe.Pointer in the internal
	// slice struct will be updated accordingly to reflect the memory relocation.
	byteHeader.Data = (*reflect.StringHeader)(unsafe.Pointer(&str)).Data

	// It is important that we access str after we assign the Data
	// pointer of the string header to the Data pointer of the slice header to
	// make sure the string (and the underlying bytes backing the string) don't get
	// GC'ed before the assignment happens.
	l := len(str)
	byteHeader.Len = l
	byteHeader.Cap = l

	return b
}
