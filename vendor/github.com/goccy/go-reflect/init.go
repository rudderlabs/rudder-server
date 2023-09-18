package reflect

import (
	"fmt"
	"unsafe"
)

var (
	errTypeOf  = "runtime error: failed to get %s type by TypeOf"
	errValueOf = "runtime error: failed to get %s value by ValueOf"
)

// validateTypeOf validate interface{} layout and Kind.
func validateTypeOf() error {
	if TypeOf(true).Kind() != Bool {
		return fmt.Errorf(errTypeOf, "bool")
	}
	if TypeOf(int(1)).Kind() != Int {
		return fmt.Errorf(errTypeOf, "int")
	}
	if TypeOf(int8(1)).Kind() != Int8 {
		return fmt.Errorf(errTypeOf, "int8")
	}
	if TypeOf(int16(1)).Kind() != Int16 {
		return fmt.Errorf(errTypeOf, "int16")
	}
	if TypeOf(int32(1)).Kind() != Int32 {
		return fmt.Errorf(errTypeOf, "int32")
	}
	if TypeOf(int64(1)).Kind() != Int64 {
		return fmt.Errorf(errTypeOf, "int64")
	}
	if TypeOf(uint(1)).Kind() != Uint {
		return fmt.Errorf(errTypeOf, "uint")
	}
	if TypeOf(uint8(1)).Kind() != Uint8 {
		return fmt.Errorf(errTypeOf, "uint8")
	}
	if TypeOf(uint16(1)).Kind() != Uint16 {
		return fmt.Errorf(errTypeOf, "uint16")
	}
	if TypeOf(uint32(1)).Kind() != Uint32 {
		return fmt.Errorf(errTypeOf, "uint32")
	}
	if TypeOf(uint64(1)).Kind() != Uint64 {
		return fmt.Errorf(errTypeOf, "uint64")
	}
	if TypeOf(uintptr(1)).Kind() != Uintptr {
		return fmt.Errorf(errTypeOf, "uintptr")
	}
	if TypeOf(float32(1)).Kind() != Float32 {
		return fmt.Errorf(errTypeOf, "float32")
	}
	if TypeOf(float64(1)).Kind() != Float64 {
		return fmt.Errorf(errTypeOf, "float64")
	}
	if TypeOf([1]int{1}).Kind() != Array {
		return fmt.Errorf(errTypeOf, "array")
	}
	if TypeOf([]int{1}).Kind() != Slice {
		return fmt.Errorf(errTypeOf, "slice")
	}
	if TypeOf(func() {}).Kind() != Func {
		return fmt.Errorf(errTypeOf, "func")
	}
	if TypeOf(map[struct{}]struct{}{}).Kind() != Map {
		return fmt.Errorf(errTypeOf, "map")
	}
	if TypeOf("").Kind() != String {
		return fmt.Errorf(errTypeOf, "string")
	}
	if TypeOf(struct{}{}).Kind() != Struct {
		return fmt.Errorf(errTypeOf, "struct")
	}
	if TypeOf(&struct{}{}).Kind() != Ptr {
		return fmt.Errorf(errTypeOf, "pointer")
	}
	if TypeOf(unsafe.Pointer(nil)).Kind() != UnsafePointer {
		return fmt.Errorf(errTypeOf, "unsafepointer")
	}
	return nil
}

// validateValueOf validate value layout and flag values.
func validateValueOf() error {
	if v := ValueOf(true); v.Type().Kind() != Bool || v.Bool() != true {
		return fmt.Errorf(errValueOf, "bool")
	}
	if v := ValueOf(int(1)); v.Type().Kind() != Int || v.Int() != 1 {
		return fmt.Errorf(errValueOf, "int")
	}
	if v := ValueOf(int8(1)); v.Type().Kind() != Int8 || v.Int() != 1 {
		return fmt.Errorf(errValueOf, "int8")
	}
	if v := ValueOf(int16(1)); v.Type().Kind() != Int16 || v.Int() != 1 {
		return fmt.Errorf(errValueOf, "int16")
	}
	if v := ValueOf(int32(1)); v.Type().Kind() != Int32 || v.Int() != 1 {
		return fmt.Errorf(errValueOf, "int32")
	}
	if v := ValueOf(int64(1)); v.Type().Kind() != Int64 || v.Int() != 1 {
		return fmt.Errorf(errValueOf, "int64")
	}
	if v := ValueOf(uint(1)); v.Type().Kind() != Uint || v.Uint() != 1 {
		return fmt.Errorf(errValueOf, "uint")
	}
	if v := ValueOf(uint8(1)); v.Type().Kind() != Uint8 || v.Uint() != 1 {
		return fmt.Errorf(errValueOf, "uint8")
	}
	if v := ValueOf(uint16(1)); v.Type().Kind() != Uint16 || v.Uint() != 1 {
		return fmt.Errorf(errValueOf, "uint16")
	}
	if v := ValueOf(uint32(1)); v.Type().Kind() != Uint32 || v.Uint() != 1 {
		return fmt.Errorf(errValueOf, "uint32")
	}
	if v := ValueOf(uint64(1)); v.Type().Kind() != Uint64 || v.Uint() != 1 {
		return fmt.Errorf(errValueOf, "uint64")
	}
	if v := ValueOf(uintptr(1)); v.Type().Kind() != Uintptr || v.Uint() != 1 {
		return fmt.Errorf(errValueOf, "uintptr")
	}
	if v := ValueOf(float32(1)); v.Type().Kind() != Float32 || v.Float() != 1 {
		return fmt.Errorf(errValueOf, "float32")
	}
	if v := ValueOf(float64(1)); v.Type().Kind() != Float64 || v.Float() != 1 {
		return fmt.Errorf(errValueOf, "float64")
	}
	if v := ValueOf([1]int{1}); v.Type().Kind() != Array || v.Len() != 1 {
		return fmt.Errorf(errValueOf, "array")
	}
	if v := ValueOf([]int{1}); v.Type().Kind() != Slice || v.Len() != 1 {
		return fmt.Errorf(errValueOf, "slice")
	}
	if v := ValueOf(func() {}); v.Type().Kind() != Func {
		return fmt.Errorf(errValueOf, "func")
	}
	if v := ValueOf(map[struct{}]struct{}{}); v.Type().Kind() != Map {
		return fmt.Errorf(errValueOf, "map")
	}
	if v := ValueOf("1"); v.Type().Kind() != String || v.String() != "1" {
		return fmt.Errorf(errValueOf, "string")
	}
	if v := ValueOf(struct{}{}); v.Type().Kind() != Struct {
		return fmt.Errorf(errValueOf, "struct")
	}
	if v := ValueOf(&struct{}{}); v.Type().Kind() != Ptr {
		return fmt.Errorf(errValueOf, "pointer")
	}
	if v := ValueOf(unsafe.Pointer(nil)); v.Type().Kind() != UnsafePointer {
		return fmt.Errorf(errValueOf, "unsafepointer")
	}
	return nil
}

func validate() error {
	if err := validateTypeOf(); err != nil {
		return err
	}
	if err := validateValueOf(); err != nil {
		return err
	}
	return nil
}

func init() {
	if err := validate(); err != nil {
		panic(err)
	}
}
