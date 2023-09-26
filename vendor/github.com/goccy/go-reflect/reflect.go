package reflect

import (
	"reflect"
	"unsafe"
)

// Type is the representation of a Go type.
//
// Not all methods apply to all kinds of types. Restrictions,
// if any, are noted in the documentation for each method.
// Use the Kind method to find out the kind of type before
// calling kind-specific methods. Calling a method
// inappropriate to the kind of type causes a run-time panic.
//
// Type values are comparable, such as with the == operator,
// so they can be used as map keys.
// Two Type values are equal if they represent identical types.
type Type = *rtype

type rtype struct{}

type flag uintptr

const (
	flagKindWidth        = 5 // there are 27 kinds
	flagKindMask    flag = 1<<flagKindWidth - 1
	flagStickyRO    flag = 1 << 5
	flagEmbedRO     flag = 1 << 6
	flagIndir       flag = 1 << 7
	flagAddr        flag = 1 << 8
	flagMethod      flag = 1 << 9
	flagMethodShift      = 10
	flagRO          flag = flagStickyRO | flagEmbedRO
)

// A Kind represents the specific kind of type that a Type represents.
// The zero Kind is not a valid kind.
type Kind = reflect.Kind

const (
	Invalid Kind = iota
	Bool
	Int
	Int8
	Int16
	Int32
	Int64
	Uint
	Uint8
	Uint16
	Uint32
	Uint64
	Uintptr
	Float32
	Float64
	Complex64
	Complex128
	Array
	Chan
	Func
	Interface
	Map
	Ptr
	Slice
	String
	Struct
	UnsafePointer
)

const (
	_             SelectDir = iota
	SelectSend              // case Chan <- Send
	SelectRecv              // case <-Chan:
	SelectDefault           // default
)

// A StructTag is the tag string in a struct field.
//
// By convention, tag strings are a concatenation of
// optionally space-separated key:"value" pairs.
// Each key is a non-empty string consisting of non-control
// characters other than space (U+0020 ' '), quote (U+0022 '"'),
// and colon (U+003A ':').  Each value is quoted using U+0022 '"'
// characters and Go string literal syntax.
type StructTag = reflect.StructTag

// ChanDir represents a channel type's direction.
type ChanDir = reflect.ChanDir

const (
	RecvDir ChanDir             = 1 << iota // <-chan
	SendDir                                 // chan<-
	BothDir = RecvDir | SendDir             // chan
)

// A MapIter is an iterator for ranging over a map.
// See Value.MapRange.
type MapIter = reflect.MapIter

// A ValueError occurs when a Value method is invoked on
// a Value that does not support it. Such cases are documented
// in the description of each method.
type ValueError = reflect.ValueError

// SliceHeader is the runtime representation of a slice.
// It cannot be used safely or portably and its representation may
// change in a later release.
// Moreover, the Data field is not sufficient to guarantee the data
// it references will not be garbage collected, so programs must keep
// a separate, correctly typed pointer to the underlying data.
type SliceHeader = reflect.SliceHeader

// StringHeader is the runtime representation of a string.
// It cannot be used safely or portably and its representation may
// change in a later release.
// Moreover, the Data field is not sufficient to guarantee the data
// it references will not be garbage collected, so programs must keep
// a separate, correctly typed pointer to the underlying data.
type StringHeader = reflect.StringHeader

// A SelectCase describes a single case in a select operation.
// The kind of case depends on Dir, the communication direction.
//
// If Dir is SelectDefault, the case represents a default case.
// Chan and Send must be zero Values.
//
// If Dir is SelectSend, the case represents a send operation.
// Normally Chan's underlying value must be a channel, and Send's underlying value must be
// assignable to the channel's element type. As a special case, if Chan is a zero Value,
// then the case is ignored, and the field Send will also be ignored and may be either zero
// or non-zero.
//
// If Dir is SelectRecv, the case represents a receive operation.
// Normally Chan's underlying value must be a channel and Send must be a zero Value.
// If Chan is a zero Value, then the case is ignored, but Send must still be a zero Value.
// When a receive operation is selected, the received Value is returned by Select.
//
type SelectCase struct {
	Dir  SelectDir // direction of case
	Chan Value     // channel to use (for send or receive)
	Send Value     // value to send (for send)
}

type SelectDir = reflect.SelectDir

// Value is the reflection interface to a Go value.
// Not all methods apply to all kinds of values.
// Restrictions, if any, are noted in the documentation for each method.
// Use the Kind method to find out the kind of value before calling kind-specific methods.
// Calling a method inappropriate to the kind of type causes a run time panic.
// The zero Value represents no value.
// Its IsValid method returns false, its Kind method returns Invalid,
// its String method returns "<invalid Value>", and all other methods panic.
// Most functions and methods never return an invalid value.
// If one does, its documentation states the conditions explicitly.
// A Value can be used concurrently by multiple goroutines provided that
// the underlying Go value can be used concurrently for the equivalent direct operations.
// To compare two Values, compare the results of the Interface method.
// Using == on two Values does not compare the underlying values they represent.
type Value struct {
	typ Type
	ptr unsafe.Pointer
	flag
}

// Method represents a single method.
type Method struct {
	// Name is the method name.
	// PkgPath is the package path that qualifies a lower case (unexported)
	// method name. It is empty for upper case (exported) method names.
	// The combination of PkgPath and Name uniquely identifies a method
	// in a method set.
	// See https://golang.org/ref/spec#Uniqueness_of_identifiers
	Name    string
	PkgPath string

	Type  Type  // method type
	Func  Value // func with receiver as first argument
	Index int   // index for Type.Method
}

// A StructField describes a single field in a struct.
type StructField struct {
	// Name is the field name.
	Name string
	// PkgPath is the package path that qualifies a lower case (unexported)
	// field name. It is empty for upper case (exported) field names.
	// See https://golang.org/ref/spec#Uniqueness_of_identifiers
	PkgPath string

	Type      Type      // field type
	Tag       StructTag // field tag string
	Offset    uintptr   // offset within struct, in bytes
	Index     []int     // index sequence for Type.FieldByIndex
	Anonymous bool      // is an embedded field
}

// ArrayOf returns the array type with the given count and element type.
// For example, if t represents int, ArrayOf(5, t) represents [5]int.
//
// If the resulting type would be larger than the available address space,
// ArrayOf panics.
func ArrayOf(count int, elem Type) Type {
	return arrayOf(count, elem)
}

// ChanOf returns the channel type with the given direction and element type.
// For example, if t represents int, ChanOf(RecvDir, t) represents <-chan int.
//
// The gc runtime imposes a limit of 64 kB on channel element types.
// If t's size is equal to or exceeds this limit, ChanOf panics.
func ChanOf(dir ChanDir, t Type) Type {
	return chanOf(dir, t)
}

// FuncOf returns the function type with the given argument and result types.
// For example if k represents int and e represents string,
// FuncOf([]Type{k}, []Type{e}, false) represents func(int) string.
//
// The variadic argument controls whether the function is variadic. FuncOf
// panics if the in[len(in)-1] does not represent a slice and variadic is
// true.
func FuncOf(in, out []Type, variadic bool) Type {
	return funcOf(in, out, variadic)
}

// MapOf returns the map type with the given key and element types.
// For example, if k represents int and e represents string,
// MapOf(k, e) represents map[int]string.
//
// If the key type is not a valid map key type (that is, if it does
// not implement Go's == operator), MapOf panics.
func MapOf(key, elem Type) Type {
	return mapOf(key, elem)
}

// PtrTo returns the pointer type with element t.
// For example, if t represents type Foo, PtrTo(t) represents *Foo.
func PtrTo(t Type) Type {
	return ptrTo(t)
}

// SliceOf returns the slice type with element type t.
// For example, if t represents int, SliceOf(t) represents []int.
func SliceOf(t Type) Type {
	return sliceOf(t)
}

// StructOf returns the struct type containing fields.
// The Offset and Index fields are ignored and computed as they would be
// by the compiler.
//
// StructOf currently does not generate wrapper methods for embedded
// fields and panics if passed unexported StructFields.
// These limitations may be lifted in a future version.
func StructOf(fields []StructField) Type {
	return structOf(fields)
}

// TypeOf returns the reflection Type that represents the dynamic type of i.
// If i is a nil interface value, TypeOf returns nil.
func TypeOf(v interface{}) Type {
	value := (*Value)(unsafe.Pointer(&v))
	return value.typ
}

// TypeID returns unique type identifier of v.
func TypeID(v interface{}) uintptr {
	return uintptr(unsafe.Pointer(TypeOf(v)))
}

func valueOf(v interface{}) Value {
	if v == nil {
		return Value{}
	}
	valueLayout := (*Value)(unsafe.Pointer(&v))
	value := Value{}
	value.typ = valueLayout.typ
	value.ptr = valueLayout.ptr
	f := flag(value.typ.Kind())
	if ifaceIndir(value.typ) {
		f |= flagIndir
	}
	value.flag = f
	return value
}

// TypeAndPtrOf returns raw Type and ptr value in favor of performance.
func TypeAndPtrOf(v interface{}) (Type, unsafe.Pointer) {
	value := (*Value)(unsafe.Pointer(&v))
	return value.typ, value.ptr
}

// ValueOf returns a new Value initialized to the concrete value
// stored in the interface i. ValueOf(nil) returns the zero Value.
func ValueOf(v interface{}) Value {
	escape(v)
	return valueOf(v)
}

// ValueNoEscapeOf no escape of ValueOf.
func ValueNoEscapeOf(v interface{}) Value {
	return valueOf(v)
}

// ToReflectType convert Type to reflect.Type
func ToReflectType(t Type) reflect.Type {
	return toRT(t)
}

// ToReflectValue convert Value to reflect.Value
func ToReflectValue(v Value) reflect.Value {
	return toRV(v)
}

// ToType convert reflect.Type to Type
func ToType(t reflect.Type) Type {
	return toT(t)
}

// ToValue convert reflect.Value to Value
func ToValue(v reflect.Value) Value {
	return toV(v)
}

// Copy copies the contents of src into dst until either
// dst has been filled or src has been exhausted.
// It returns the number of elements copied.
// Dst and src each must have kind Slice or Array, and
// dst and src must have the same element type.
//
// As a special case, src can have kind String if the element type of dst is kind Uint8.
func Copy(dst, src Value) int {
	return value_Copy(dst, src)
}

// DeepEqual reports whether x and y are ``deeply equal,'' defined as follows.
// Two values of identical type are deeply equal if one of the following cases applies.
// Values of distinct types are never deeply equal.
//
// Array values are deeply equal when their corresponding elements are deeply equal.
//
// Struct values are deeply equal if their corresponding fields,
// both exported and unexported, are deeply equal.
//
// Func values are deeply equal if both are nil; otherwise they are not deeply equal.
//
// Interface values are deeply equal if they hold deeply equal concrete values.
//
// Map values are deeply equal when all of the following are true:
// they are both nil or both non-nil, they have the same length,
// and either they are the same map object or their corresponding keys
// (matched using Go equality) map to deeply equal values.
//
// Pointer values are deeply equal if they are equal using Go's == operator
// or if they point to deeply equal values.
//
// Slice values are deeply equal when all of the following are true:
// they are both nil or both non-nil, they have the same length,
// and either they point to the same initial entry of the same underlying array
// (that is, &x[0] == &y[0]) or their corresponding elements (up to length) are deeply equal.
// Note that a non-nil empty slice and a nil slice (for example, []byte{} and []byte(nil))
// are not deeply equal.
//
// Other values - numbers, bools, strings, and channels - are deeply equal
// if they are equal using Go's == operator.
//
// In general DeepEqual is a recursive relaxation of Go's == operator.
// However, this idea is impossible to implement without some inconsistency.
// Specifically, it is possible for a value to be unequal to itself,
// either because it is of func type (uncomparable in general)
// or because it is a floating-point NaN value (not equal to itself in floating-point comparison),
// or because it is an array, struct, or interface containing
// such a value.
// On the other hand, pointer values are always equal to themselves,
// even if they point at or contain such problematic values,
// because they compare equal using Go's == operator, and that
// is a sufficient condition to be deeply equal, regardless of content.
// DeepEqual has been defined so that the same short-cut applies
// to slices and maps: if x and y are the same slice or the same map,
// they are deeply equal regardless of content.
//
// As DeepEqual traverses the data values it may find a cycle. The
// second and subsequent times that DeepEqual compares two pointer
// values that have been compared before, it treats the values as
// equal rather than examining the values to which they point.
// This ensures that DeepEqual terminates.
func DeepEqual(x, y interface{}) bool {
	return reflect.DeepEqual(x, y)
}

func Swapper(slice interface{}) func(i, j int) {
	return reflect.Swapper(slice)
}

// Append appends the values x to a slice s and returns the resulting slice.
// As in Go, each x's value must be assignable to the slice's element type.
func Append(s Value, x ...Value) Value {
	return value_Append(s, x...)
}

// AppendSlice appends a slice t to a slice s and returns the resulting slice.
// The slices s and t must have the same element type.
func AppendSlice(s, t Value) Value {
	return value_AppendSlice(s, t)
}

// Indirect returns the value that v points to.
// If v is a nil pointer, Indirect returns a zero Value.
// If v is not a pointer, Indirect returns v.
func Indirect(v Value) Value {
	return value_Indirect(v)
}

// MakeChan creates a new channel with the specified type and buffer size.
func MakeChan(typ Type, buffer int) Value {
	return value_MakeChan(typ, buffer)
}

// MakeFunc returns a new function of the given Type
// that wraps the function fn. When called, that new function
// does the following:
//
//	- converts its arguments to a slice of Values.
//	- runs results := fn(args).
//	- returns the results as a slice of Values, one per formal result.
//
// The implementation fn can assume that the argument Value slice
// has the number and type of arguments given by typ.
// If typ describes a variadic function, the final Value is itself
// a slice representing the variadic arguments, as in the
// body of a variadic function. The result Value slice returned by fn
// must have the number and type of results given by typ.
//
// The Value.Call method allows the caller to invoke a typed function
// in terms of Values; in contrast, MakeFunc allows the caller to implement
// a typed function in terms of Values.
//
// The Examples section of the documentation includes an illustration
// of how to use MakeFunc to build a swap function for different types.
//
func MakeFunc(typ Type, fn func(args []Value) (results []Value)) Value {
	return value_MakeFunc(typ, fn)
}

// MakeMap creates a new map with the specified type.
func MakeMap(typ Type) Value {
	return value_MakeMap(typ)
}

// MakeMapWithSize creates a new map with the specified type
// and initial space for approximately n elements.
func MakeMapWithSize(typ Type, n int) Value {
	return value_MakeMapWithSize(typ, n)
}

// MakeSlice creates a new zero-initialized slice value
// for the specified slice type, length, and capacity.
func MakeSlice(typ Type, len, cap int) Value {
	return value_MakeSlice(typ, len, cap)
}

// New returns a Value representing a pointer to a new zero value
// for the specified type. That is, the returned Value's Type is PtrTo(typ).
func New(typ Type) Value {
	return value_New(typ)
}

// NewAt returns a Value representing a pointer to a value of the
// specified type, using p as that pointer.
func NewAt(typ Type, p unsafe.Pointer) Value {
	return value_NewAt(typ, p)
}

// Select executes a select operation described by the list of cases.
// Like the Go select statement, it blocks until at least one of the cases
// can proceed, makes a uniform pseudo-random choice,
// and then executes that case. It returns the index of the chosen case
// and, if that case was a receive operation, the value received and a
// boolean indicating whether the value corresponds to a send on the channel
// (as opposed to a zero value received because the channel is closed).
func Select(cases []SelectCase) (int, Value, bool) {
	return value_Select(cases)
}

// Zero returns a Value representing the zero value for the specified type.
// The result is different from the zero value of the Value struct,
// which represents no value at all.
// For example, Zero(TypeOf(42)) returns a Value with Kind Int and value 0.
// The returned value is neither addressable nor settable.
func Zero(typ Type) Value {
	return value_Zero(typ)
}

// Align returns the alignment in bytes of a value of
// this type when allocated in memory.
func (t *rtype) Align() int {
	return type_Align(t)
}

// FieldAlign returns the alignment in bytes of a value of
// this type when used as a field in a struct.
func (t *rtype) FieldAlign() int {
	return type_FieldAlign(t)
}

// Method returns the i'th method in the type's method set.
// It panics if i is not in the range [0, NumMethod()).
//
// For a non-interface type T or *T, the returned Method's Type and Func
// fields describe a function whose first argument is the receiver.
//
// For an interface type, the returned Method's Type field gives the
// method signature, without a receiver, and the Func field is nil.
//
// Only exported methods are accessible and they are sorted in
// lexicographic order.
func (t *rtype) Method(a0 int) Method {
	return toM(type_Method(t, a0))
}

// MethodByName returns the method with that name in the type's
// method set and a boolean indicating if the method was found.
//
// For a non-interface type T or *T, the returned Method's Type and Func
// fields describe a function whose first argument is the receiver.
//
// For an interface type, the returned Method's Type field gives the
// method signature, without a receiver, and the Func field is nil.
func (t *rtype) MethodByName(a0 string) (Method, bool) {
	mtd, ok := type_MethodByName(t, a0)
	return toM(mtd), ok
}

// NumMethod returns the number of exported methods in the type's method set.
func (t *rtype) NumMethod() int {
	return type_NumMethod(t)
}

// Name returns the type's name within its package for a defined type.
// For other (non-defined) types it returns the empty string.
func (t *rtype) Name() string {
	return type_Name(t)
}

// PkgPath returns a defined type's package path, that is, the import path
// that uniquely identifies the package, such as "encoding/base64".
// If the type was predeclared (string, error) or not defined (*T, struct{},
// []int, or A where A is an alias for a non-defined type), the package path
// will be the empty string.
func (t *rtype) PkgPath() string {
	return type_PkgPath(t)
}

// Size returns the number of bytes needed to store
// a value of the given type; it is analogous to unsafe.Sizeof.
func (t *rtype) Size() uintptr {
	return type_Size(t)
}

// String returns a string representation of the type.
// The string representation may use shortened package names
// (e.g., base64 instead of "encoding/base64") and is not
// guaranteed to be unique among types. To test for type identity,
// compare the Types directly.
func (t *rtype) String() string {
	return type_String(t)
}

// Kind returns the specific kind of this type.
func (t *rtype) Kind() Kind {
	return type_Kind(t)
}

// Implements reports whether the type implements the interface type u.
func (t *rtype) Implements(u Type) bool {
	return type_Implements(t, toRT(u))
}

// AssignableTo reports whether a value of the type is assignable to type u.
func (t *rtype) AssignableTo(u Type) bool {
	return type_AssignableTo(t, toRT(u))
}

// ConvertibleTo reports whether a value of the type is convertible to type u.
func (t *rtype) ConvertibleTo(u Type) bool {
	return type_ConvertibleTo(t, toRT(u))
}

// Comparable reports whether values of this type are comparable.
func (t *rtype) Comparable() bool {
	return type_Comparable(t)
}

// Methods applicable only to some types, depending on Kind.
// The methods allowed for each kind are:
//
//	Int*, Uint*, Float*, Complex*: Bits
//	Array: Elem, Len
//	Chan: ChanDir, Elem
//	Func: In, NumIn, Out, NumOut, IsVariadic.
//	Map: Key, Elem
//	Ptr: Elem
//	Slice: Elem
//	Struct: Field, FieldByIndex, FieldByName, FieldByNameFunc, NumField

// Bits returns the size of the type in bits.
// It panics if the type's Kind is not one of the
// sized or unsized Int, Uint, Float, or Complex kinds.
func (t *rtype) Bits() int {
	return type_Bits(t)
}

// ChanDir returns a channel type's direction.
// It panics if the type's Kind is not Chan.
func (t *rtype) ChanDir() ChanDir {
	return type_ChanDir(t)
}

// IsVariadic reports whether a function type's final input parameter
// is a "..." parameter. If so, t.In(t.NumIn() - 1) returns the parameter's
// implicit actual type []T.
//
// For concreteness, if t represents func(x int, y ... float64), then
//
//	t.NumIn() == 2
//	t.In(0) is the reflect.Type for "int"
//	t.In(1) is the reflect.Type for "[]float64"
//	t.IsVariadic() == true
//
// IsVariadic panics if the type's Kind is not Func.
func (t *rtype) IsVariadic() bool {
	return type_IsVariadic(t)
}

// Elem returns a type's element type.
// It panics if the type's Kind is not Array, Chan, Map, Ptr, or Slice.
func (t *rtype) Elem() Type {
	return ToType(type_Elem(t))
}

// Field returns a struct type's i'th field.
// It panics if the type's Kind is not Struct.
// It panics if i is not in the range [0, NumField()).
func (t *rtype) Field(i int) StructField {
	return toSF(type_Field(t, i))
}

// FieldByIndex returns the nested field corresponding
// to the index sequence. It is equivalent to calling Field
// successively for each index i.
// It panics if the type's Kind is not Struct.
func (t *rtype) FieldByIndex(index []int) StructField {
	return toSF(type_FieldByIndex(t, index))
}

// FieldByName returns the struct field with the given name
// and a boolean indicating if the field was found.
func (t *rtype) FieldByName(name string) (StructField, bool) {
	field, ok := type_FieldByName(t, name)
	return toSF(field), ok
}

// FieldByNameFunc returns the struct field with a name
// that satisfies the match function and a boolean indicating if
// the field was found.
//
// FieldByNameFunc considers the fields in the struct itself
// and then the fields in any embedded structs, in breadth first order,
// stopping at the shallowest nesting depth containing one or more
// fields satisfying the match function. If multiple fields at that depth
// satisfy the match function, they cancel each other
// and FieldByNameFunc returns no match.
// This behavior mirrors Go's handling of name lookup in
// structs containing embedded fields.
func (t *rtype) FieldByNameFunc(match func(string) bool) (StructField, bool) {
	field, ok := type_FieldByNameFunc(t, match)
	return toSF(field), ok
}

// In returns the type of a function type's i'th input parameter.
// It panics if the type's Kind is not Func.
// It panics if i is not in the range [0, NumIn()).
func (t *rtype) In(i int) Type {
	return ToType(type_In(t, i))
}

// Key returns a map type's key type.
// It panics if the type's Kind is not Map.
func (t *rtype) Key() Type {
	return ToType(type_Key(t))
}

// Len returns an array type's length.
// It panics if the type's Kind is not Array.
func (t *rtype) Len() int {
	return type_Len(t)
}

// NumField returns a struct type's field count.
// It panics if the type's Kind is not Struct.
func (t *rtype) NumField() int {
	return type_NumField(t)
}

// NumIn returns a function type's input parameter count.
// It panics if the type's Kind is not Func.
func (t *rtype) NumIn() int {
	return type_NumIn(t)
}

// NumOut returns a function type's output parameter count.
// It panics if the type's Kind is not Func.
func (t *rtype) NumOut() int {
	return type_NumOut(t)
}

// Out returns the type of a function type's i'th output parameter.
// It panics if the type's Kind is not Func.
// It panics if i is not in the range [0, NumOut()).
func (t *rtype) Out(i int) Type {
	return toT(type_Out(t, i))
}

// Addr returns a pointer value representing the address of v.
// It panics if CanAddr() returns false.
// Addr is typically used to obtain a pointer to a struct field
// or slice element in order to call a method that requires a
// pointer receiver.
func (v Value) Addr() Value {
	return value_Addr(v)
}

// Bool returns v's underlying value.
// It panics if v's kind is not Bool.
func (v Value) Bool() bool {
	return value_Bool(v)
}

// Bytes returns v's underlying value.
// It panics if v's underlying value is not a slice of bytes.
func (v Value) Bytes() []byte {
	return value_Bytes(v)
}

// Call calls the function v with the input arguments in.
// For example, if len(in) == 3, v.Call(in) represents the Go call v(in[0], in[1], in[2]).
// Call panics if v's Kind is not Func.
// It returns the output results as Values.
// As in Go, each input argument must be assignable to the
// type of the function's corresponding input parameter.
// If v is a variadic function, Call creates the variadic slice parameter
// itself, copying in the corresponding values.
func (v Value) Call(in []Value) []Value {
	return value_Call(v, in)
}

// CallSlice calls the variadic function v with the input arguments in,
// assigning the slice in[len(in)-1] to v's final variadic argument.
// For example, if len(in) == 3, v.CallSlice(in) represents the Go call v(in[0], in[1], in[2]...).
// CallSlice panics if v's Kind is not Func or if v is not variadic.
// It returns the output results as Values.
// As in Go, each input argument must be assignable to the
// type of the function's corresponding input parameter.
func (v Value) CallSlice(in []Value) []Value {
	return value_CallSlice(v, in)
}

// CanAddr reports whether the value's address can be obtained with Addr.
// Such values are called addressable. A value is addressable if it is
// an element of a slice, an element of an addressable array,
// a field of an addressable struct, or the result of dereferencing a pointer.
// If CanAddr returns false, calling Addr will panic.
func (v Value) CanAddr() bool {
	return value_CanAddr(v)
}

// CanInterface reports whether Interface can be used without panicking.
func (v Value) CanInterface() bool {
	return value_CanInterface(v)
}

// CanSet reports whether the value of v can be changed.
// A Value can be changed only if it is addressable and was not
// obtained by the use of unexported struct fields.
// If CanSet returns false, calling Set or any type-specific
// setter (e.g., SetBool, SetInt) will panic.
func (v Value) CanSet() bool {
	return value_CanSet(v)
}

// Cap returns v's capacity.
// It panics if v's Kind is not Array, Chan, or Slice.
func (v Value) Cap() int {
	return value_Cap(v)
}

// Close closes the channel v.
// It panics if v's Kind is not Chan.
func (v Value) Close() {
	value_Close(v)
}

// Complex returns v's underlying value, as a complex128.
// It panics if v's Kind is not Complex64 or Complex128.
func (v Value) Complex() complex128 {
	return value_Complex(v)
}

// Convert returns the value v converted to type t.
// If the usual Go conversion rules do not allow conversion
// of the value v to type t, Convert panics.
func (v Value) Convert(t Type) Value {
	return value_Convert(v, t)
}

// Elem returns the value that the interface v contains
// or that the pointer v points to.
// It panics if v's Kind is not Interface or Ptr.
// It returns the zero Value if v is nil.
func (v Value) Elem() Value {
	return value_Elem(v)
}

// Field returns the i'th field of the struct v.
// It panics if v's Kind is not Struct or i is out of range.
func (v Value) Field(i int) Value {
	return value_Field(v, i)
}

// FieldByIndex returns the nested field corresponding to index.
// It panics if v's Kind is not struct.
func (v Value) FieldByIndex(index []int) Value {
	return value_FieldByIndex(v, index)
}

// FieldByName returns the struct field with the given name.
// It returns the zero Value if no field was found.
// It panics if v's Kind is not struct.
func (v Value) FieldByName(name string) Value {
	return value_FieldByName(v, name)
}

// FieldByNameFunc returns the struct field with a name
// that satisfies the match function.
// It panics if v's Kind is not struct.
// It returns the zero Value if no field was found.
func (v Value) FieldByNameFunc(match func(string) bool) Value {
	return value_FieldByNameFunc(v, match)
}

// Float returns v's underlying value, as a float64.
// It panics if v's Kind is not Float32 or Float64.
func (v Value) Float() float64 {
	return value_Float(v)
}

// Index returns v's i'th element.
// It panics if v's Kind is not Array, Slice, or String or i is out of range.
func (v Value) Index(i int) Value {
	return value_Index(v, i)
}

// Int returns v's underlying value, as an int64.
// It panics if v's Kind is not Int, Int8, Int16, Int32, or Int64.
func (v Value) Int() int64 {
	return value_Int(v)
}

// Interface returns v's current value as an interface{}.
// It is equivalent to:
//	var i interface{} = (v's underlying value)
// It panics if the Value was obtained by accessing
// unexported struct fields.
func (v Value) Interface() interface{} {
	return value_Interface(v)
}

// InterfaceData returns the interface v's value as a uintptr pair.
// It panics if v's Kind is not Interface.
func (v Value) InterfaceData() [2]uintptr {
	return value_InterfaceData(v)
}

// IsNil reports whether its argument v is nil. The argument must be
// a chan, func, interface, map, pointer, or slice value; if it is
// not, IsNil panics. Note that IsNil is not always equivalent to a
// regular comparison with nil in Go. For example, if v was created
// by calling ValueOf with an uninitialized interface variable i,
// i==nil will be true but v.IsNil will panic as v will be the zero
// Value.
func (v Value) IsNil() bool {
	return value_IsNil(v)
}

// IsValid reports whether v represents a value.
// It returns false if v is the zero Value.
// If IsValid returns false, all other methods except String panic.
// Most functions and methods never return an invalid Value.
// If one does, its documentation states the conditions explicitly.
func (v Value) IsValid() bool {
	return value_IsValid(v)
}

// Kind returns v's Kind.
// If v is the zero Value (IsValid returns false), Kind returns Invalid.
func (v Value) Kind() Kind {
	return value_Kind(v)
}

// Len returns v's length.
// It panics if v's Kind is not Array, Chan, Map, Slice, or String.
func (v Value) Len() int {
	return value_Len(v)
}

// MapIndex returns the value associated with key in the map v.
// It panics if v's Kind is not Map.
// It returns the zero Value if key is not found in the map or if v represents a nil map.
// As in Go, the key's value must be assignable to the map's key type.
func (v Value) MapIndex(key Value) Value {
	return value_MapIndex(v, key)
}

// MapKeys returns a slice containing all the keys present in the map,
// in unspecified order.
// It panics if v's Kind is not Map.
// It returns an empty slice if v represents a nil map.
func (v Value) MapKeys() []Value {
	return value_MapKeys(v)
}

// MapRange returns a range iterator for a map.
// It panics if v's Kind is not Map.
//
// Call Next to advance the iterator, and Key/Value to access each entry.
// Next returns false when the iterator is exhausted.
// MapRange follows the same iteration semantics as a range statement.
//
// Example:
//
//	iter := reflect.ValueOf(m).MapRange()
// 	for iter.Next() {
//		k := iter.Key()
//		v := iter.Value()
//		...
//	}
//
func (v Value) MapRange() *MapIter {
	return value_MapRange(v)
}

// Method returns a function value corresponding to v's i'th method.
// The arguments to a Call on the returned function should not include
// a receiver; the returned function will always use v as the receiver.
// Method panics if i is out of range or if v is a nil interface value.
func (v Value) Method(i int) Value {
	return value_Method(v, i)
}

// MethodByName returns a function value corresponding to the method
// of v with the given name.
// The arguments to a Call on the returned function should not include
// a receiver; the returned function will always use v as the receiver.
// It returns the zero Value if no method was found.
func (v Value) MethodByName(name string) Value {
	return value_MethodByName(v, name)
}

// NumField returns the number of fields in the struct v.
// It panics if v's Kind is not Struct.
func (v Value) NumField() int {
	return value_NumField(v)
}

// NumMethod returns the number of exported methods in the value's method set.
func (v Value) NumMethod() int {
	return value_NumMethod(v)
}

// OverflowComplex reports whether the complex128 x cannot be represented by v's type.
// It panics if v's Kind is not Complex64 or Complex128.
func (v Value) OverflowComplex(x complex128) bool {
	return value_OverflowComplex(v, x)
}

// OverflowFloat reports whether the float64 x cannot be represented by v's type.
// It panics if v's Kind is not Float32 or Float64.
func (v Value) OverflowFloat(x float64) bool {
	return value_OverflowFloat(v, x)
}

// OverflowInt reports whether the int64 x cannot be represented by v's type.
// It panics if v's Kind is not Int, Int8, Int16, Int32, or Int64.
func (v Value) OverflowInt(x int64) bool {
	return value_OverflowInt(v, x)
}

// OverflowUint reports whether the uint64 x cannot be represented by v's type.
// It panics if v's Kind is not Uint, Uintptr, Uint8, Uint16, Uint32, or Uint64.
func (v Value) OverflowUint(x uint64) bool {
	return value_OverflowUint(v, x)
}

//go:nocheckptr
// This prevents inlining Value.Pointer when -d=checkptr is enabled,
// which ensures cmd/compile can recognize unsafe.Pointer(v.Pointer())
// and make an exception.

// Pointer returns v's value as a uintptr.
// It returns uintptr instead of unsafe.Pointer so that
// code using reflect cannot obtain unsafe.Pointers
// without importing the unsafe package explicitly.
// It panics if v's Kind is not Chan, Func, Map, Ptr, Slice, or UnsafePointer.
//
// If v's Kind is Func, the returned pointer is an underlying
// code pointer, but not necessarily enough to identify a
// single function uniquely. The only guarantee is that the
// result is zero if and only if v is a nil func Value.
//
// If v's Kind is Slice, the returned pointer is to the first
// element of the slice. If the slice is nil the returned value
// is 0.  If the slice is empty but non-nil the return value is non-zero.
func (v Value) Pointer() uintptr {
	return value_Pointer(v)
}

// Recv receives and returns a value from the channel v.
// It panics if v's Kind is not Chan.
// The receive blocks until a value is ready.
// The boolean value ok is true if the value x corresponds to a send
// on the channel, false if it is a zero value received because the channel is closed.
func (v Value) Recv() (Value, bool) {
	return value_Recv(v)
}

// Send sends x on the channel v.
// It panics if v's kind is not Chan or if x's type is not the same type as v's element type.
// As in Go, x's value must be assignable to the channel's element type.
func (v Value) Send(x Value) {
	value_Send(v, x)
}

// Set assigns x to the value v.
// It panics if CanSet returns false.
// As in Go, x's value must be assignable to v's type.
func (v Value) Set(x Value) {
	value_Set(v, x)
}

// SetBool sets v's underlying value.
// It panics if v's Kind is not Bool or if CanSet() is false.
func (v Value) SetBool(x bool) {
	value_SetBool(v, x)
}

// SetBytes sets v's underlying value.
// It panics if v's underlying value is not a slice of bytes.
func (v Value) SetBytes(x []byte) {
	value_SetBytes(v, x)
}

// SetCap sets v's capacity to n.
// It panics if v's Kind is not Slice or if n is smaller than the length or
// greater than the capacity of the slice.
func (v Value) SetCap(n int) {
	value_SetCap(v, n)
}

// SetComplex sets v's underlying value to x.
// It panics if v's Kind is not Complex64 or Complex128, or if CanSet() is false.
func (v Value) SetComplex(x complex128) {
	value_SetComplex(v, x)
}

// SetFloat sets v's underlying value to x.
// It panics if v's Kind is not Float32 or Float64, or if CanSet() is false.
func (v Value) SetFloat(x float64) {
	value_SetFloat(v, x)
}

// SetInt sets v's underlying value to x.
// It panics if v's Kind is not Int, Int8, Int16, Int32, or Int64, or if CanSet() is false.
func (v Value) SetInt(x int64) {
	value_SetInt(v, x)
}

// SetLen sets v's length to n.
// It panics if v's Kind is not Slice or if n is negative or
// greater than the capacity of the slice.
func (v Value) SetLen(n int) {
	value_SetLen(v, n)
}

// SetMapIndex sets the element associated with key in the map v to elem.
// It panics if v's Kind is not Map.
// If elem is the zero Value, SetMapIndex deletes the key from the map.
// Otherwise if v holds a nil map, SetMapIndex will panic.
// As in Go, key's elem must be assignable to the map's key type,
// and elem's value must be assignable to the map's elem type.
func (v Value) SetMapIndex(key, elem Value) {
	value_SetMapIndex(v, key, elem)
}

// SetPointer sets the unsafe.Pointer value v to x.
// It panics if v's Kind is not UnsafePointer.
func (v Value) SetPointer(x unsafe.Pointer) {
	value_SetPointer(v, x)
}

// SetString sets v's underlying value to x.
// It panics if v's Kind is not String or if CanSet() is false.
func (v Value) SetString(x string) {
	value_SetString(v, x)
}

// SetUint sets v's underlying value to x.
// It panics if v's Kind is not Uint, Uintptr, Uint8, Uint16, Uint32, or Uint64, or if CanSet() is false.
func (v Value) SetUint(x uint64) {
	value_SetUint(v, x)
}

// Slice returns v[i:j].
// It panics if v's Kind is not Array, Slice or String, or if v is an unaddressable array,
// or if the indexes are out of bounds.
func (v Value) Slice(i, j int) Value {
	return value_Slice(v, i, j)
}

// Slice3 is the 3-index form of the slice operation: it returns v[i:j:k].
// It panics if v's Kind is not Array or Slice, or if v is an unaddressable array,
// or if the indexes are out of bounds.
func (v Value) Slice3(i, j, k int) Value {
	return value_Slice3(v, i, j, k)
}

// String returns the string v's underlying value, as a string.
// String is a special case because of Go's String method convention.
// Unlike the other getters, it does not panic if v's Kind is not String.
// Instead, it returns a string of the form "<T value>" where T is v's type.
// The fmt package treats Values specially. It does not call their String
// method implicitly but instead prints the concrete values they hold.
func (v Value) String() string {
	return value_String(v)
}

// TryRecv attempts to receive a value from the channel v but will not block.
// It panics if v's Kind is not Chan.
// If the receive delivers a value, x is the transferred value and ok is true.
// If the receive cannot finish without blocking, x is the zero Value and ok is false.
// If the channel is closed, x is the zero value for the channel's element type and ok is false.
func (v Value) TryRecv() (Value, bool) {
	return value_TryRecv(v)
}

// TrySend attempts to send x on the channel v but will not block.
// It panics if v's Kind is not Chan.
// It reports whether the value was sent.
// As in Go, x's value must be assignable to the channel's element type.
func (v Value) TrySend(x Value) bool {
	return value_TrySend(v, x)
}

// Type returns v's type.
func (v Value) Type() Type {
	return value_Type(v)
}

// Uint returns v's underlying value, as a uint64.
// It panics if v's Kind is not Uint, Uintptr, Uint8, Uint16, Uint32, or Uint64.
func (v Value) Uint() uint64 {
	return value_Uint(v)
}

//go:nocheckptr
// This prevents inlining Value.UnsafeAddr when -d=checkptr is enabled,
// which ensures cmd/compile can recognize unsafe.Pointer(v.UnsafeAddr())
// and make an exception.

// UnsafeAddr returns a pointer to v's data.
// It is for advanced clients that also import the "unsafe" package.
// It panics if v is not addressable.
func (v Value) UnsafeAddr() uintptr {
	return value_UnsafeAddr(v)
}
