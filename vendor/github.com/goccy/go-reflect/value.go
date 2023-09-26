package reflect

import (
	"reflect"
	"unsafe"
)

func value_Copy(dst Value, src Value) int {
	return reflect.Copy(toRV(dst), toRV(src))
}

func value_Append(v Value, args ...Value) Value {
	return toV(reflect.Append(toRV(v), toRVs(args)...))
}

func value_AppendSlice(s, t Value) Value {
	return toV(reflect.AppendSlice(toRV(s), toRV(t)))
}

func value_Indirect(v Value) Value {
	return toV(reflect.Indirect(toRV(v)))
}

func value_MakeChan(typ Type, buffer int) Value {
	return toV(reflect.MakeChan(toRT(typ), buffer))
}

func value_MakeFunc(typ Type, fn func([]Value) []Value) Value {
	return toV(reflect.MakeFunc(toRT(typ), func(args []reflect.Value) []reflect.Value {
		return toRVs(fn(toVs(args)))
	}))
}

func value_MakeMap(typ Type) Value {
	return toV(reflect.MakeMap(toRT(typ)))
}

func value_MakeMapWithSize(typ Type, n int) Value {
	return toV(reflect.MakeMapWithSize(toRT(typ), n))
}

func value_MakeSlice(typ Type, len, cap int) Value {
	return toV(reflect.MakeSlice(toRT(typ), len, cap))
}

func value_New(typ Type) Value {
	return toV(reflect.New(toRT(typ)))
}

func value_NewAt(typ Type, p unsafe.Pointer) Value {
	return toV(reflect.NewAt(toRT(typ), p))
}

func value_Select(cases []SelectCase) (int, Value, bool) {
	chosen, recv, recvOK := reflect.Select(toRSCs(cases))
	return chosen, toV(recv), recvOK
}

func value_Zero(typ Type) Value {
	return toV(reflect.Zero(toRT(typ)))
}

func value_Addr(v Value) Value {
	return toV(toRV(v).Addr())
}

func value_Bool(v Value) bool {
	return toRV(v).Bool()
}

func value_Bytes(v Value) []byte {
	return toRV(v).Bytes()
}

func value_Call(v Value, in []Value) []Value {
	return toVs(toRV(v).Call(toRVs(in)))
}

func value_CallSlice(v Value, in []Value) []Value {
	return toVs(toRV(v).CallSlice(toRVs(in)))
}

func value_CanAddr(v Value) bool {
	return toRV(v).CanAddr()
}

func value_CanSet(v Value) bool {
	return toRV(v).CanSet()
}

func value_CanInterface(v Value) bool {
	return toRV(v).CanInterface()
}

func value_Cap(v Value) int {
	return toRV(v).Cap()
}

func value_Close(v Value) {
	toRV(v).Close()
}

func value_Complex(v Value) complex128 {
	return toRV(v).Complex()
}

func value_Convert(v Value, typ Type) Value {
	return toV(toRV(v).Convert(toRT(typ)))
}

func value_Elem(v Value) Value {
	return toV(toRV(v).Elem())
}

func value_Field(v Value, i int) Value {
	return toV(toRV(v).Field(i))
}

func value_FieldByIndex(v Value, i []int) Value {
	return toV(toRV(v).FieldByIndex(i))
}

func value_FieldByName(v Value, name string) Value {
	return toV(toRV(v).FieldByName(name))
}

func value_FieldByNameFunc(v Value, fn func(string) bool) Value {
	return toV(toRV(v).FieldByNameFunc(fn))
}

func value_Float(v Value) float64 {
	return toRV(v).Float()
}

func value_Index(v Value, i int) Value {
	return toV(toRV(v).Index(i))
}

func value_Int(v Value) int64 {
	return toRV(v).Int()
}

func value_Interface(v Value) interface{} {
	return toRV(v).Interface()
}

func value_InterfaceData(v Value) [2]uintptr {
	return toRV(v).InterfaceData()
}

func value_IsNil(v Value) bool {
	return toRV(v).IsNil()
}

func value_IsValid(v Value) bool {
	return toRV(v).IsValid()
}

func value_Kind(v Value) Kind {
	return toRV(v).Kind()
}

func value_Len(v Value) int {
	return toRV(v).Len()
}

func value_MapIndex(v Value, key Value) Value {
	return toV(toRV(v).MapIndex(toRV(key)))
}

func value_MapKeys(v Value) []Value {
	return toVs(toRV(v).MapKeys())
}

func value_MapRange(v Value) *MapIter {
	return (*MapIter)(toRV(v).MapRange())
}

func value_Method(v Value, i int) Value {
	return toV(toRV(v).Method(i))
}

func value_MethodByName(v Value, name string) Value {
	return toV(toRV(v).MethodByName(name))
}

func value_NumField(v Value) int {
	return toRV(v).NumField()
}

func value_NumMethod(v Value) int {
	return toRV(v).NumMethod()
}

func value_OverflowComplex(v Value, c complex128) bool {
	return toRV(v).OverflowComplex(c)
}

func value_OverflowFloat(v Value, f float64) bool {
	return toRV(v).OverflowFloat(f)
}

func value_OverflowInt(v Value, i int64) bool {
	return toRV(v).OverflowInt(i)
}

func value_OverflowUint(v Value, u uint64) bool {
	return toRV(v).OverflowUint(u)
}

func value_Pointer(v Value) uintptr {
	return toRV(v).Pointer()
}

func value_Recv(v Value) (Value, bool) {
	value, ok := toRV(v).Recv()
	return toV(value), ok
}

func value_Send(v Value, x Value) {
	toRV(v).Send(toRV(x))
}

func value_Set(v Value, x Value) {
	toRV(v).Set(toRV(x))
}

func value_SetBool(v Value, b bool) {
	toRV(v).SetBool(b)
}

func value_SetBytes(v Value, b []byte) {
	toRV(v).SetBytes(b)
}

func value_SetCap(v Value, i int) {
	toRV(v).SetCap(i)
}

func value_SetComplex(v Value, c complex128) {
	toRV(v).SetComplex(c)
}

func value_SetFloat(v Value, f float64) {
	toRV(v).SetFloat(f)
}

func value_SetInt(v Value, i int64) {
	toRV(v).SetInt(i)
}

func value_SetLen(v Value, i int) {
	toRV(v).SetLen(i)
}

func value_SetMapIndex(v Value, key Value, elem Value) {
	toRV(v).SetMapIndex(toRV(key), toRV(elem))
}

func value_SetPointer(v Value, p unsafe.Pointer) {
	toRV(v).SetPointer(p)
}

func value_SetString(v Value, s string) {
	toRV(v).SetString(s)
}

func value_SetUint(v Value, u uint64) {
	toRV(v).SetUint(u)
}

func value_Slice(v Value, i int, j int) Value {
	return toV(toRV(v).Slice(i, j))
}

func value_Slice3(v Value, i int, j int, k int) Value {
	return toV(toRV(v).Slice3(i, j, k))
}

func value_String(v Value) string {
	return toRV(v).String()
}

func value_TryRecv(v Value) (Value, bool) {
	value, ok := toRV(v).TryRecv()
	return toV(value), ok
}

func value_TrySend(v Value, x Value) bool {
	return toRV(v).TrySend(toRV(x))
}

func value_Type(v Value) Type {
	return ToType(toRV(v).Type())
}

func value_Uint(v Value) uint64 {
	return toRV(v).Uint()
}

func value_UnsafeAddr(v Value) uintptr {
	return toRV(v).UnsafeAddr()
}
