package reflect

import (
	"reflect"
	_ "unsafe"
)

//go:linkname ifaceIndir reflect.ifaceIndir
//go:noescape
func ifaceIndir(Type) bool

func arrayOf(i int, typ Type) Type {
	return toT(reflect.ArrayOf(i, toRT(typ)))
}

func chanOf(dir ChanDir, typ Type) Type {
	return toT(reflect.ChanOf(dir, toRT(typ)))
}

func funcOf(in []Type, out []Type, variadic bool) Type {
	return toT(reflect.FuncOf(toRTs(in), toRTs(out), variadic))
}

func mapOf(key Type, elem Type) Type {
	return toT(reflect.MapOf(toRT(key), toRT(elem)))
}

func ptrTo(typ Type) Type {
	return toT(reflect.PtrTo(toRT(typ)))
}

func sliceOf(t Type) Type {
	return toT(reflect.SliceOf(toRT(t)))
}

func structOf(fields []StructField) Type {
	return ToType(reflect.StructOf(toRSFs(fields)))
}

//go:linkname type_Align reflect.(*rtype).Align
//go:noescape
func type_Align(Type) int

//go:linkname type_FieldAlign reflect.(*rtype).FieldAlign
//go:noescape
func type_FieldAlign(Type) int

//go:linkname type_Method reflect.(*rtype).Method
//go:noescape
func type_Method(Type, int) reflect.Method

//go:linkname type_MethodByName reflect.(*rtype).MethodByName
//go:noescape
func type_MethodByName(Type, string) (reflect.Method, bool)

//go:linkname type_NumMethod reflect.(*rtype).NumMethod
//go:noescape
func type_NumMethod(Type) int

//go:linkname type_Name reflect.(*rtype).Name
//go:noescape
func type_Name(Type) string

//go:linkname type_PkgPath reflect.(*rtype).PkgPath
//go:noescape
func type_PkgPath(Type) string

//go:linkname type_Size reflect.(*rtype).Size
//go:noescape
func type_Size(Type) uintptr

//go:linkname type_String reflect.(*rtype).String
//go:noescape
func type_String(Type) string

//go:linkname type_Kind reflect.(*rtype).Kind
//go:noescape
func type_Kind(Type) reflect.Kind

//go:linkname type_Implements reflect.(*rtype).Implements
//go:noescape
func type_Implements(Type, reflect.Type) bool

//go:linkname type_AssignableTo reflect.(*rtype).AssignableTo
//go:noescape
func type_AssignableTo(Type, reflect.Type) bool

//go:linkname type_ConvertibleTo reflect.(*rtype).ConvertibleTo
//go:noescape
func type_ConvertibleTo(Type, reflect.Type) bool

//go:linkname type_Comparable reflect.(*rtype).Comparable
//go:noescape
func type_Comparable(Type) bool

//go:linkname type_Bits reflect.(*rtype).Bits
//go:noescape
func type_Bits(Type) int

//go:linkname type_ChanDir reflect.(*rtype).ChanDir
//go:noescape
func type_ChanDir(Type) ChanDir

//go:linkname type_IsVariadic reflect.(*rtype).IsVariadic
//go:noescape
func type_IsVariadic(Type) bool

//go:linkname type_Elem reflect.(*rtype).Elem
//go:noescape
func type_Elem(t Type) reflect.Type

//go:linkname type_Field reflect.(*rtype).Field
//go:noescape
func type_Field(typ Type, i int) reflect.StructField

//go:linkname type_FieldByIndex reflect.(*rtype).FieldByIndex
//go:noescape
func type_FieldByIndex(Type, []int) reflect.StructField

//go:linkname type_FieldByName reflect.(*rtype).FieldByName
//go:noescape
func type_FieldByName(Type, string) (reflect.StructField, bool)

//go:linkname type_FieldByNameFunc reflect.(*rtype).FieldByNameFunc
//go:noescape
func type_FieldByNameFunc(Type, func(string) bool) (reflect.StructField, bool)

//go:linkname type_In reflect.(*rtype).In
//go:noescape
func type_In(Type, int) reflect.Type

//go:linkname type_Key reflect.(*rtype).Key
//go:noescape
func type_Key(Type) reflect.Type

//go:linkname type_Len reflect.(*rtype).Len
//go:noescape
func type_Len(Type) int

//go:linkname type_NumField reflect.(*rtype).NumField
//go:noescape
func type_NumField(Type) int

//go:linkname type_NumIn reflect.(*rtype).NumIn
//go:noescape
func type_NumIn(Type) int

//go:linkname type_NumOut reflect.(*rtype).NumOut
//go:noescape
func type_NumOut(Type) int

//go:linkname type_Out reflect.(*rtype).Out
//go:noescape
func type_Out(Type, int) reflect.Type

//go:linkname type_toType reflect.toType
//go:noescape
func type_toType(t Type) reflect.Type

var dummy struct {
	b bool
	x interface{}
}

func escape(x interface{}) {
	if dummy.b {
		dummy.x = x
	}
}
