package reflect

import (
	"reflect"
	"unsafe"
)

func toRT(t Type) reflect.Type {
	return type_toType(t)
}

func toRTs(t []Type) []reflect.Type {
	out := make([]reflect.Type, len(t))
	for idx, tt := range t {
		out[idx] = toRT(tt)
	}
	return out
}

func toT(t reflect.Type) Type {
	return (Type)(((*Value)(unsafe.Pointer(&t))).ptr)
}

func toRV(v Value) reflect.Value {
	return *(*reflect.Value)(unsafe.Pointer(&v))
}

func toRVs(v []Value) []reflect.Value {
	out := make([]reflect.Value, len(v))
	for idx, vv := range v {
		out[idx] = toRV(vv)
	}
	return out
}

func toV(v reflect.Value) Value {
	return *(*Value)(unsafe.Pointer(&v))
}

func toVs(v []reflect.Value) []Value {
	out := make([]Value, len(v))
	for idx, vv := range v {
		out[idx] = toV(vv)
	}
	return out
}

func toRSFs(v []StructField) []reflect.StructField {
	out := make([]reflect.StructField, len(v))
	for idx, vv := range v {
		out[idx] = toRSF(vv)
	}
	return out
}

func toRSF(v StructField) reflect.StructField {
	return reflect.StructField{
		Name:      v.Name,
		PkgPath:   v.PkgPath,
		Type:      ToReflectType(v.Type),
		Tag:       v.Tag,
		Offset:    v.Offset,
		Index:     v.Index,
		Anonymous: v.Anonymous,
	}
}

func toSF(v reflect.StructField) StructField {
	return StructField{
		Name:      v.Name,
		PkgPath:   v.PkgPath,
		Type:      ToType(v.Type),
		Tag:       v.Tag,
		Offset:    v.Offset,
		Index:     v.Index,
		Anonymous: v.Anonymous,
	}
}

func toM(v reflect.Method) Method {
	return Method{
		Name:    v.Name,
		PkgPath: v.PkgPath,
		Type:    ToType(v.Type),
		Func:    toV(v.Func),
		Index:   v.Index,
	}
}

func toRSC(v SelectCase) reflect.SelectCase {
	return reflect.SelectCase{
		Dir:  v.Dir,
		Chan: toRV(v.Chan),
		Send: toRV(v.Send),
	}
}

func toRSCs(v []SelectCase) []reflect.SelectCase {
	out := make([]reflect.SelectCase, len(v))
	for idx, vv := range v {
		out[idx] = toRSC(vv)
	}
	return out
}
