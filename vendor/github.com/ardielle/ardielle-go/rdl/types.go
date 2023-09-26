// Copyright 2015 Yahoo Inc.
// Licensed under the terms of the Apache version 2.0 license. See LICENSE file for terms.

package rdl

import (
	"fmt"
	"strings"
)

//
// TypeRegistry is a set of related types, usually initialized from a schema.
//
type TypeRegistry interface {
	Name() string
	FindType(name TypeRef) *Type
	BaseType(t *Type) BaseType
	BaseTypeName(tn TypeRef) TypeName
	FindBaseType(typeName TypeRef) BaseType
	IsBaseTypeName(name TypeRef) bool
	IsStringType(t *Type) bool
	IsStringTypeName(tn TypeRef) bool
	IsArrayType(t *Type) bool
	IsArrayTypeName(tn TypeRef) bool
}

type typeRegistry struct {
	name         Identifier
	baseTypeDefs map[string]*Type
	baseTypes    map[string]BaseType
	userTypes    map[string]*Type
}

// NewTypeRegistry creates and returns a new TypeRegistry, initialized with the specified schema
func NewTypeRegistry(schema *Schema) TypeRegistry {
	return newTypeRegistry(schema)
}

func newTypeRegistry(schema *Schema) *typeRegistry {
	reg := new(typeRegistry)
	reg.name = schema.Name
	reg.baseTypeDefs = make(map[string]*Type)
	reg.baseTypes = make(map[string]BaseType)
	reg.userTypes = make(map[string]*Type)

	for k, v := range namesBaseType {
		n := strings.ToLower(v)
		bt := BaseType(k)
		t := &Type{Variant: TypeVariantBaseType, BaseType: &bt}
		reg.baseTypeDefs[n] = t
		reg.baseTypes[v] = BaseType(k)
	}
	if schema != nil {
		for _, t := range schema.Types {
			reg.addType(t)
		}
	}
	return reg
}

func (reg *typeRegistry) Name() string {
	return string(reg.name)
}
func (reg *typeRegistry) FindType(name TypeRef) *Type {
	n := strings.ToLower(string(name))
	t, ok := reg.baseTypeDefs[n] //the basetypes are reserved, i.e. usertypes cannot override them
	if ok {
		return t
	}
	t, ok = reg.userTypes[n]
	if ok {
		return t
	}
	return nil
}

// TypeInfo returns common info that every type shares.
func TypeInfo(t *Type) (TypeName, TypeRef, string) {
	switch t.Variant {
	case TypeVariantAliasTypeDef:
		td := t.AliasTypeDef
		return td.Name, td.Type, td.Comment
	case TypeVariantStringTypeDef:
		td := t.StringTypeDef
		return td.Name, td.Type, td.Comment
	case TypeVariantNumberTypeDef:
		td := t.NumberTypeDef
		return td.Name, td.Type, td.Comment
	case TypeVariantArrayTypeDef:
		td := t.ArrayTypeDef
		return td.Name, td.Type, td.Comment
	case TypeVariantMapTypeDef:
		td := t.MapTypeDef
		return td.Name, td.Type, td.Comment
	case TypeVariantStructTypeDef:
		td := t.StructTypeDef
		return td.Name, td.Type, td.Comment
	case TypeVariantBytesTypeDef:
		td := t.BytesTypeDef
		return td.Name, td.Type, td.Comment
	case TypeVariantEnumTypeDef:
		td := t.EnumTypeDef
		return td.Name, td.Type, td.Comment
	case TypeVariantUnionTypeDef:
		td := t.UnionTypeDef
		return td.Name, td.Type, td.Comment
	case TypeVariantBaseType:
		td := t.BaseType
		s := namesBaseType[int(*td)]
		return TypeName(s), TypeRef(s), ""
	default:
		panic("whoops")
	}
}

func (reg *typeRegistry) addType(t *Type) {
	tName, _, _ := TypeInfo(t)
	n := strings.ToLower(string(tName))
	reg.userTypes[n] = t
}

func optionalAnyToString(any interface{}) string {
	if any == nil {
		return "null"
	}
	switch v := any.(type) {
	case *bool:
		return fmt.Sprintf("%v", *v)
	case *int8:
		return fmt.Sprintf("%d", *v)
	case *int16:
		return fmt.Sprintf("%d", *v)
	case *int32:
		return fmt.Sprintf("%d", *v)
	case *int64:
		return fmt.Sprintf("%d", *v)
	case *float32:
		return fmt.Sprintf("%g", *v)
	case *float64:
		return fmt.Sprintf("%g", *v)
	case *string:
		return *v
	case bool:
		return fmt.Sprintf("%v", v)
	case int8:
		return fmt.Sprintf("%d", v)
	case int16:
		return fmt.Sprintf("%d", v)
	case int32:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case float32:
		return fmt.Sprintf("%g", v)
	case float64:
		return fmt.Sprintf("%g", v)
	case string:
		return fmt.Sprintf("%v", v)
	default:
		panic("optionalAnyToString")
	}
}

//
// BaseTypeName returns the name of the RDL base type of the type specified by the given name
//
func (reg *typeRegistry) BaseType(t *Type) BaseType {
	var bt BaseType
	if t == nil {
		return bt //the zero value
	}
	tName, tType, _ := TypeInfo(t)
	for TypeRef(tName) != tType {
		tt := reg.FindType(tType)
		if tt == nil {
			return bt //the zero value
		}
		tName, tType, _ = TypeInfo(tt)
	}
	n, _ := reg.baseTypes[string(tName)]
	return n
}

func (reg *typeRegistry) BaseTypeName(typeName TypeRef) TypeName {
	return TypeName(reg.FindBaseType(typeName).String())
}

func (reg *typeRegistry) FindBaseType(typeName TypeRef) BaseType {
	var bt BaseType
	t := reg.FindType(typeName)
	if t != nil {
		bt = reg.BaseType(t)
	}
	return bt
}

// IsArrayType returns true if the type has an Array base type
func (reg *typeRegistry) IsArrayType(t *Type) bool {
	return reg.BaseType(t) == BaseTypeArray
}

// IsArrayTypeName returns true if the type name identifies a type with an Array base type
func (reg *typeRegistry) IsArrayTypeName(tn TypeRef) bool {
	t := reg.FindType(tn)
	if t == nil {
		return false
	}
	return reg.IsArrayType(t)
}

// 	IsBaseTypeName returns true if the type name is one of the base types
func (reg *typeRegistry) IsBaseTypeName(name TypeRef) bool {
	if name != "" {
		lower := strings.ToLower(string(name))
		for _, n := range namesBaseType {
			if lower == strings.ToLower(n) {
				return true
			}
		}
	}
	return false
}

// IsStringType returns true if the type has a String base type
func (reg *typeRegistry) IsStringType(t *Type) bool {
	return reg.BaseType(t) == BaseTypeString
}

// IsStringTypeName returns true if the type with the given name has a String base type
func (reg *typeRegistry) IsStringTypeName(tn TypeRef) bool {
	t := reg.FindType(tn)
	if t == nil {
		return false
	}
	return reg.IsStringType(t)
}

func isNumericType(baseType string) bool {
	switch baseType {
	case "Int32", "Float64", "Int64", "Int16", "Int8", "Float32":
		return true
	default:
		return false
	}
}
