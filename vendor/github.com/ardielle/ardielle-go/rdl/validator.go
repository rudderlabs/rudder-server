// Copyright 2015 Yahoo Inc.
// Licensed under the terms of the Apache version 2.0 license. See LICENSE file for terms.

package rdl

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

//
// Validatable - if implemented, this can be called directly to validate. Otherwise, introspection is used.
//
type Validatable interface {
	Validate() error
}

// Validation is the result of a call to the validator. When sucessful, the Valid field is
// true, and when not, it is false, with other optional fields providing extra information
type Validation struct {
	Valid   bool        `json:"valid"`
	Type    string      `json:"type,omitempty"`
	Error   string      `json:"error,omitempty"`
	Value   interface{} `json:"value,omitempty"`
	Context string      `json:"context,omitempty"`
}

func (v Validation) String() string {
	data, _ := json.MarshalIndent(v, "", "    ")
	return string(data)
}

// A validator contains the requisite information to validate a schema against its types
type validator struct {
	registry TypeRegistry
	schema   *Schema
}

var validatorCache = struct {
	sync.RWMutex
	m map[*Schema]*validator
}{m: make(map[*Schema]*validator)}

var useValidatorCache bool

func ValidatorUseCache(flag bool) {
	useValidatorCache = flag
}

func getValidator(schema *Schema) *validator {
	var v *validator
	if !useValidatorCache {
		v = &validator{
			schema:   schema,
			registry: NewTypeRegistry(schema),
		}
		return v
	}

	validatorCache.RLock()
	if v, ok := validatorCache.m[schema]; ok {
		validatorCache.RUnlock()
		return v
	}
	validatorCache.RUnlock()

	validatorCache.Lock()
	defer validatorCache.Unlock()

	// Check to see if someone else got in and wrote it prior to us
	v, ok := validatorCache.m[schema]
	if !ok {
		v = &validator{
			schema:   schema,
			registry: NewTypeRegistry(schema),
		}
		validatorCache.m[schema] = v
	}
	return v
}

// Validate tests the provided generic data against a type in the specified schema. If the typename is empty,
// an attempt to guess the type is made, otherwise the check is done against the single type.
func Validate(schema *Schema, typename string, data interface{}) Validation {
	v := getValidator(schema)
	return validateWithValidator(v, typename, data)
}

// ValidateWithValidator tests the provided generic data using the supplied validator.  If the typename is empty,
// an attempt to guess the type is made, otherwise the check is done against the single type.
// Supplying the validator allows it to be reused between validations rather than constructing anew
func validateWithValidator(validator *validator, typename string, data interface{}) Validation {
	typelist := validator.schema.Types
	if typelist == nil {
		return validator.bad("top level", "Schema contains no types", data, "")
	}
	if typename == "" {
		//iterate over the types until we find the most (defined last) general match
		//But: not always useful: if structs are not "closed", they match on almost anything.
		for i := len(typelist) - 1; i >= 0; i-- {
			t := typelist[i]
			tName, _, _ := TypeInfo(t)
			v := validator.validate(t, data, string(tName))
			if v.Error == "" {
				return v
			}
		}
		return validator.bad("top level", "Cannot determine type of data in schema", data, "")
	}

	context := typename
	typedef := validator.registry.FindType(TypeRef(typename))

	if typedef != nil {
		return validator.validate(typedef, data, context)
	}
	return validator.bad(context, "No such type", nil, "")
}

func (checker *validator) resolveAliases(typedef *Type, context string) *Type {
	for typedef.Variant == TypeVariantAliasTypeDef {
		typedef = checker.registry.FindType(typedef.AliasTypeDef.Type)
	}
	return typedef
}

func (checker *validator) validate(t *Type, data interface{}, context string) Validation {
	t = checker.resolveAliases(t, context)
	base := checker.registry.BaseType(t)
	switch base {
	case BaseTypeAny:
		return checker.good(t, data)
	case BaseTypeUnion:
		return checker.validateUnion(t.UnionTypeDef, data, context)
	case BaseTypeString:
		return checker.validateString(t, data, context)
	}
	switch d := data.(type) {
	case bool:
		if base == BaseTypeBool {
			return checker.validateBool(t, d, context)
		}
	case float64:
		//only float64 is set up here, because that is what encoding/json returns for all numbers
		//this would have to be extended to support other sources of data than json
		switch base {
		case BaseTypeFloat64, BaseTypeFloat32:
			return checker.validateFloat(t, d, context)
		case BaseTypeInt32, BaseTypeInt64, BaseTypeInt16, BaseTypeInt8:
			return checker.validateFloat(t, float64(d), context)
		}
	case string:
		switch base {
		case BaseTypeString:
			return checker.validateString(t, d, context)
		case BaseTypeUUID:
			return checker.validateUUID(t, d, context)
		case BaseTypeSymbol:
			return checker.validateSymbol(t, d, context)
		case BaseTypeTimestamp:
			return checker.validateTimestamp(t, d, context)
		case BaseTypeEnum:
			return checker.validateEnum(t, d, context)
		}
	case []interface{}:
		if base == BaseTypeArray {
			return checker.validateArray(t, d, context)
		}
	case map[string]interface{}:
		if base == BaseTypeStruct {
			return checker.validateStruct(t, d, context)
		} else if base == BaseTypeMap {
			return checker.validateMap(t, d, context)
		}
	case map[Symbol]interface{}:
		dd := make(map[string]interface{})
		for k, v := range d {
			dd[string(k)] = v
		}
		if base == BaseTypeStruct {
			return checker.validateStruct(t, dd, context)
		} else if base == BaseTypeMap {
			return checker.validateMap(t, dd, context)
		}
	default:
		v, ok := data.(Validatable)
		if ok {
			err := v.Validate()
			if err == nil {
				return checker.good(t, data)
			}
		}
	}
	return checker.typeMismatchVariant(context, data, t)
}

func (checker *validator) flattenStringConstraints(t *Type, name TypeName, pattern string, values []string, min, max *int32) (TypeName, string, []string, *int32, *int32) {
	var super TypeRef
	switch t.Variant {
	case TypeVariantStringTypeDef:
		typedef := t.StringTypeDef
		if name == "" {
			name = typedef.Name
		}
		if pattern == "" && typedef.Pattern != "" {
			pattern = typedef.Pattern
		}
		if values == nil && typedef.Values != nil {
			values = typedef.Values
		}
		if min == nil && typedef.MinSize != nil {
			min = typedef.MinSize
		}
		if max == nil && typedef.MaxSize != nil {
			max = typedef.MaxSize
		}
		super = typedef.Type
	case TypeVariantAliasTypeDef:
		super = t.AliasTypeDef.Type
		if name == "" {
			name = t.AliasTypeDef.Name
		}
	case TypeVariantBaseType:
		if name == "" {
			name = "String"
		}
		return name, pattern, values, min, max
	}
	st := checker.registry.FindType(super)
	return checker.flattenStringConstraints(st, name, pattern, values, min, max)
}

func (checker *validator) validateString(t *Type, rawdata interface{}, context string) Validation {
	name, pattern, values, min, max := checker.flattenStringConstraints(t, "", "", nil, nil, nil)
	data := fmt.Sprintf("%s", rawdata)
	if strings.HasPrefix(data, "%!s") || data[0] == '&' {
		return checker.bad(context, "Not a string", rawdata, name)
	}
	if min != nil {
		if len(data) < int(*min) {
			return checker.bad(context, "String too small", data, name)
		}
	}
	if max != nil {
		if len(data) > int(*max) {
			return checker.bad(context, "String too large", data, name)
		}
	}
	if values != nil {
		match := false
		for _, v := range values {
			if v == data {
				match = true
				break
			}
		}
		if !match {
			return checker.bad(context, "Value mismatch in String type", data, name)
		}
	}
	if pattern != "" {
		pat := "^" + pattern + "$"
		matcher, err := regexp.Compile(pat)
		if err != nil {
			return checker.bad(context, "Bad pattern in String type definition /"+pat+"/", data, name)
		}
		if !matcher.MatchString(data) {
			return checker.bad(context, "Pattern mismatch in String type /"+pat+"/", data, name)
		}
	}
	return checker.good(t, data)
}

func (checker *validator) validateUUID(t *Type, data string, context string) Validation {
	matcher := regexp.MustCompile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
	if matcher.MatchString(data) {
		//fixme: check a few more bits to ensure a valid version
		return checker.good(t, data)
	}
	return checker.typeMismatchVariant(context, data, t)
}

func (checker *validator) validateSymbol(t *Type, data string, context string) Validation {
	return checker.good(t, data)
}

func (checker *validator) validateTimestamp(t *Type, data string, context string) Validation {
	matcher := regexp.MustCompile("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-5][0-9]:[0-5][0-9](.[0-9]+)?Z$")
	if matcher.MatchString(data) {
		year, _ := strconv.Atoi(data[0:4])
		month, _ := strconv.Atoi(data[5:7])
		day, _ := strconv.Atoi(data[8:10])
		hour, _ := strconv.Atoi(data[11:13])
		minute, _ := strconv.Atoi(data[14:16])
		second, _ := strconv.Atoi(data[17:19])
		//any precision is ok: decimal, _ := strconv.Atoi(data[17:len(data)-1])
		if year >= 1970 && month >= 1 && month <= 12 {
			if hour >= 0 && hour < 24 && minute >= 0 && minute < 60 && second >= 0 && second < 60 && day >= 1 {
				maxDay := 31
				switch month {
				case 4, 6, 9, 11:
					maxDay = 30
				case 2:
					leap := (year % 4) == 0
					if (year % 100) == 0 {
						leap = (year % 400) == 0
					}
					if leap {
						maxDay = 29
					} else {
						maxDay = 28
					}
				}
				if day <= maxDay {
					return checker.good(t, data)
				}
			}
		}
	}
	return checker.typeMismatchVariant(context, data, t)
}

func (checker *validator) validateBool(t *Type, data bool, context string) Validation {
	return checker.good(t, data)
}

func (checker *validator) validateFloat(t *Type, data float64, context string) Validation {
	if t.Variant == TypeVariantNumberTypeDef {
		typedef := t.NumberTypeDef
		if typedef.Min != nil {
			min := toFloat(typedef.Min, -math.MaxFloat64)
			if data < min {
				return checker.bad(context, "Value is less than 'min' constraint", data, typedef.Name)
			}
		}
		if typedef.Max != nil {
			max := toFloat(typedef.Max, math.MaxFloat64)
			if data > max {
				return checker.bad(context, "Value is greater than 'max' constraint", data, typedef.Name)
			}
		}
	}
	return checker.good(t, data)
}

func (checker *validator) validateMap(t *Type, data map[string]interface{}, context string) Validation {
	typedef := t.MapTypeDef
	mlen := len(data)
	if typedef.Size != nil {
		size := toInt(typedef.Size, math.MinInt32)
		if mlen != size {
			return checker.bad(context, "Map is not of the specified size", data, typedef.Name)
		}
	}
	if typedef.MinSize != nil {
		minsize := toInt(typedef.MinSize, math.MinInt32)
		if mlen < minsize {
			return checker.bad(context, "Map is smaller than specified minimum size", data, typedef.Name)
		}
	}
	if typedef.MaxSize != nil {
		maxsize := toInt(typedef.MaxSize, math.MaxInt32)
		if mlen > maxsize {
			return checker.bad(context, "Map is larger than specified maximum size", data, typedef.Name)
		}
	}
	if mlen > 0 {
		if typedef.Items != "Any" {
			if typedef.Keys != "Any" {
				//check both keys and item types
				it := checker.registry.FindType(typedef.Items)
				kt := checker.registry.FindType(typedef.Keys)
				for key, item := range data {
					v := checker.validate(kt, key, fmt.Sprintf("%s[%v]", context, key))
					if v.Error == "" {
						v = checker.validate(it, item, fmt.Sprintf("%s[%v]", context, key))
					}
					if !v.Valid {
						return v
					}
				}
			} else {
				//we have a specified item type, make sure all items are of that type
				it := checker.registry.FindType(typedef.Items)
				for key, item := range data {
					v := checker.validate(it, item, fmt.Sprintf("%s[%v]", context, key))
					if !v.Valid {
						return v
					}
				}
			}
		} else if typedef.Keys != "String" {
			kt := checker.registry.FindType(typedef.Keys)
			for key := range data {
				v := checker.validate(kt, key, fmt.Sprintf("%s[%v]", context, key))
				if !v.Valid {
					return v
				}
			}
		}
	}
	return checker.good(t, data)
}

func (checker *validator) validateArray(t *Type, data []interface{}, context string) Validation {
	typedef := t.ArrayTypeDef
	alen := len(data)
	if typedef.Size != nil {
		size := toInt(typedef.Size, math.MinInt32)
		if alen != size {
			return checker.bad(context, "Array is not of the specified size", data, typedef.Name)
		}
	}
	if typedef.MinSize != nil {
		minsize := toInt(typedef.MinSize, math.MinInt32)
		if alen < minsize {
			return checker.bad(context, "Array is smaller than specified minimum size", data, typedef.Name)
		}
	}
	if typedef.MaxSize != nil {
		maxsize := toInt(typedef.MaxSize, math.MaxInt32)
		if alen > maxsize {
			return checker.bad(context, "Array is larger than specified maximum size", data, typedef.Name)
		}
	}
	if alen > 0 && typedef.Items != "Any" {
		//we have a specified item type, make sure all items are of that type
		it := checker.registry.FindType(typedef.Items)
		for i, item := range data {
			v := checker.validate(it, item, fmt.Sprintf("%s[%d]", context, i))
			if !v.Valid {
				return v
			}
		}
	}
	return checker.good(t, data)
}

func (checker *validator) synthesizeFieldType(t *Type, field *StructFieldDef) *Type {
	tName, _, _ := TypeInfo(t)
	if tName == "Map" {
		mt := new(MapTypeDef)
		k := TypeRef("String")
		i := TypeRef("Any")
		if field.Keys != "" {
			k = field.Keys
		}
		if field.Items != "" {
			i = field.Items
		}
		mt.Name = TypeName("MapOf" + string(k) + "To" + string(i))
		mt.Type = "Map"
		mt.Keys = k
		mt.Items = i
		return &Type{Variant: TypeVariantMapTypeDef, MapTypeDef: mt}
	} else if tName == "Array" {
		at := new(ArrayTypeDef)
		i := TypeRef("Any")
		if field.Items != "" {
			i = field.Items
		}
		at.Name = TypeName("ArrayOf" + string(i))
		at.Type = "Array"
		at.Items = i
		return &Type{Variant: TypeVariantArrayTypeDef, ArrayTypeDef: at}
	}
	return t
}

func (checker *validator) validateStruct(t *Type, data map[string]interface{}, context string) Validation {
	typedef := t.StructTypeDef
	closed := typedef.Closed
	var fields []*StructFieldDef
	baseType := typedef
	for {
		for _, f := range baseType.Fields {
			fields = append(fields, f)
		}
		if strings.ToLower(string(baseType.Type)) == "struct" {
			break
		}
		t := checker.registry.FindType(baseType.Type)
		baseType = t.StructTypeDef
	}
	var seen map[Identifier]Identifier
	if closed {
		seen = make(map[Identifier]Identifier)
	}
	for _, f := range fields {
		if seen != nil {
			seen[f.Name] = f.Name
		}
		if d, ok := data[string(f.Name)]; ok {
			t := checker.registry.FindType(f.Type)
			tf := checker.synthesizeFieldType(t, f)
			v := checker.validate(tf, d, context+"."+string(f.Name))
			if !v.Valid {
				return v
			}
		} else {
			if !f.Optional && f.Default == nil {
				return checker.bad(context, "Field missing: "+string(f.Name), data, typedef.Name)
			}
		}
	}
	if seen != nil {
		for k := range data {
			if _, ok := seen[Identifier(k)]; !ok {
				return checker.bad(context, "Unexpected field: '"+k+"'", data, typedef.Name)
			}
		}
	}
	return checker.good(t, data)
}

func (checker *validator) validateEnum(t *Type, data string, context string) Validation {
	typedef := t.EnumTypeDef
	for _, e := range typedef.Elements {
		if string(e.Symbol) == data {
			return checker.good(t, data)
		}
	}
	return checker.bad(context, "Invalid value in Enum type", data, typedef.Name)
}

func (checker *validator) validateUnion(typedef *UnionTypeDef, data interface{}, context string) Validation {
	wrapper, ok := data.(map[string]interface{})
	if !ok {
		return checker.bad(context, "Missing wrapper for Union type", data, typedef.Name)
	}
	if len(wrapper) != 1 {
		return checker.bad(context, "Bad wrapper for Union type", data, typedef.Name)
	}
	var t *Type
	var d interface{}
	for k := range wrapper {
		t = checker.registry.FindType(TypeRef(k))
		if t == nil {
			return checker.bad(context, "Bad type, not part of Union", data, TypeName(k))
		}
		d = wrapper[k]
	}
	return checker.validate(t, d, context)
}

func (checker *validator) good(t *Type, data interface{}) Validation {
	tName, _, _ := TypeInfo(t)
	return Validation{true, string(tName), "", data, ""}

}

func (checker *validator) bad(context string, msg string, data interface{}, typename TypeName) Validation {
	var d interface{}
	d = data
	v := Validation{false, string(typename), msg, d, context}
	return v
}

func (checker *validator) typeMismatch(context string, data interface{}, typename TypeName) Validation {
	if strings.ToLower(string(typename)) == "any" {
		panic("HERE!")
	}
	return checker.bad(context, "Bad "+string(typename), data, typename)
}

func (checker *validator) typeMismatchVariant(context string, data interface{}, t *Type) Validation {
	typename := TypeName("?")
	switch t.Variant {
	case TypeVariantBaseType:
		typename = TypeName(t.String())
	case TypeVariantStructTypeDef:
		typename = t.StructTypeDef.Name
	case TypeVariantEnumTypeDef:
		typename = t.EnumTypeDef.Name
	case TypeVariantStringTypeDef:
		typename = t.StringTypeDef.Name
	case TypeVariantMapTypeDef:
		typename = t.MapTypeDef.Name
	case TypeVariantArrayTypeDef:
		typename = t.ArrayTypeDef.Name
	case TypeVariantBytesTypeDef:
		typename = t.BytesTypeDef.Name
	case TypeVariantNumberTypeDef:
		typename = t.NumberTypeDef.Name
	case TypeVariantUnionTypeDef:
		typename = t.UnionTypeDef.Name
	case TypeVariantAliasTypeDef:
		typename = t.AliasTypeDef.Name
	}
	return checker.typeMismatch(context, data, typename)
}
