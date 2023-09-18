// Copyright 2015 Yahoo Inc.
// Licensed under the terms of the Apache version 2.0 license. See LICENSE file for terms.

package rdl

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"text/template"
)

const MAX_COLUMNS = 128

func UnparseRDLFile(schema *Schema, filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(f)
	err = UnparseRDL(schema, writer)
	if err != nil {
		return err
	}
	return f.Close()
}

func UnparseRDL(schema *Schema, out *bufio.Writer) error {
	reg := newTypeRegistry(schema)
	funcMap := template.FuncMap{
		"header": func() string {
			s := formatComment(schema.Comment, 0, MAX_COLUMNS)
			if schema.Namespace != "" {
				s += fmt.Sprintf("namespace %s;\n", schema.Namespace)
			}
			s += fmt.Sprintf("name %s;\n", schema.Name)
			if schema.Version != nil {
				s += fmt.Sprintf("version %d;\n", *schema.Version)
			}
			if schema.Base != "" {
				s += fmt.Sprintf("base %q;\n", schema.Base)
			}
			return s
		},
		"unparseType":     func(t *Type) string { return unparseType(reg, t) },
		"unparseResource": func(r *Resource) string { return unparseResource(reg, r) },
	}
	unparseTemplate := `{{header}}{{range .Types}}
{{unparseType .}}{{end}}
{{range .Resources}}
{{unparseResource .}}{{end}}`
	t := template.Must(template.New("rdl").Funcs(funcMap).Parse(unparseTemplate))
	err := t.Execute(out, schema)
	out.Flush()
	return err
}

func unparseType(reg TypeRegistry, t *Type) string {
	switch t.Variant {
	//case TypeVariantBaseTypeDef: //never happens
	case TypeVariantStructTypeDef:
		return unparseStructType(reg, t.StructTypeDef)
	case TypeVariantMapTypeDef:
		return unparseMapType(t.MapTypeDef)
	case TypeVariantArrayTypeDef:
		return unparseArrayType(t.ArrayTypeDef)
	case TypeVariantEnumTypeDef:
		return unparseEnumType(t.EnumTypeDef)
	case TypeVariantUnionTypeDef:
		return unparseUnionType(t.UnionTypeDef)
	case TypeVariantStringTypeDef:
		return unparseStringType(t.StringTypeDef)
	case TypeVariantBytesTypeDef:
		return unparseBytesType(t.BytesTypeDef)
	case TypeVariantNumberTypeDef:
		return unparseNumberType(t.NumberTypeDef)
	case TypeVariantAliasTypeDef:
		return unparseAliasType(t.AliasTypeDef)
	default:
		return ""
	}
}

func unparseNumberValue(n *Number) string {
	switch n.Variant {
	case NumberVariantInt8:
		return fmt.Sprint(*n.Int8)
	case NumberVariantInt16:
		return fmt.Sprint(*n.Int16)
	case NumberVariantInt32:
		return fmt.Sprint(*n.Int32)
	case NumberVariantInt64:
		return fmt.Sprint(*n.Int64)
	case NumberVariantFloat32:
		return fmt.Sprint(*n.Float32)
	case NumberVariantFloat64:
		return fmt.Sprint(*n.Float64)
	default:
		return ""
	}
}

func unparseLiteral(reg TypeRegistry, tref TypeRef, o interface{}) string {
	switch v := o.(type) {
	case *Number:
		return unparseNumberValue(v)
	case string:
		if tref != "String" {
			t := reg.BaseTypeName(tref)
			if t == "Enum" {
				return v
			}
		}
		return fmt.Sprintf("%q", v)
	default:
		return fmt.Sprint(o)
	}
}

func unparseNumberType(td *NumberTypeDef) string {
	s := ""
	if td.Comment != "" {
		s = formatComment(td.Comment, 0, MAX_COLUMNS)
	}
	s += fmt.Sprintf("type %s %s", td.Name, td.Type)
	options := make([]string, 0)
	if td.Min != nil {
		options = append(options, fmt.Sprintf("min=%v", unparseNumberValue(td.Min)))
	}
	if td.Max != nil {
		options = append(options, fmt.Sprintf("max=%v", unparseNumberValue(td.Max)))
	}
	if td.Annotations != nil {
		for k, v := range td.Annotations {
			options = append(options, fmt.Sprintf("%s=%q", k, v))
		}
	}
	if len(options) > 0 {
		s = s + " (" + strings.Join(options, ", ") + ")"
	}
	s += ";\n"
	return s
}

func unparseStringType(td *StringTypeDef) string {
	s := ""
	if td.Comment != "" {
		s = formatComment(td.Comment, 0, MAX_COLUMNS)
	}
	s += fmt.Sprintf("type %s %s", td.Name, td.Type)
	options := make([]string, 0)
	if td.Pattern != "" {
		options = append(options, fmt.Sprintf("pattern=%q", td.Pattern))
	}
	if td.MaxSize != nil {
		options = append(options, fmt.Sprintf("maxsize=%d", *td.MaxSize))
	}
	if td.MinSize != nil {
		options = append(options, fmt.Sprintf("minsize=%d", *td.MinSize))
	}
	if td.Values != nil {
		o := "values=["
		for i, v := range td.Values {
			if i != 0 {
				o += ","
			}
			o += fmt.Sprintf("%q", v)
		}
		o += "]"
		options = append(options, o)
	}
	if td.Annotations != nil {
		for k, v := range td.Annotations {
			options = append(options, fmt.Sprintf("%s=%q", k, v))
		}
	}
	if len(options) > 0 {
		s = s + " (" + strings.Join(options, ", ") + ")"
	}
	return s + ";\n"
}

func unparseBytesType(td *BytesTypeDef) string {
	s := ""
	if td.Comment != "" {
		s = formatComment(td.Comment, 0, MAX_COLUMNS)
	}
	s += fmt.Sprintf("type %s %s", td.Name, td.Type)
	options := make([]string, 0)
	if td.Size != nil {
		options = append(options, fmt.Sprintf("size=%d", *td.Size))
	}
	if td.MaxSize != nil {
		options = append(options, fmt.Sprintf("maxsize=%d", *td.MaxSize))
	}
	if td.MinSize != nil {
		options = append(options, fmt.Sprintf("minsize=%d", *td.MinSize))
	}
	if td.Annotations != nil {
		for k, v := range td.Annotations {
			options = append(options, fmt.Sprintf("%s=%q", k, v))
		}
	}
	if len(options) > 0 {
		s = s + " (" + strings.Join(options, ", ") + ")"
	}
	return s + ";\n"
}

func unparseArrayType(td *ArrayTypeDef) string {
	s := ""
	if td.Comment != "" {
		s = formatComment(td.Comment, 0, MAX_COLUMNS)
	}
	items := "Any"
	if td.Items != "" {
		items = string(td.Items)
	}
	s += fmt.Sprintf("type %s %s<%s>", td.Name, td.Type, items)
	options := make([]string, 0)
	if td.Size != nil {
		options = append(options, fmt.Sprintf("size=%d", *td.Size))
	}
	if td.MinSize != nil {
		options = append(options, fmt.Sprintf("minsize=%d", *td.MinSize))
	}
	if td.MaxSize != nil {
		options = append(options, fmt.Sprintf("maxsize=%d", *td.MaxSize))
	}
	if td.Annotations != nil {
		for k, v := range td.Annotations {
			options = append(options, fmt.Sprintf("%s=%q", k, v))
		}
	}
	if len(options) > 0 {
		s += " (" + strings.Join(options, ", ") + ")"
	}
	s += ";\n"
	return s
}

func unparseMapType(td *MapTypeDef) string {
	s := ""
	if td.Comment != "" {
		s = formatComment(td.Comment, 0, MAX_COLUMNS)
	}
	keys := "String"
	if td.Keys != "" {
		keys = string(td.Keys)
	}
	items := "Any"
	if td.Items != "" {
		items = string(td.Items)
	}
	s += fmt.Sprintf("type %s %s<%s,%s>", td.Name, td.Type, keys, items)
	options := make([]string, 0)
	if td.Size != nil {
		options = append(options, fmt.Sprintf("size=%d", *td.Size))
	}
	if td.MinSize != nil {
		options = append(options, fmt.Sprintf("minsize=%d", *td.MinSize))
	}
	if td.MaxSize != nil {
		options = append(options, fmt.Sprintf("maxsize=%d", *td.MaxSize))
	}
	if td.Annotations != nil {
		for k, v := range td.Annotations {
			options = append(options, fmt.Sprintf("%s=%q", k, v))
		}
	}
	if len(options) > 0 {
		s += " (" + strings.Join(options, ", ") + ")"
	}
	s += ";\n"
	return s
}

func unparseStructType(reg TypeRegistry, td *StructTypeDef) string {
	s := ""
	if td.Comment != "" {
		s = formatComment(td.Comment, 0, MAX_COLUMNS)
	}
	s += fmt.Sprintf("type %s %s", td.Name, td.Type)
	options := make([]string, 0)
	if td.Annotations != nil {
		for k, v := range td.Annotations {
			options = append(options, fmt.Sprintf("%s=%q", k, v))
		}
	}
	if len(options) > 0 {
		s += " (" + strings.Join(options, ", ") + ")"
	}
	s += " {\n"
	for _, f := range td.Fields {
		//fields
		if f.Type == "Map" {
			k := "String"
			if f.Keys != "" {
				k = string(f.Keys)
			}
			v := "Any"
			if f.Items != "" {
				v = string(f.Items)
			}
			s += fmt.Sprintf("\tMap<%s,%s> %s", k, v, f.Name)
		} else if f.Type == "Array" {
			v := "Any"
			if f.Items != "" {
				v = string(f.Items)
			}
			s += fmt.Sprintf("\tArray<%s> %s", v, f.Name)
		} else {
			s += fmt.Sprintf("\t%s %s", f.Type, f.Name)
		}
		options := make([]string, 0)
		if f.Optional {
			options = append(options, "optional")
		}
		if f.Default != nil {
			//bug: enum literals get quoted, RDL spec says they shouldn't
			options = append(options, fmt.Sprintf("default=%s", unparseLiteral(reg, f.Type, f.Default)))
		}
		if f.Annotations != nil {
			for k, v := range f.Annotations {
				options = append(options, fmt.Sprintf("%s=%q", k, v))
			}
		}
		if len(options) > 0 {
			s = s + " (" + strings.Join(options, ", ") + ")"
		}
		//optional, default, annotations
		if f.Comment != "" {
			s += "; //" + f.Comment + "\n"
		} else {
			s += ";\n"
		}
	}
	return s + "}\n"
}

func unparseEnumType(td *EnumTypeDef) string {
	s := ""
	if td.Comment != "" {
		s = formatComment(td.Comment, 0, MAX_COLUMNS)
	}
	s += fmt.Sprintf("type %s %s", td.Name, td.Type)
	options := make([]string, 0)
	if td.Annotations != nil {
		for k, v := range td.Annotations {
			options = append(options, fmt.Sprintf("%s=%q", k, v))
		}
	}
	if len(options) > 0 {
		s += " (" + strings.Join(options, ", ") + ")"
	}
	s += " {\n"
	for _, e := range td.Elements {
		s += fmt.Sprintf("\t%s", e.Symbol)
		if e.Comment != "" {
			s += " // " + e.Comment
		}
		s += "\n"
	}
	return s + "}\n"
}

func unparseUnionType(td *UnionTypeDef) string {
	s := ""
	if td.Comment != "" {
		s = formatComment(td.Comment, 0, MAX_COLUMNS)
	}
	v := make([]string, 0)
	for _, e := range td.Variants {
		v = append(v, string(e))
	}
	e := strings.Join(v, ",")
	s += fmt.Sprintf("type %s %s<%s>", td.Name, td.Type, e)
	options := make([]string, 0)
	if td.Annotations != nil {
		for k, v := range td.Annotations {
			options = append(options, fmt.Sprintf("%s=%q", k, v))
		}
	}
	if len(options) > 0 {
		s += " (" + strings.Join(options, ", ") + ")"
	}
	return s + "\n"
}

func unparseAliasType(td *AliasTypeDef) string {
	s := ""
	if td.Comment != "" {
		s = formatComment(td.Comment, 0, MAX_COLUMNS)
	}
	s += fmt.Sprintf("type %s %s", td.Name, td.Type)
	options := make([]string, 0)
	if td.Annotations != nil {
		for k, v := range td.Annotations {
			options = append(options, fmt.Sprintf("%s=%q", k, v))
		}
	}
	if len(options) > 0 {
		s += " (" + strings.Join(options, ", ") + ")"
	}
	return s + ";\n"
}

func unparseQueryParam(r *Resource) string {
	q := ""
	for _, in := range r.Inputs {
		if in.QueryParam != "" {
			if q == "" {
				q = "?"
			} else {
				q += "&"
			}
			q += fmt.Sprintf("%s={%s}", in.QueryParam, in.Name)
		}
	}
	return q
}

func unparseResource(reg TypeRegistry, r *Resource) string {
	s := ""
	if r.Comment != "" {
		s = formatComment(r.Comment, 0, MAX_COLUMNS)
	}
	q := unparseQueryParam(r)
	s += fmt.Sprintf("resource %s %s %q", r.Type, r.Method, r.Path+q)
	options := make([]string, 0)
	if r.Async != nil && *r.Async {
		options = append(options, "async")
	}
	if r.Name != "" {
		options = append(options, fmt.Sprintf("name=%s", r.Name))
	}
	if r.Annotations != nil {
		for k, v := range r.Annotations {
			options = append(options, fmt.Sprintf("%s=%q", k, v))
		}
	}
	if len(options) > 0 {
		s += " (" + strings.Join(options, ", ") + ")"
	}
	s += " {\n"
	for _, in := range r.Inputs {
		s += fmt.Sprintf("\t%s %s", in.Type, in.Name)
		//header
		options := make([]string, 0)
		isOptional := false
		if in.QueryParam != "" {
			isOptional = true
		} else if in.Header != "" {
			isOptional = true
			options = append(options, fmt.Sprintf("header=%q", in.Header))
		}
		if isOptional {
			if in.Default != nil {
				options = append(options, fmt.Sprintf("default=%v", in.Default))
			} else {
				options = append(options, "optional")
			}
		}
		if in.Annotations != nil {
			for k, v := range in.Annotations {
				options = append(options, fmt.Sprintf("%s=%q", k, v))
			}
		}
		if len(options) > 0 {
			s += " (" + strings.Join(options, ", ") + ")"
		}
		if in.Comment != "" {
			s += "; //" + in.Comment + "\n"
		} else {
			s += ";\n"
		}
	}
	for _, out := range r.Outputs {
		s += fmt.Sprintf("\t%s %s", out.Type, out.Name)
		options := make([]string, 0)
		if out.Header != "" {
			options = append(options, fmt.Sprintf("header=%q", out.Header))
			options = append(options, "out")
		}
		if out.Optional {
			options = append(options, "optional")
		}
		if out.Annotations != nil {
			for k, v := range out.Annotations {
				options = append(options, fmt.Sprintf("%s=%q", k, v))
			}
		}
		if len(options) > 0 {
			s += " (" + strings.Join(options, ", ") + ")"
		}
		if out.Comment != "" {
			s += "; //" + out.Comment + "\n"
		} else {
			s += ";\n"
		}
	}
	if r.Auth != nil {
		//authorize("DELETE", "contacts");
		if r.Auth.Action != "" && r.Auth.Resource != "" {
			s += fmt.Sprintf("\tauthorize(%q, %q);\n", r.Auth.Action, r.Auth.Resource)
		} else if r.Auth.Authenticate {
			s += fmt.Sprintf("\tauthenticate;\n")
		}
	}
	expected := "OK"
	if r.Expected != "" {
		expected = r.Expected
	}
	if r.Alternatives != nil {
		expected += ", " + strings.Join(r.Alternatives, ", ")
	}
	s += "\texpected " + expected + ";\n"
	if r.Exceptions != nil {
		s += "\texceptions {\n"
		for ecode, e := range r.Exceptions {
			c := ""
			if e.Comment != "" {
				c = " // " + e.Comment
			}
			s += fmt.Sprintf("\t\t%s %s;%s\n", e.Type, ecode, c)
		}
		s += "\t}\n"
	}
	if r.Consumes != nil {
		//fix me
	}
	if r.Produces != nil {
		//fix me
	}
	return s + "}\n"
}
