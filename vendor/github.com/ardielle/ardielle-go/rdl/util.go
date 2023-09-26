// Copyright 2015 Yahoo Inc.
// Licensed under the terms of the Apache version 2.0 license. See LICENSE file for terms.

package rdl

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

func equal(o1 interface{}, o2 interface{}) bool {
	return reflect.DeepEqual(o1, o2)
}

func toInt(o interface{}, defaultValue int) int {
	switch n := o.(type) {
	case *float64:
		return int(*n)
	case *float32:
		return int(*n)
	case *int64:
		return int(*n)
	case *int32:
		return int(*n)
	case *int16:
		return int(*n)
	case *int8:
		return int(*n)
	case *int:
		return *n
	default:
		return defaultValue
	}
}

func toFloat(o interface{}, defaultValue float64) float64 {
	switch n := o.(type) {
	case *float64:
		return float64(*n)
	case *float32:
		return float64(*n)
	case *int64:
		return float64(*n)
	case *int32:
		return float64(*n)
	case *int16:
		return float64(*n)
	case *int8:
		return float64(*n)
	case *int:
		return float64(*n)
	default:
		return defaultValue
	}
}

func addFields(reg TypeRegistry, dst []*StructFieldDef, t *Type) []*StructFieldDef {
	switch t.Variant {
	case TypeVariantStructTypeDef:
		st := t.StructTypeDef
		if st.Type != "Struct" {
			dst = addFields(reg, dst, reg.FindType(st.Type))
		}
		for _, f := range st.Fields {
			dst = append(dst, f)
		}
	}
	return dst
}

func flattenedFields(reg TypeRegistry, t *Type) []*StructFieldDef {
	return addFields(reg, make([]*StructFieldDef, 0), t)
}

func formatBlock(s string, leftCol int, rightCol int, prefix string) string {
	if s == "" {
		return ""
	}
	tab := spaces(leftCol)
	var buf bytes.Buffer
	max := rightCol
	col := leftCol
	lines := 1
	tokens := strings.Split(s, " ")
	for _, tok := range tokens {
		toklen := len(tok)
		if col+toklen >= max {
			buf.WriteString("\n")
			lines++
			buf.WriteString(tab)
			buf.WriteString(prefix)
			buf.WriteString(tok)
			col = leftCol + 3 + toklen
		} else {
			if col == leftCol {
				col += len(prefix)
				buf.WriteString(prefix)
			} else {
				buf.WriteString(" ")
			}
			buf.WriteString(tok)
			col += toklen + 1
		}
	}
	buf.WriteString("\n")
	emptyPrefix := strings.Trim(prefix, " ")
	pad := tab + emptyPrefix + "\n"
	return pad + buf.String() + pad
}

func formatComment(s string, leftCol int, rightCol int) string {
	return formatBlock(s, leftCol, rightCol, "// ")
}

func spaces(count int) string {
	return stringOfChar(count, ' ')
}

func stringOfChar(count int, b byte) string {
	buf := make([]byte, 0, count)
	for i := 0; i < count; i++ {
		buf = append(buf, b)
	}
	return string(buf)
}

func capitalize(text string) string {
	return strings.ToUpper(text[0:1]) + text[1:]
}

func uncapitalize(text string) string {
	return strings.ToLower(text[0:1]) + text[1:]
}

func leftJustified(text string, width int) string {
	return text + spaces(width-len(text))
}

func (s Schema) String() string {
	data, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		return "<schema: error = " + err.Error() + ">"
	}
	return string(data)
}

func (s TypeDef) String() string {
	data, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		return "<TypeDef: error = " + err.Error() + ">"
	}
	return string(data)
}
func (s ArrayTypeDef) String() string {
	data, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		return "<ArrayTypeDef: error = " + err.Error() + ">"
	}
	return string(data)
}
func (s MapTypeDef) String() string {
	data, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		return "<MapTypeDef: error = " + err.Error() + ">"
	}
	return string(data)
}
func (s StructTypeDef) String() string {
	data, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		return "<StructTypeDef: error = " + err.Error() + ">"
	}
	return string(data)
}
func (s StringTypeDef) String() string {
	data, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		return "<StringTypeDef: error = " + err.Error() + ">"
	}
	return string(data)
}
func (s NumberTypeDef) String() string {
	data, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		return "<NumberTypeDef: error = " + err.Error() + ">"
	}
	return string(data)
}
func (s EnumTypeDef) String() string {
	data, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		return "<EnumTypeDef: error = " + err.Error() + ">"
	}
	return string(data)
}
func (s UnionTypeDef) String() string {
	data, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		return "<UnionTypeDef: error = " + err.Error() + ">"
	}
	return string(data)
}
func (s AliasTypeDef) String() string {
	data, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		return "<AliasTypeDef: error = " + err.Error() + ">"
	}
	return string(data)
}

func (r Resource) String() string {
	data, err := json.MarshalIndent(r, "", "    ")
	if err != nil {
		return "<resource: error = " + err.Error() + ">"
	}
	return string(data)
}

func (f StructFieldDef) String() string {
	data, err := json.MarshalIndent(f, "", "    ")
	if err != nil {
		return "<schema: error = " + err.Error() + ">"
	}
	return string(data)
}

//StatusMessage returns the full message for a symbolic identifier, i.e. "OK" -> "200 OK"
func StatusMessage(sym string) string {
	switch sym {
	case "CONTINUE", "100":
		return "Continue"
	case "SWITCHING_PROTOCOLS", "101":
		return "Switching Protocols"
	case "OK", "200":
		return "OK"
	case "Created", "201":
		return "Created"
	case "ACCEPTED", "202":
		return "Accepted"
	case "NONAUTHORITATIVE_INFORMATION", "203":
		return "Non-Authoritative Information"
	case "NO_CONTENT", "204":
		return "No Content"
	case "RESET_CONTENT", "205":
		return "Reset Content"
	case "PARTIAL_CONTENT", "206":
		return "Partial Content"
	case "MULTIPLE_CHOICES", "300":
		return "Multiple Choices"
	case "MOVED_PERMANENTLY", "301":
		return "Moved Permanently"
	case "FOUND", "302":
		return "Found"
	case "SEE_OTHER", "303":
		return "See Other"
	case "NOT_MODIFIED", "304":
		return "Not Modified"
	case "USE_PROXY", "305":
		return "Use Proxy"
	case "TEMPORARY_REDIRECT", "307":
		return "Temporary Redirect"
	case "BAD_REQUEST", "400":
		return "Bad Request"
	case "UNAUTHORIZED", "401":
		return "Unauthorized"
	case "FORBIDDEN", "403":
		return "Forbidden"
	case "NOT_FOUND", "404":
		return "Not Found"
	case "METHOD_NOT_ALLOWED", "405":
		return "Method Not Allowed"
	case "NOT_ACCEPTABLE", "406":
		return "Not Acceptable"
	case "PROXY_AUTHENTICATION_REQUIRED", "407":
		return "Proxy Authentication Required"
	case "REQUEST_TIMEOUT", "408":
		return "Request Timeout"
	case "CONFLICT", "409":
		return "Conflict"
	case "GONE", "410":
		return "Gone"
	case "LENGTH_REQUIRED", "411":
		return "Length Required"
	case "PRECONDITION_FAILED", "412":
		return "Precondition Failed"
	case "REQUEST_ENTITY_TOO_LARGE", "413":
		return "Request Entity Too Large"
	case "REQUEST_URI_TOO_LONG", "414":
		return "Request-URI Too Long"
	case "UNSUPPORTED_MEDIA_TYPE", "415":
		return "Unsupported Media Type"
	case "REQUEST_RANGE_NOT_SATISFIABLE", "416":
		return "Requested Range Not Satisfiable"
	case "EXPECTATION_FAILED", "417":
		return "Expectation Failed"
	case "UNPROCESSABLE_ENTITY", "422":
		return "Unprocessable Entity"
	case "PRECONDITION_REQUIRED", "428":
		return "Precondition Required"
	case "TOO_MANY_REQUESTS", "429":
		return "Too Many Requests"
	case "REQUEST_HEADER_FIELDS_TOO_LARGE", "431":
		return "Request Header Fields Too Large"
	case "INTERNAL_SERVER_ERROR", "500":
		return "Internal Server Error"
	case "NOT_IMPLEMENTED", "501":
		return "Not Implemented"
	case "BAD_GATEWAY", "502":
		return "Bad Gateway"
	case "SERVICE_UNAVAILABLE", "503":
		return "Service Unavailable"
	case "GATEWAY_TIMEOUT", "504":
		return "Gateway Timeout"
	case "HTTP_VERSION_NOT_SUPPORTED", "505":
		return "HTTP Version Not Supported"
	case "NETWORK_AUTHENTICATION_REQUIRED", "511":
		return "Network Authentication Required"
	default:
		return sym
	}
}

//StatusCode returns the integer status code (as a string) for the symbolic name. I.e. "OK" -> "200"
func StatusCode(sym string) string {
	switch sym {
	case "CONTINUE", "100":
		return "100"
	case "SWITCHING_PROTOCOLS", "101":
		return "101"
	case "OK", "200":
		return "200"
	case "CREATED", "201":
		return "201"
	case "ACCEPTED", "202":
		return "202"
	case "NONAUTHORITATIVE_INFORMATION", "203":
		return "203"
	case "NO_CONTENT", "204":
		return "204"
	case "RESET_CONTENT", "205":
		return "205"
	case "PARTIAL_CONTENT", "206":
		return "206"
	case "MULTIPLE_CHOICES", "300":
		return "300"
	case "MOVED_PERMANENTLY", "301":
		return "301"
	case "FOUND", "302":
		return "302"
	case "SEE_OTHER", "303":
		return "303"
	case "NOT_MODIFIED", "304":
		return "304"
	case "USE_PROXY", "305":
		return "305"
	case "TEMPORARY_REDIRECT", "307":
		return "307"
	case "BAD_REQUEST", "400":
		return "400"
	case "UNAUTHORIZED", "401":
		return "401"
	case "FORBIDDEN", "403":
		return "403"
	case "NOT_FOUND", "404":
		return "404"
	case "METHOD_NOT_ALLOWED", "405":
		return "405"
	case "NOT_ACCEPTABLE", "406":
		return "406"
	case "PROXY_AUTHENTICATION_REQUIRED", "407":
		return "407"
	case "REQUEST_TIMEOUT", "408":
		return "408"
	case "CONFLICT", "409":
		return "409"
	case "GONE", "410":
		return "410"
	case "LENGTH_REQUIRED", "411":
		return "411"
	case "PRECONDITION_FAILED", "412":
		return "412"
	case "REQUEST_ENTITY_TOO_LARGE", "413":
		return "413"
	case "REQUEST_URI_TOO_LONG", "414":
		return "414"
	case "UNSUPPORTED_MEDIA_TYPE", "415":
		return "415"
	case "REQUEST_RANGE_NOT_SATISFIABLE", "416":
		return "416"
	case "EXPECTATION_FAILED", "417":
		return "417"
	case "UNPROCESSABLE_ENTITY", "422":
		return "422"
	case "PRECONDITION_REQUIRED", "428":
		return "428"
	case "TOO_MANY_REQUESTS", "429":
		return "429"
	case "REQUEST_HEADER_FIELDS_TOO_LARGE", "431":
		return "431"
	case "INTERNAL_SERVER_ERROR", "500":
		return "500"
	case "NOT_IMPLEMENTED", "501":
		return "501"
	case "BAD_GATEWAY", "502":
		return "502"
	case "SERVICE_UNAVAILABLE", "503":
		return "503"
	case "GATEWAY_TIMEOUT", "504":
		return "504"
	case "HTTP_VERSION_NOT_SUPPORTED", "505":
		return "505"
	case "NETWORK_AUTHENTICATION_REQUIRED", "511":
		return "511"
	default:
		return sym
	}
}

func outputWriter(outdir string, name string, ext string) (*bufio.Writer, *os.File, string, error) {
	sname := "anonymous"
	if strings.HasSuffix(outdir, ext) {
		name = filepath.Base(outdir)
		sname = name[:len(name)-len(ext)]
		outdir = filepath.Dir(outdir)
	}
	if name != "" {
		sname = name
	}
	if outdir == "" {
		return bufio.NewWriter(os.Stdout), nil, sname, nil
	}
	outfile := sname
	if !strings.HasSuffix(outfile, ext) {
		outfile += ext
	}
	path := filepath.Join(outdir, outfile)
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, "", err
	}
	writer := bufio.NewWriter(f)
	return writer, f, sname, nil
}

// ExportToJSON - v2 schema export to JSON. If out is empty, dump to stdout.
func ExportToJSON(schema *Schema, outpath string) error {
	out, file, _, err := outputWriter(outpath, string(schema.Name), ".json")
	if err != nil {
		return err
	}
	err = WriteJSON(schema, out)
	if file != nil {
		file.Close()
	}
	return err
}

// WriteJSON - v2 schema export to JSON
func WriteJSON(schema *Schema, out *bufio.Writer) error {
	j, err := json.MarshalIndent(schema, "", "    ")
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "%s", j)
	out.Flush()
	return nil
}

func Pretty(obj interface{}) string {
	j, err := json.MarshalIndent(obj, "", "    ")
	if err != nil {
		return fmt.Sprint(obj)
	}
	return string(j)
}

func CompareSchemas(s1 *Schema, s2 *Schema) string {
	if s1.Namespace != s2.Namespace {
		return fmt.Sprintf("Namespaces differ: %q vs %q", s1.Namespace, s2.Namespace)
	}
	if s1.Name != s2.Name {
		return fmt.Sprintf("Names differ: %q vs %q", s1.Name, s2.Name)
	}
	if s1.Version != nil {
		if s2.Version != nil {
			if *s1.Version != *s2.Version {
				return fmt.Sprintf("Versions differ: %d vs %d", *s1.Version, *s2.Version)
			}
		} else {
			return fmt.Sprintf("Versions differ: %d vs nil", *s1.Version)
		}
	} else {
		if s2.Version != nil {
			return fmt.Sprintf("Versions differ: nil vs %d", *s2.Version)
		}
	}
	if s1.Comment != s2.Comment {
		return fmt.Sprintf("Comments differ: %q vs %q", s1.Comment, s2.Comment)
	}
	if len(s1.Types) != len(s2.Types) {
		return fmt.Sprintf("Number of Types differ: %d vs %d", len(s1.Types), len(s2.Types))
	}
	for i := 0; i < len(s1.Types); i++ {
		s := compareTypes(s1.Types[i], s2.Types[i])
		if s != "" {
			return s
		}
	}
	if len(s1.Resources) != len(s2.Resources) {
		return fmt.Sprintf("Number of Resources differ: %d vs %d", len(s1.Resources), len(s2.Resources))
	}
	for i := 0; i < len(s1.Resources); i++ {
		s := compareResources(s1.Resources[i], s2.Resources[i])
		if s != "" {
			return s
		}
	}
	return ""
}

func compareTypes(t1 *Type, t2 *Type) string {
	if t1.Variant != t2.Variant {
		return fmt.Sprintf("Type variants differ: %v vs %v", t1.Variant, t2.Variant)
	}
	switch t1.Variant {
	case TypeVariantBaseType:
		if t1.BaseType != t2.BaseType {
			return fmt.Sprintf("Types differ: %v vs %v", t1.BaseType, t2.BaseType)
		}
		return ""
	case TypeVariantStructTypeDef:
		return compareStructTypes(t1.StructTypeDef, t2.StructTypeDef)
	case TypeVariantMapTypeDef:
		return compareMapTypes(t1.MapTypeDef, t2.MapTypeDef)
	case TypeVariantArrayTypeDef:
		return compareArrayTypes(t1.ArrayTypeDef, t2.ArrayTypeDef)
	case TypeVariantEnumTypeDef:
		return compareEnumTypes(t1.EnumTypeDef, t2.EnumTypeDef)
	case TypeVariantUnionTypeDef:
		return compareUnionTypes(t1.UnionTypeDef, t2.UnionTypeDef)
	case TypeVariantStringTypeDef:
		return compareStringTypes(t1.StringTypeDef, t2.StringTypeDef)
	case TypeVariantBytesTypeDef:
		return compareBytesTypes(t1.BytesTypeDef, t2.BytesTypeDef)
	case TypeVariantNumberTypeDef:
		return compareNumberTypes(t1.NumberTypeDef, t2.NumberTypeDef)
	case TypeVariantAliasTypeDef:
		return compareAliasTypes(t1.AliasTypeDef, t2.AliasTypeDef)
	}
	return "" //never happens
}

func compareAnnotations(name TypeName, a1 map[ExtendedAnnotation]string, a2 map[ExtendedAnnotation]string) string {
	if a1 == nil && a2 == nil {
		return ""
	}
	if a1 == nil {
		return fmt.Sprintf("Type annotations of %s differ: nil vs %v", name, a2)
	}
	if a2 == nil {
		return fmt.Sprintf("Type annotations of %s differ: %v vs nil", name, a1)
	}
	if len(a1) != len(a2) {
		return fmt.Sprintf("Type annotations of %s differ in length: %d vs %d", name, len(a1), len(a2))
	}
	for k, v1 := range a1 {
		if v2, ok := a2[k]; !ok {
			return fmt.Sprintf("Type annotations of %s differ: %q", name, k)
		} else if v1 != v2 {
			return fmt.Sprintf("Type annotations of %s differ: %q = %q vs %q", name, k, v1, v2)
		}
	}
	return ""
}

func compareNumbers(name TypeName, n1 *Number, n2 *Number) string {
	fail := false
	if n1 == nil || n2 == nil {
		fail = true
	} else if n1.Variant != n2.Variant {
		fail = true
	} else {
		switch n1.Variant {
		case NumberVariantInt8:
			if n1.Int8 != n2.Int8 {
				fail = true
			}
		case NumberVariantInt16:
			if n1.Int16 != n2.Int16 {
				fail = true
			}
		case NumberVariantInt32:
			if n1.Int32 != n2.Int32 {
				fail = true
			}
		case NumberVariantInt64:
			if n1.Int64 != n2.Int64 {
				fail = true
			}
		case NumberVariantFloat32:
			if n1.Float32 != n2.Float32 {
				fail = true
			}
		case NumberVariantFloat64:
			if n1.Float64 != n2.Float64 {
				fail = true
			}
		}
	}
	if fail {
		return fmt.Sprintf("Type %s values differ: %v vs %v", name, n1, n2)
	}
	return ""
}

func compareStringTypes(t1 *StringTypeDef, t2 *StringTypeDef) string {
	if t1.Name != t2.Name {
		return fmt.Sprintf("Types differ: %v vs %v", t1.Name, t2.Name)
	}
	if t1.Type != t2.Type {
		return fmt.Sprintf("Type supertypes of %s differ: %v vs %v", t1.Name, t1.Type, t2.Type)
	}
	if t1.Comment != t2.Comment {
		return fmt.Sprintf("Type comments of %s differ: %v vs %v", t1.Name, t1.Comment, t2.Comment)
	}
	if t1.Pattern != t2.Pattern {
		return fmt.Sprintf("Type patterns of %s differ: %v vs %v", t1.Name, t1.Pattern, t2.Pattern)
	}
	if t1.MinSize != t2.MinSize {
		if t1.MinSize == nil || t2.MinSize == nil {
			return fmt.Sprintf("Type MinSize differ: %v vs %v", t1, t2)
		}
	}
	if t1.MaxSize != t2.MaxSize {
		if t1.MaxSize == nil || t2.MaxSize == nil {
			return fmt.Sprintf("Type MaxSize differ: %v vs %v", t1, t2)
		}
	}
	if len(t1.Values) != len(t2.Values) {
		return fmt.Sprintf("Type values of %s differ in length: %v vs %v", t1.Name, len(t1.Values), len(t2.Values))
	}
	for i := 0; i < len(t1.Values); i++ {
		if t1.Values[i] != t2.Values[i] {
			return fmt.Sprintf("Type values of %s differ: %q vs %q", t1.Name, t1.Values[i], t2.Values[i])
		}
	}
	return compareAnnotations(t1.Name, t1.Annotations, t2.Annotations)
}

func compareBytesTypes(t1 *BytesTypeDef, t2 *BytesTypeDef) string {
	if t1.Name != t2.Name {
		return fmt.Sprintf("Types differ: %v vs %v", t1.Name, t2.Name)
	}
	if t1.Type != t2.Type {
		return fmt.Sprintf("Type supertypes of %s differ: %v vs %v", t1.Name, t1.Type, t2.Type)
	}
	if t1.Comment != t2.Comment {
		return fmt.Sprintf("Type comments of %s differ: %v vs %v", t1.Name, t1.Comment, t2.Comment)
	}
	if t1.Size != t2.Size {
		if t1.Size == nil || t2.Size == nil {
			return fmt.Sprintf("Type Size differs: %v vs %v", t1, t2)
		}
		if *t1.Size != *t2.Size {
			return fmt.Sprintf("Type Size differs: %v vs %v", t1, t2)
		}
	}
	if t1.MinSize != t2.MinSize {
		if t1.MinSize == nil || t2.MinSize == nil {
			return fmt.Sprintf("Type MinSize differ: %v vs %v", t1, t2)
		}
		if *t1.MinSize != *t2.MinSize {
			return fmt.Sprintf("Type MinSize differs: %v vs %v", t1, t2)
		}
	}
	if t1.MaxSize != t2.MaxSize {
		if t1.MaxSize == nil || t2.MaxSize == nil {
			return fmt.Sprintf("Type MaxSize differ: %v vs %v", t1, t2)
		}
		if *t1.MaxSize != *t2.MaxSize {
			return fmt.Sprintf("Type MaxSize differs: %v vs %v", t1, t2)
		}
	}
	return compareAnnotations(t1.Name, t1.Annotations, t2.Annotations)
}

func compareNumberTypes(t1 *NumberTypeDef, t2 *NumberTypeDef) string {
	if t1.Name != t2.Name {
		return fmt.Sprintf("Types differ: %v vs %v", t1.Name, t2.Name)
	}
	if t1.Type != t2.Type {
		return fmt.Sprintf("Type supertypes of %s differ: %v vs %v", t1.Name, t1.Type, t2.Type)
	}
	if t1.Comment != t2.Comment {
		return fmt.Sprintf("Type comments of %s differ: %v vs %v", t1.Name, t1.Comment, t2.Comment)
	}
	if t1.Min != t2.Min {
		s := compareNumbers(TypeName(fmt.Sprintf("%s.Min", t1.Name)), t1.Min, t2.Min)
		if s != "" {
			return s
		}
	}
	if t1.Max != t2.Max {
		s := compareNumbers(TypeName(fmt.Sprintf("%s.Max", t1.Name)), t1.Max, t2.Max)
		if s != "" {
			return s
		}
	}
	return compareAnnotations(t1.Name, t1.Annotations, t2.Annotations)
}

func compareArrayTypes(t1 *ArrayTypeDef, t2 *ArrayTypeDef) string {
	if t1.Name != t2.Name {
		return fmt.Sprintf("Types differ: %v vs %v", t1.Name, t2.Name)
	}
	if t1.Type != t2.Type {
		return fmt.Sprintf("Type supertypes of %s differ: %v vs %v", t1.Name, t1.Type, t2.Type)
	}
	if t1.Comment != t2.Comment {
		return fmt.Sprintf("Type comments of %s differ: %v vs %v", t1.Name, t1.Comment, t2.Comment)
	}
	if t1.Size != t2.Size {
		if t1.Size == nil || t2.Size == nil {
			return fmt.Sprintf("Type Size differs: %v vs %v", t1, t2)
		}
		if *t1.Size != *t2.Size {
			return fmt.Sprintf("Type Size differs: %v vs %v", t1, t2)
		}
	}
	if t1.MinSize != t2.MinSize {
		if t1.MinSize == nil || t2.MinSize == nil {
			return fmt.Sprintf("Type MinSize differ: %v vs %v", t1, t2)
		}
		if *t1.MinSize != *t2.MinSize {
			return fmt.Sprintf("Type MinSize differs: %v vs %v", t1, t2)
		}
	}
	if t1.MaxSize != t2.MaxSize {
		if t1.MaxSize == nil || t2.MaxSize == nil {
			return fmt.Sprintf("Type MaxSize differ: %v vs %v", t1, t2)
		}
		if *t1.MaxSize != *t2.MaxSize {
			return fmt.Sprintf("Type MaxSize differs: %v vs %v", t1, t2)
		}
	}
	if t1.Items != t2.Items {
		return fmt.Sprintf("Type Items differs: %v vs %v", t1, t2)
	}
	return compareAnnotations(t1.Name, t1.Annotations, t2.Annotations)
}

func compareMapTypes(t1 *MapTypeDef, t2 *MapTypeDef) string {
	if t1.Name != t2.Name {
		return fmt.Sprintf("Types differ: %v vs %v", t1.Name, t2.Name)
	}
	if t1.Type != t2.Type {
		return fmt.Sprintf("Type supertypes of %s differ: %v vs %v", t1.Name, t1.Type, t2.Type)
	}
	if t1.Comment != t2.Comment {
		return fmt.Sprintf("Type comments of %s differ: %v vs %v", t1.Name, t1.Comment, t2.Comment)
	}
	if t1.Size != t2.Size {
		if t1.Size == nil || t2.Size == nil {
			return fmt.Sprintf("Type Size differs: %v vs %v", t1, t2)
		}
		if *t1.Size != *t2.Size {
			return fmt.Sprintf("Type Size differs: %v vs %v", t1, t2)
		}
	}
	if t1.MinSize != t2.MinSize {
		if t1.MinSize == nil || t2.MinSize == nil {
			return fmt.Sprintf("Type MinSize differ: %v vs %v", t1, t2)
		}
		if *t1.MinSize != *t2.MinSize {
			return fmt.Sprintf("Type MinSize differs: %v vs %v", t1, t2)
		}
	}
	if t1.MaxSize != t2.MaxSize {
		if t1.MaxSize == nil || t2.MaxSize == nil {
			return fmt.Sprintf("Type MaxSize differ: %v vs %v", t1, t2)
		}
		if *t1.MaxSize != *t2.MaxSize {
			return fmt.Sprintf("Type MaxSize differs: %v vs %v", t1, t2)
		}
	}
	if t1.Keys != t2.Keys {
		return fmt.Sprintf("Type Keys differs: %v vs %v", t1, t2)
	}
	if t1.Items != t2.Items {
		return fmt.Sprintf("Type Items differs: %v vs %v", t1, t2)
	}
	return compareAnnotations(t1.Name, t1.Annotations, t2.Annotations)
}

func compareUnionTypes(t1 *UnionTypeDef, t2 *UnionTypeDef) string {
	if t1.Name != t2.Name {
		return fmt.Sprintf("Types differ: %v vs %v", t1.Name, t2.Name)
	}
	if t1.Type != t2.Type {
		return fmt.Sprintf("Type supertypes of %s differ: %v vs %v", t1.Name, t1.Type, t2.Type)
	}
	if t1.Comment != t2.Comment {
		return fmt.Sprintf("Type comments of %s differ: %v vs %v", t1.Name, t1.Comment, t2.Comment)
	}
	if len(t1.Variants) != len(t2.Variants) {
		return fmt.Sprintf("Union types (%s) have different number of variants: %v vs %v", t1.Name, len(t1.Variants), len(t2.Variants))
	}
	for i := 0; i < len(t1.Variants); i++ {
		if t1.Variants[i] != t2.Variants[i] {
			return fmt.Sprintf("Union types (%s) have variant mismatch: %v vs %v", t1.Name, t1.Variants[i], t2.Variants[i])
		}
	}
	return compareAnnotations(t1.Name, t1.Annotations, t2.Annotations)
}

func compareEnumTypes(t1 *EnumTypeDef, t2 *EnumTypeDef) string {
	if t1.Name != t2.Name {
		return fmt.Sprintf("Types differ: %v vs %v", t1.Name, t2.Name)
	}
	if t1.Type != t2.Type {
		return fmt.Sprintf("Type supertypes of %s differ: %v vs %v", t1.Name, t1.Type, t2.Type)
	}
	if t1.Comment != t2.Comment {
		return fmt.Sprintf("Type comments of %s differ: %v vs %v", t1.Name, t1.Comment, t2.Comment)
	}
	if len(t1.Elements) != len(t2.Elements) {
		return fmt.Sprintf("Enum types (%s) have different number of elements: %v vs %v", t1.Name, len(t1.Elements), len(t2.Elements))
	}
	for i := 0; i < len(t1.Elements); i++ {
		if t1.Elements[i].Symbol != t2.Elements[i].Symbol {
			return fmt.Sprintf("Enum types (%s) have element mismatch: %v vs %v", t1.Name, t1.Elements[i].Symbol, t2.Elements[i].Symbol)
		}
		if t1.Elements[i].Comment != t2.Elements[i].Comment {
			return fmt.Sprintf("Enum types (%s) have element comment mismatch: %v vs %v", t1.Name, t1.Elements[i].Comment, t2.Elements[i].Comment)
		}
	}
	return compareAnnotations(t1.Name, t1.Annotations, t2.Annotations)
}

func compareStructTypes(t1 *StructTypeDef, t2 *StructTypeDef) string {
	if t1.Name != t2.Name {
		return fmt.Sprintf("Types differ: %v vs %v", t1.Name, t2.Name)
	}
	if t1.Type != t2.Type {
		return fmt.Sprintf("Type supertypes of %s differ: %v vs %v", t1.Name, t1.Type, t2.Type)
	}
	if t1.Comment != t2.Comment {
		return fmt.Sprintf("Type comments of %s differ: %v vs %v", t1.Name, t1.Comment, t2.Comment)
	}
	if t1.Closed != t2.Closed {
		return fmt.Sprintf("Struct Closed fields differ: %v vs %v", t1, t2)
	}
	if len(t1.Fields) != len(t2.Fields) {
		return fmt.Sprintf("Struct types (%s) have different number of fields: %v vs %v", t1.Name, len(t1.Fields), len(t2.Fields))
	}
	for i := 0; i < len(t1.Fields); i++ {
		s := compareField(t1.Name, t1.Fields[i], t2.Fields[i])
		if s != "" {
			return s
		}
	}
	return compareAnnotations(t1.Name, t1.Annotations, t2.Annotations)
}

func compareField(tname TypeName, f1 *StructFieldDef, f2 *StructFieldDef) string {
	if f1.Name != f2.Name {
		return fmt.Sprintf("Struct fields differ: %q vs %q", f1.Name, f2.Name)
	}
	if f1.Type != f2.Type {
		return fmt.Sprintf("Struct field (%s) types differ: %q vs %q", f1.Name, f1.Type, f2.Type)
	}
	if f1.Optional != f2.Optional {
		return fmt.Sprintf("Struct field (%s) optional flags differ: %v vs %v", f1.Name, f1.Optional, f2.Optional)
	}
	if f1.Default != f2.Default { //!!
		return fmt.Sprintf("Struct field (%s) optional defaults differ: %v vs %v", f1.Name, f1.Default, f2.Default)
	}
	if f1.Keys != f2.Keys {
		return fmt.Sprintf("Struct field (%s) Keys differs: %v vs %v", tname, f1, f2)
	}
	if f1.Items != f2.Items {
		return fmt.Sprintf("Struct field (%s) Items differs: %v vs %v", tname, f1, f2)
	}
	return compareAnnotations(TypeName(fmt.Sprintf("%s.%s", tname, f1.Name)), f1.Annotations, f2.Annotations)
}

func compareAliasTypes(t1 *AliasTypeDef, t2 *AliasTypeDef) string {
	if t1.Name != t2.Name {
		return fmt.Sprintf("Types differ: %v vs %v", t1.Name, t2.Name)
	}
	if t1.Type != t2.Type {
		return fmt.Sprintf("Type supertypes of %s differ: %v vs %v", t1.Name, t1.Type, t2.Type)
	}
	if t1.Comment != t2.Comment {
		return fmt.Sprintf("Type comments of %s differ: %v vs %v", t1.Name, t1.Comment, t2.Comment)
	}
	return compareAnnotations(t1.Name, t1.Annotations, t2.Annotations)
}

func compareResources(r1 *Resource, r2 *Resource) string {
	if r1.Type != r2.Type {
		return fmt.Sprintf("Resource types differ: %q vs %q", r1.Type, r2.Type)
	}
	if r1.Method != r2.Method {
		return fmt.Sprintf("Resource methods differ: %v vs %v", r1, r2)
	}
	if r1.Path != r2.Path {
		return fmt.Sprintf("Resource paths differ: %v vs %v", r1, r2)
	}
	//fix me
	return ""
}
