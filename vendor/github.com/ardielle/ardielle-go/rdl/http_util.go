// Copyright 2015 Yahoo Inc.
// Licensed under the terms of the Apache version 2.0 license. See LICENSE file for terms.

package rdl

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// for client/server generated code support

// ResourceContext is the context for a handler callout, in case access to the underlying request information is needed.
// Because explicit arguments are declared in the API with RDL (include path, query, and header params), the
// need to access this is rare.
// Note that the map of name to value is not protected for concurrency - the app must do this itself if it plans
// on sharing ResourceContext across threads.
//
type ResourceContext struct {
	Writer    http.ResponseWriter
	Request   *http.Request
	Params    map[string]string
	Principal Principal
	Values    map[string]interface{}
}

//
// Get - returns an application data value on the context.
//
func (ctx *ResourceContext) Get(name string) interface{} {
	if ctx.Values != nil {
		if v, ok := ctx.Values[name]; ok {
			return v
		}
	}
	return nil
}

//
// Put - associates an application datum to the given name in the context.
// This call is not thread-safe, the app must arrange to allow multi-threaded access.
//
func (ctx *ResourceContext) Put(name string, value interface{}) *ResourceContext {
	if ctx.Values == nil {
		ctx.Values = make(map[string]interface{})
	}
	ctx.Values[name] = value
	return ctx
}

//
// ResourceError is the generic container for service errors.
//
type ResourceError struct {
	Code    int    `json:"code"`    //the http status code
	Message string `json:"message"` //a human readable message

}

func (e ResourceError) StatusCode() int {
	return e.Code
}

func (e ResourceError) Error() string {
	return fmt.Sprintf("%d %s", e.Code, e.Message)
}

// JSONResponse provides response encoded as JSON.
func JSONResponse(w http.ResponseWriter, code int, data interface{}) {
	w.Header()["Content-Type"] = []string{"application/json"}
	w.WriteHeader(code)
	switch code {
	case 204, 304:
		/* no body */
	default:
		if data == nil {
			data = ResourceError{code, "Server Error"}
		}
		b, e := json.MarshalIndent(data, "", "  ")
		if e != nil {
			code = http.StatusInternalServerError
			b, _ = json.MarshalIndent(ResourceError{500, "Server Error"}, "", "  ")
		}
		fmt.Fprintf(w, "%s\n", string(b))
	}
}

// OptionalStringParam parses and returns an optional parameter
// from the form body (multipart/form-data encoded).
func OptionalStringParam(r *http.Request, name string) string {
	if r.Form == nil {
		r.ParseMultipartForm(32 << 20)
	}
	if vs := r.Form[name]; len(vs) == 1 {
		return vs[0]
	}
	return ""
}

func StringParam(r *http.Request, name string, defaultValue string) (string, bool) {
	s := OptionalStringParam(r, name)
	if s == "" {
		if defaultValue == "" {
			return "", false
		}
		s = defaultValue
	}
	return s, true
}

func OptionalInt32Param(r *http.Request, name string) (*int32, error) {
	if r.Form == nil {
		r.ParseMultipartForm(32 << 20)
	}
	if vs := r.Form[name]; len(vs) == 1 {
		if i, err := strconv.ParseInt(vs[0], 10, 32); err == nil {
			n := int32(i)
			return &n, nil
		}
		return nil, &ResourceError{400, "Parameter '" + name + "' is not an Int32: " + vs[0]}
	}
	return nil, nil
}

func Int32Param(r *http.Request, name string, defaultValue int32) (int32, error) {
	pi, err := OptionalInt32Param(r, name)
	if err != nil {
		return 0, err
	}
	if pi != nil {
		return *pi, nil
	}
	return defaultValue, nil
}

func OptionalInt64Param(r *http.Request, name string) (*int64, error) {
	if r.Form == nil {
		r.ParseMultipartForm(32 << 20)
	}
	if vs := r.Form[name]; len(vs) == 1 {
		if i, err := strconv.ParseInt(vs[0], 10, 64); err == nil {
			return &i, nil
		}
		return nil, &ResourceError{400, "Parameter '" + name + "' is not an Int64: " + vs[0]}
	}
	return nil, nil
}

func Int64Param(r *http.Request, name string, defaultValue int64) (int64, error) {
	pi, err := OptionalInt64Param(r, name)
	if err != nil {
		return 0, err
	}
	if pi != nil {
		return *pi, nil
	}
	return defaultValue, nil
}

func OptionalFloat64Param(r *http.Request, name string) (*float64, error) {
	if r.Form == nil {
		r.ParseMultipartForm(32 << 20)
	}
	if vs := r.Form[name]; len(vs) == 1 {
		if i, err := strconv.ParseFloat(vs[0], 64); err == nil {
			return &i, nil
		}
		return nil, &ResourceError{400, "Parameter '" + name + "' is not an Float64: " + vs[0]}
	}
	return nil, nil
}

func Float64Param(r *http.Request, name string, defaultValue float64) (float64, error) {
	pi, err := OptionalFloat64Param(r, name)
	if err != nil {
		return 0, err
	}
	if pi != nil {
		return *pi, nil
	}
	return defaultValue, nil
}
func Float32Param(r *http.Request, name string, defaultValue float32) (float32, error) {
	pi, err := OptionalFloat64Param(r, name)
	if err != nil {
		return 0, err
	}
	if pi != nil {
		return float32(*pi), nil
	}
	return defaultValue, nil
}

func OptionalBoolParam(r *http.Request, name string) (*bool, error) {
	var b bool
	if r.Form == nil {
		r.ParseMultipartForm(32 << 20)
	}
	if vs := r.Form[name]; len(vs) == 1 {
		switch vs[0] {
		case "true":
			b = true
		case "false":
			b = false
		default:
			return nil, &ResourceError{400, "Parameter '" + name + "' is not a Bool: " + vs[0]}
		}
	}
	return &b, nil
}

func BoolParam(r *http.Request, name string, defaultValue bool) (bool, error) {
	pb, err := OptionalBoolParam(r, name)
	if err != nil {
		return false, err
	}
	if pb != nil {
		return *pb, nil
	}
	return defaultValue, nil
}

func OptionalHeaderParam(r *http.Request, name string) string {
	if tmp, ok := r.Header[name]; ok {
		s := strings.Join(tmp, ",")
		return s
	}
	return ""
}

func HeaderParam(r *http.Request, name string, defaultValue string) string {
	val := OptionalHeaderParam(r, name)
	if val == "" {
		val = defaultValue
	}
	return val
}

// FoldHttpHeaderName adapts to the Go misfeature: all headers are
// canonicalized as Capslike-This (for a header "CapsLike-this").
func FoldHttpHeaderName(name string) string {
	return http.CanonicalHeaderKey(name)
}
