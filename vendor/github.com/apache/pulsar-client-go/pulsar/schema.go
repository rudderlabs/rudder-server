// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pulsar

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/maphash"
	"reflect"
	"unsafe"

	log "github.com/sirupsen/logrus"

	"github.com/linkedin/goavro/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

type SchemaType int

const (
	NONE        SchemaType = iota //No schema defined
	STRING                        //Simple String encoding with UTF-8
	JSON                          //JSON object encoding and validation
	PROTOBUF                      //Protobuf message encoding and decoding
	AVRO                          //Serialize and deserialize via Avro
	BOOLEAN                       //
	INT8                          //A 8-byte integer.
	INT16                         //A 16-byte integer.
	INT32                         //A 32-byte integer.
	INT64                         //A 64-byte integer.
	FLOAT                         //A float number.
	DOUBLE                        //A double number
	_                             //
	_                             //
	_                             //
	KeyValue                      //A Schema that contains Key Schema and Value Schema.
	BYTES       = -1              //A bytes array.
	AUTO        = -2              //
	AutoConsume = -3              //Auto Consume Type.
	AutoPublish = -4              // Auto Publish Type.
	ProtoNative = 20              //Protobuf native message encoding and decoding
)

// Encapsulates data around the schema definition
type SchemaInfo struct {
	Name       string
	Schema     string
	Type       SchemaType
	Properties map[string]string
}

func (s SchemaInfo) hash() uint64 {
	h := maphash.Hash{}
	h.SetSeed(seed)
	h.Write([]byte(s.Schema))
	return h.Sum64()
}

type Schema interface {
	Encode(v interface{}) ([]byte, error)
	Decode(data []byte, v interface{}) error
	Validate(message []byte) error
	GetSchemaInfo() *SchemaInfo
}

func NewSchema(schemaType SchemaType, schemaData []byte, properties map[string]string) (schema Schema, err error) {
	var schemaDef = string(schemaData)
	var s Schema
	switch schemaType {
	case STRING:
		s = NewStringSchema(properties)
	case JSON:
		s = NewJSONSchema(schemaDef, properties)
	case PROTOBUF:
		s = NewProtoSchema(schemaDef, properties)
	case AVRO:
		s = NewAvroSchema(schemaDef, properties)
	case INT8:
		s = NewInt8Schema(properties)
	case INT16:
		s = NewInt16Schema(properties)
	case INT32:
		s = NewInt32Schema(properties)
	case INT64:
		s = NewInt64Schema(properties)
	case FLOAT:
		s = NewFloatSchema(properties)
	case DOUBLE:
		s = NewDoubleSchema(properties)
	case ProtoNative:
		s = newProtoNativeSchema(schemaDef, properties)
	default:
		err = fmt.Errorf("not support schema type of %v", schemaType)
	}
	schema = s
	return
}

type AvroCodec struct {
	Codec *goavro.Codec
}

func NewSchemaDefinition(schema *goavro.Codec) *AvroCodec {
	schemaDef := &AvroCodec{
		Codec: schema,
	}
	return schemaDef
}

// initAvroCodec returns a Codec used to translate between a byte slice of either
// binary or textual Avro data and native Go data.
func initAvroCodec(codec string) (*goavro.Codec, error) {
	return goavro.NewCodec(codec)
}

type JSONSchema struct {
	AvroCodec
	SchemaInfo
}

// NewJSONSchema creates a new JSONSchema
// Note: the function will panic if creation of codec fails
func NewJSONSchema(jsonAvroSchemaDef string, properties map[string]string) *JSONSchema {
	js, err := NewJSONSchemaWithValidation(jsonAvroSchemaDef, properties)
	if err != nil {
		log.Fatalf("JSONSchema init codec error:%v", err)
	}
	return js
}

// NewJSONSchemaWithValidation creates a new JSONSchema and error to indicate codec failure
func NewJSONSchemaWithValidation(jsonAvroSchemaDef string, properties map[string]string) (*JSONSchema, error) {
	js := new(JSONSchema)
	avroCodec, err := initAvroCodec(jsonAvroSchemaDef)
	if err != nil {
		return nil, err
	}
	schemaDef := NewSchemaDefinition(avroCodec)
	js.SchemaInfo.Schema = schemaDef.Codec.Schema()
	js.SchemaInfo.Type = JSON
	js.SchemaInfo.Properties = properties
	js.SchemaInfo.Name = "JSON"
	return js, nil
}

func (js *JSONSchema) Encode(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

func (js *JSONSchema) Decode(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (js *JSONSchema) Validate(message []byte) error {
	return js.Decode(message, nil)
}

func (js *JSONSchema) GetSchemaInfo() *SchemaInfo {
	return &js.SchemaInfo
}

type ProtoSchema struct {
	AvroCodec
	SchemaInfo
}

var seed = maphash.MakeSeed()

// NewProtoSchema creates a new ProtoSchema
// Note: the function will panic if creation of codec fails
func NewProtoSchema(protoAvroSchemaDef string, properties map[string]string) *ProtoSchema {
	ps, err := NewProtoSchemaWithValidation(protoAvroSchemaDef, properties)
	if err != nil {
		log.Fatalf("ProtoSchema init codec error:%v", err)
	}
	return ps
}

// NewProtoSchemaWithValidation creates a new ProtoSchema and error to indicate codec failure
func NewProtoSchemaWithValidation(protoAvroSchemaDef string, properties map[string]string) (*ProtoSchema, error) {
	ps := new(ProtoSchema)
	avroCodec, err := initAvroCodec(protoAvroSchemaDef)
	if err != nil {
		return nil, err
	}
	schemaDef := NewSchemaDefinition(avroCodec)
	ps.AvroCodec.Codec = schemaDef.Codec
	ps.SchemaInfo.Schema = schemaDef.Codec.Schema()
	ps.SchemaInfo.Type = PROTOBUF
	ps.SchemaInfo.Properties = properties
	ps.SchemaInfo.Name = "Proto"
	return ps, nil
}

func (ps *ProtoSchema) Encode(data interface{}) ([]byte, error) {
	return proto.Marshal(data.(proto.Message))
}

func (ps *ProtoSchema) Decode(data []byte, v interface{}) error {
	return proto.Unmarshal(data, v.(proto.Message))
}

func (ps *ProtoSchema) Validate(message []byte) error {
	return ps.Decode(message, nil)
}

func (ps *ProtoSchema) GetSchemaInfo() *SchemaInfo {
	return &ps.SchemaInfo
}

type ProtoNativeSchema struct {
	SchemaInfo
}

func NewProtoNativeSchemaWithMessage(message proto.Message, properties map[string]string) *ProtoNativeSchema {
	schemaDef, err := getProtoNativeSchemaInfo(message)
	if err != nil {
		log.Fatalf("Get ProtoNative schema info error:%v", err)
	}
	return newProtoNativeSchema(schemaDef, properties)
}

func newProtoNativeSchema(protoNativeSchemaDef string, properties map[string]string) *ProtoNativeSchema {
	pns := new(ProtoNativeSchema)
	pns.SchemaInfo.Schema = protoNativeSchemaDef
	pns.SchemaInfo.Type = ProtoNative
	pns.SchemaInfo.Properties = properties
	pns.SchemaInfo.Name = "ProtoNative"
	return pns
}

func getProtoNativeSchemaInfo(message proto.Message) (string, error) {
	fileDesc := message.ProtoReflect().Descriptor().ParentFile()
	fileProtoMap := make(map[string]*descriptorpb.FileDescriptorProto)
	getFileProto(fileDesc, fileProtoMap)

	fileDescList := make([]*descriptorpb.FileDescriptorProto, 0, len(fileProtoMap))
	for _, v := range fileProtoMap {
		fileDescList = append(fileDescList, v)
	}
	fileDescSet := descriptorpb.FileDescriptorSet{
		File: fileDescList,
	}
	bytesData, err := proto.Marshal(&fileDescSet)
	if err != nil {
		return "", err
	}
	schemaData := ProtoNativeSchemaData{
		FileDescriptorSet:      bytesData,
		RootMessageTypeName:    string(message.ProtoReflect().Descriptor().FullName()),
		RootFileDescriptorName: fileDesc.Path(),
	}
	jsonData, err := json.Marshal(schemaData)
	if err != nil {
		return "", err
	}
	return string(jsonData), nil
}

type ProtoNativeSchemaData struct {
	FileDescriptorSet      []byte `json:"fileDescriptorSet"`
	RootMessageTypeName    string `json:"rootMessageTypeName"`
	RootFileDescriptorName string `json:"rootFileDescriptorName"`
}

func getFileProto(fileDesc protoreflect.FileDescriptor, protoMap map[string]*descriptorpb.FileDescriptorProto) {
	for i := 0; i < fileDesc.Imports().Len(); i++ {
		getFileProto(fileDesc.Imports().Get(i).ParentFile(), protoMap)
	}
	protoMap[fileDesc.Path()] = protodesc.ToFileDescriptorProto(fileDesc)
}

func (ps *ProtoNativeSchema) Encode(data interface{}) ([]byte, error) {
	return proto.Marshal(data.(proto.Message))
}

func (ps *ProtoNativeSchema) Decode(data []byte, v interface{}) error {
	return proto.Unmarshal(data, v.(proto.Message))
}

func (ps *ProtoNativeSchema) Validate(message []byte) error {
	return ps.Decode(message, nil)
}

func (ps *ProtoNativeSchema) GetSchemaInfo() *SchemaInfo {
	return &ps.SchemaInfo
}

type AvroSchema struct {
	AvroCodec
	SchemaInfo
}

// NewAvroSchema creates a new AvroSchema
// Note: the function will panic if creation of codec fails
func NewAvroSchema(avroSchemaDef string, properties map[string]string) *AvroSchema {
	ps, err := NewAvroSchemaWithValidation(avroSchemaDef, properties)
	if err != nil {
		log.Fatalf("AvroSchema init codec error:%v", err)
	}
	return ps
}

// NewAvroSchemaWithValidation creates a new AvroSchema and error to indicate codec failure
func NewAvroSchemaWithValidation(avroSchemaDef string, properties map[string]string) (*AvroSchema, error) {
	as := new(AvroSchema)
	avroCodec, err := initAvroCodec(avroSchemaDef)
	if err != nil {
		return nil, err
	}
	schemaDef := NewSchemaDefinition(avroCodec)
	as.AvroCodec.Codec = schemaDef.Codec
	as.SchemaInfo.Schema = schemaDef.Codec.Schema()
	as.SchemaInfo.Type = AVRO
	as.SchemaInfo.Name = "Avro"
	as.SchemaInfo.Properties = properties
	return as, nil
}

func (as *AvroSchema) Encode(data interface{}) ([]byte, error) {
	textual, err := json.Marshal(data)
	if err != nil {
		log.Errorf("serialize data error:%s", err.Error())
		return nil, err
	}
	native, _, err := as.Codec.NativeFromTextual(textual)
	if err != nil {
		log.Errorf("convert native Go form to binary Avro data error:%s", err.Error())
		return nil, err
	}
	return as.Codec.BinaryFromNative(nil, native)
}

func (as *AvroSchema) Decode(data []byte, v interface{}) error {
	native, _, err := as.Codec.NativeFromBinary(data)
	if err != nil {
		log.Errorf("convert binary Avro data back to native Go form error:%s", err.Error())
		return err
	}
	textual, err := as.Codec.TextualFromNative(nil, native)
	if err != nil {
		log.Errorf("convert native Go form to textual Avro data error:%s", err.Error())
		return err
	}
	err = json.Unmarshal(textual, v)
	if err != nil {
		log.Errorf("unSerialize textual error:%s", err.Error())
		return err
	}
	return nil
}

func (as *AvroSchema) Validate(message []byte) error {
	return as.Decode(message, nil)
}

func (as *AvroSchema) GetSchemaInfo() *SchemaInfo {
	return &as.SchemaInfo
}

type StringSchema struct {
	SchemaInfo
}

func NewStringSchema(properties map[string]string) *StringSchema {
	strSchema := new(StringSchema)
	strSchema.SchemaInfo.Properties = properties
	strSchema.SchemaInfo.Name = "String"
	strSchema.SchemaInfo.Type = STRING
	strSchema.SchemaInfo.Schema = ""
	return strSchema
}

func (ss *StringSchema) Encode(v interface{}) ([]byte, error) {
	return []byte(v.(string)), nil
}

// Decode convert from byte slice to string without allocating a new string
func (ss *StringSchema) Decode(data []byte, v interface{}) error {
	strPtr := (*string)(unsafe.Pointer(&data))
	reflect.ValueOf(v).Elem().Set(reflect.ValueOf(strPtr))
	return nil
}

func (ss *StringSchema) Validate(message []byte) error {
	return ss.Decode(message, nil)
}

func (ss *StringSchema) GetSchemaInfo() *SchemaInfo {
	return &ss.SchemaInfo
}

type BytesSchema struct {
	SchemaInfo
}

func NewBytesSchema(properties map[string]string) *BytesSchema {
	bytesSchema := new(BytesSchema)
	bytesSchema.SchemaInfo.Properties = properties
	bytesSchema.SchemaInfo.Name = "Bytes"
	bytesSchema.SchemaInfo.Type = BYTES
	bytesSchema.SchemaInfo.Schema = ""
	return bytesSchema
}

func (bs *BytesSchema) Encode(data interface{}) ([]byte, error) {
	return data.([]byte), nil
}

func (bs *BytesSchema) Decode(data []byte, v interface{}) error {
	reflect.ValueOf(v).Elem().Set(reflect.ValueOf(data))
	return nil
}

func (bs *BytesSchema) Validate(message []byte) error {
	return bs.Decode(message, nil)
}

func (bs *BytesSchema) GetSchemaInfo() *SchemaInfo {
	return &bs.SchemaInfo
}

type Int8Schema struct {
	SchemaInfo
}

func NewInt8Schema(properties map[string]string) *Int8Schema {
	int8Schema := new(Int8Schema)
	int8Schema.SchemaInfo.Properties = properties
	int8Schema.SchemaInfo.Schema = ""
	int8Schema.SchemaInfo.Type = INT8
	int8Schema.SchemaInfo.Name = "INT8"
	return int8Schema
}

func (is8 *Int8Schema) Encode(value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := WriteElements(&buf, value.(int8))
	return buf.Bytes(), err
}

func (is8 *Int8Schema) Decode(data []byte, v interface{}) error {
	buf := bytes.NewReader(data)
	return ReadElements(buf, v)
}

func (is8 *Int8Schema) Validate(message []byte) error {
	if len(message) != 1 {
		return newError(InvalidMessage, "size of data received by Int8Schema is not 1")
	}
	return nil
}

func (is8 *Int8Schema) GetSchemaInfo() *SchemaInfo {
	return &is8.SchemaInfo
}

type Int16Schema struct {
	SchemaInfo
}

func NewInt16Schema(properties map[string]string) *Int16Schema {
	int16Schema := new(Int16Schema)
	int16Schema.SchemaInfo.Properties = properties
	int16Schema.SchemaInfo.Name = "INT16"
	int16Schema.SchemaInfo.Type = INT16
	int16Schema.SchemaInfo.Schema = ""
	return int16Schema
}

func (is16 *Int16Schema) Encode(value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := WriteElements(&buf, value.(int16))
	return buf.Bytes(), err
}

func (is16 *Int16Schema) Decode(data []byte, v interface{}) error {
	buf := bytes.NewReader(data)
	return ReadElements(buf, v)
}

func (is16 *Int16Schema) Validate(message []byte) error {
	if len(message) != 2 {
		return newError(InvalidMessage, "size of data received by Int16Schema is not 2")
	}
	return nil
}

func (is16 *Int16Schema) GetSchemaInfo() *SchemaInfo {
	return &is16.SchemaInfo
}

type Int32Schema struct {
	SchemaInfo
}

func NewInt32Schema(properties map[string]string) *Int32Schema {
	int32Schema := new(Int32Schema)
	int32Schema.SchemaInfo.Properties = properties
	int32Schema.SchemaInfo.Schema = ""
	int32Schema.SchemaInfo.Name = "INT32"
	int32Schema.SchemaInfo.Type = INT32
	return int32Schema
}

func (is32 *Int32Schema) Encode(value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := WriteElements(&buf, value.(int32))
	return buf.Bytes(), err
}

func (is32 *Int32Schema) Decode(data []byte, v interface{}) error {
	buf := bytes.NewReader(data)
	return ReadElements(buf, v)
}

func (is32 *Int32Schema) Validate(message []byte) error {
	if len(message) != 4 {
		return newError(InvalidMessage, "size of data received by Int32Schema is not 4")
	}
	return nil
}

func (is32 *Int32Schema) GetSchemaInfo() *SchemaInfo {
	return &is32.SchemaInfo
}

type Int64Schema struct {
	SchemaInfo
}

func NewInt64Schema(properties map[string]string) *Int64Schema {
	int64Schema := new(Int64Schema)
	int64Schema.SchemaInfo.Properties = properties
	int64Schema.SchemaInfo.Name = "INT64"
	int64Schema.SchemaInfo.Type = INT64
	int64Schema.SchemaInfo.Schema = ""
	return int64Schema
}

func (is64 *Int64Schema) Encode(value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := WriteElements(&buf, value.(int64))
	return buf.Bytes(), err
}

func (is64 *Int64Schema) Decode(data []byte, v interface{}) error {
	buf := bytes.NewReader(data)
	return ReadElements(buf, v)
}

func (is64 *Int64Schema) Validate(message []byte) error {
	if len(message) != 8 {
		return newError(InvalidMessage, "size of data received by Int64Schema is not 8")
	}
	return nil
}

func (is64 *Int64Schema) GetSchemaInfo() *SchemaInfo {
	return &is64.SchemaInfo
}

type FloatSchema struct {
	SchemaInfo
}

func NewFloatSchema(properties map[string]string) *FloatSchema {
	floatSchema := new(FloatSchema)
	floatSchema.SchemaInfo.Properties = properties
	floatSchema.SchemaInfo.Type = FLOAT
	floatSchema.SchemaInfo.Name = "FLOAT"
	floatSchema.SchemaInfo.Schema = ""
	return floatSchema
}

func (fs *FloatSchema) Encode(value interface{}) ([]byte, error) {
	return BinarySerializer.PutFloat(value)
}

func (fs *FloatSchema) Decode(data []byte, v interface{}) error {
	floatValue, err := BinarySerializer.Float32(data)
	if err != nil {
		log.Errorf("unSerialize float error:%s", err.Error())
		return err
	}
	reflect.ValueOf(v).Elem().Set(reflect.ValueOf(floatValue))
	return nil
}

func (fs *FloatSchema) Validate(message []byte) error {
	if len(message) != 4 {
		return newError(InvalidMessage, "size of data received by FloatSchema is not 4")
	}
	return nil
}

func (fs *FloatSchema) GetSchemaInfo() *SchemaInfo {
	return &fs.SchemaInfo
}

type DoubleSchema struct {
	SchemaInfo
}

func NewDoubleSchema(properties map[string]string) *DoubleSchema {
	doubleSchema := new(DoubleSchema)
	doubleSchema.SchemaInfo.Properties = properties
	doubleSchema.SchemaInfo.Type = DOUBLE
	doubleSchema.SchemaInfo.Name = "DOUBLE"
	doubleSchema.SchemaInfo.Schema = ""
	return doubleSchema
}

func (ds *DoubleSchema) Encode(value interface{}) ([]byte, error) {
	return BinarySerializer.PutDouble(value)
}

func (ds *DoubleSchema) Decode(data []byte, v interface{}) error {
	doubleValue, err := BinarySerializer.Float64(data)
	if err != nil {
		log.Errorf("unSerialize double value error:%s", err.Error())
		return err
	}
	reflect.ValueOf(v).Elem().Set(reflect.ValueOf(doubleValue))
	return nil
}

func (ds *DoubleSchema) Validate(message []byte) error {
	if len(message) != 8 {
		return newError(InvalidMessage, "size of data received by DoubleSchema is not 8")
	}
	return nil
}

func (ds *DoubleSchema) GetSchemaInfo() *SchemaInfo {
	return &ds.SchemaInfo
}
