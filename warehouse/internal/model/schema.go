package model

import "time"

type (
	SchemaType  string
	TableSchema map[string]string
	Schema      map[string]TableSchema
)

const (
	StringDataType  SchemaType = "string"
	BooleanDataType SchemaType = "boolean"
	IntDataType     SchemaType = "int"
	BigIntDataType  SchemaType = "bigint"
	FloatDataType   SchemaType = "float"
	JSONDataType    SchemaType = "json"
	TextDataType    SchemaType = "text"
)

type WHSchema struct {
	ID              int64
	UploadID        int64
	SourceID        string
	Namespace       string
	DestinationID   string
	DestinationType string
	Schema          Schema
	CreatedAt       time.Time
	UpdatedAt       time.Time
}
