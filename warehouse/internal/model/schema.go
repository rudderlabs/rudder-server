package model

import (
	"strings"
	"time"
)

type (
	SchemaType  = string
	TableSchema map[string]string
	Schema      map[string]TableSchema
)

const (
	StringDataType         SchemaType = "string"
	BooleanDataType        SchemaType = "boolean"
	IntDataType            SchemaType = "int"
	BigIntDataType         SchemaType = "bigint"
	FloatDataType          SchemaType = "float"
	JSONDataType           SchemaType = "json"
	TextDataType           SchemaType = "text"
	DateTimeDataType       SchemaType = "datetime"
	ArrayOfBooleanDataType SchemaType = "array(boolean)"
)

type WHSchema struct {
	ID              int64
	SourceID        string
	Namespace       string
	DestinationID   string
	DestinationType string
	Schema          Schema
	CreatedAt       time.Time
	UpdatedAt       time.Time
	ExpiresAt       time.Time
}

func (t TableSchema) String() string {
	first := true
	sb := strings.Builder{}
	for k, v := range t {
		if !first {
			sb.WriteString(",")
		}
		sb.WriteString(k)
		sb.WriteString(":")
		sb.WriteString(v)
		first = false
	}
	return sb.String()
}
