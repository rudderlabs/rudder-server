package model

type SchemaType string

const (
	StringDataType  SchemaType = "string"
	BooleanDataType            = "boolean"
	IntDataType                = "int"
	BigIntDataType             = "bigint"
	FloatDataType              = "float"
	JSONDataType               = "json"
	TextDataType               = "text"
)
