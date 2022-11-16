package model

type SchemaType string

const (
	StringDataType  SchemaType = "string"
	BooleanDataType SchemaType = "boolean"
	IntDataType     SchemaType = "int"
	BigIntDataType  SchemaType = "bigint"
	FloatDataType   SchemaType = "float"
	JSONDataType    SchemaType = "json"
	TextDataType    SchemaType = "text"
)
