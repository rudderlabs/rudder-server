package model

type (
	SchemaType = string
)

const (
	StringDataType   SchemaType = "string"
	BooleanDataType  SchemaType = "boolean"
	IntDataType      SchemaType = "int"
	FloatDataType    SchemaType = "float"
	JSONDataType     SchemaType = "json"
	TextDataType     SchemaType = "text"
	DateTimeDataType SchemaType = "datetime"
)
