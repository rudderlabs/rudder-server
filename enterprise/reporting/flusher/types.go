package flusher

type Handler interface {
	Decode(report map[string]interface{}) (interface{}, error)
	Aggregate(aggReport interface{}, report interface{}) error
}
