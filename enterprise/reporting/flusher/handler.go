//go:generate mockgen -destination=./handler_mock.go -package=flusher -source=./handler.go Handler
package flusher

type Handler interface {
	Decode(report map[string]interface{}) (interface{}, error)
	Aggregate(aggReport interface{}, report interface{}) error
}
