package api

import (
	"io"
	"net/http"
)

type API struct {
	clientURL   string
	requestDoer requestDoer
}

type requestDoer interface {
	Do(*http.Request) (*http.Response, error)
}

func New(clientURL string, requestDoer requestDoer) *API {
	return &API{
		clientURL:   clientURL,
		requestDoer: requestDoer,
	}
}

func mustReadAll(r io.Reader) []byte {
	data, _ := io.ReadAll(r)
	return data
}
