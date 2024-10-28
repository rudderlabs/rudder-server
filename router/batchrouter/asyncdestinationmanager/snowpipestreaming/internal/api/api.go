package api

import (
	"io"
	"net/http"

	jsoniter "github.com/json-iterator/go"
)

type (
	API struct {
		clientURL   string
		requestDoer requestDoer
	}

	requestDoer interface {
		Do(*http.Request) (*http.Response, error)
	}
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func New(clientURL string, requestDoer requestDoer) *API {
	return &API{
		clientURL:   clientURL,
		requestDoer: requestDoer,
	}
}

func mustRead(r io.Reader) []byte {
	data, _ := io.ReadAll(r)
	return data
}
