//go:generate mockgen -destination=./client_mock.go -package=client -source=./client.go Client
package client

import "context"

type Client interface {
	MakePOSTRequest(ctx context.Context, payload interface{}) error
}
