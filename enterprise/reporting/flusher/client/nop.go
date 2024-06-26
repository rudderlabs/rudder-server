package client

import "context"

type NOP struct{}

func (n *NOP) MakePOSTRequest(ctx context.Context, payload interface{}) error {
	return nil
}
