package apiDeleter

import "github.com/rudderlabs/rudder-server/regulation-worker/internal/model"

type APIDeleter interface {
	CallTransformer() (string, error)
}

type Deleter struct {
	Dest model.Destination
}

func (d *Deleter) CallTransformer() (string, error) {
	//make API call to transformer and get response
	//based on the response set appropriate status string and return
	var status string
	return status, nil
}
