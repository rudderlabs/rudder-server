package batchDeleter

import "github.com/rudderlabs/rudder-server/regulation-worker/internal/model"

type BatchDeleter interface {
	GetData() error
	DeleteData() error
	UploadData() error
}

type Deleter struct {
	Dest model.Destination
	data interface{}
}

func (d *Deleter) GetData() error {
	d.data = nil
	return nil
}

func (d *Deleter) DeleteData() error {
	return nil
}

func (d *Deleter) UploadData() error {
	return nil
}
