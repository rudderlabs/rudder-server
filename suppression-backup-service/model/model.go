package model

import "sync"

type File struct {
	Path string
	Mu   *sync.RWMutex
}
