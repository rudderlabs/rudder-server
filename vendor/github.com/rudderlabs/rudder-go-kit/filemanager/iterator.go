package filemanager

import "context"

// IterateFilesWithPrefix returns an iterator that can be used to iterate over all files with the given prefix.
func IterateFilesWithPrefix(ctx context.Context, prefix, startAfter string, maxItems int64, manager FileManager) *ObjectIterator {
	it := &ObjectIterator{
		session: manager.ListFilesWithPrefix(ctx, startAfter, prefix, maxItems),
	}
	return it
}

// NewListIterator returns a new iterator for the given list session.
func NewListIterator(session ListSession) *ObjectIterator {
	return &ObjectIterator{
		session: session,
	}
}

type ObjectIterator struct {
	session ListSession

	item  *FileInfo
	items []*FileInfo
	err   error
}

func (it *ObjectIterator) Next() bool {
	var err error
	if len(it.items) == 0 {
		it.items, err = it.session.Next()
		if err != nil {
			it.err = err
			return false
		}
	}

	if len(it.items) > 0 {
		it.item = it.items[0]
		it.items = it.items[1:]
		return true
	}
	return false
}

func (it *ObjectIterator) Get() *FileInfo {
	return it.item
}

func (it *ObjectIterator) Err() error {
	return it.err
}
