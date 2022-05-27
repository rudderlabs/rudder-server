package jobsdb

import (
	"database/sql"
	"errors"
	"sync"
)

// ErrEmptyxTx sentinel error indicating an Empty Tx
var ErrEmptyxTx = errors.New("jobsdb: empty tx")

// TxGetter provides access to an sql.Tx
type TxGetter interface {
	// Tx retrieves the tx or returns an error if the tx could not be retrieved (started)
	Tx() (*sql.Tx, error)
	// MustTx retrieves the tx or panics in case of an error
	MustTx() *sql.Tx
}

type noTx struct{}

// EmptyTx returns an empty interface usable only for tests
func EmptyTx() TxGetter {
	return &noTx{}
}

func (*noTx) Tx() (*sql.Tx, error) {
	return nil, ErrEmptyxTx
}

func (*noTx) MustTx() *sql.Tx {
	return nil
}

type eagerTx struct {
	tx *sql.Tx
}

func (r *eagerTx) Tx() (*sql.Tx, error) {
	return r.tx, nil
}

func (r *eagerTx) MustTx() *sql.Tx {
	return r.tx
}

type lazyTx struct {
	dbHandle *sql.DB
	once     sync.Once
	tx       *sql.Tx
	err      error
}

func (r *lazyTx) Tx() (*sql.Tx, error) {
	r.once.Do(func() {
		r.tx, r.err = r.dbHandle.Begin()
	})
	return r.tx, r.err
}

func (r *lazyTx) MustTx() *sql.Tx {
	tx, err := r.Tx()
	if err != nil {
		panic(err)
	}
	return tx
}

// StoreSafeTx sealed interface
type StoreSafeTx interface {
	TxGetter
	storeSafeTxIdentifier() string
}

type storeSafeTx struct {
	TxGetter
	identity string
}

func (r *storeSafeTx) storeSafeTxIdentifier() string {
	return r.identity
}

// EmptyStoreSafeTx returns an empty interface usable only for tests
func EmptyStoreSafeTx() StoreSafeTx {
	return &storeSafeTx{TxGetter: &noTx{}}
}

// UpdateSafeTx sealed interface
type UpdateSafeTx interface {
	TxGetter
	updateSafeTxSealIdentifier() string
}
type updateSafeTx struct {
	TxGetter
	identity string
}

func (r *updateSafeTx) updateSafeTxSealIdentifier() string {
	return r.identity
}

// EmptyUpdateSafeTx returns an empty interface usable only for tests
func EmptyUpdateSafeTx() UpdateSafeTx {
	return &updateSafeTx{TxGetter: &noTx{}}
}
