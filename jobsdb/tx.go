package jobsdb

import "database/sql"

type Tx interface {
	Tx() *sql.Tx
	AddSuccessListener(listener func())
	commit() error
}

type internalTx struct {
	successListeners []func()
	tx               *sql.Tx
}

func (r *internalTx) Tx() *sql.Tx {
	return r.tx
}

func (r *internalTx) AddSuccessListener(listener func()) {
	r.successListeners = append(r.successListeners, listener)
}

func (r *internalTx) commit() error {
	err := r.tx.Commit()
	if err != nil {
		return err
	}
	// execute success listeners
	for i := range r.successListeners {
		r.successListeners[i]()
	}
	return nil
}

// EmptyTx returns an empty interface usable only for tests
func EmptyTx() Tx {
	return &internalTx{}
}

// StoreSafeTx sealed interface
type StoreSafeTx interface {
	Tx
	storeSafeTxIdentifier() string
}

type storeSafeTx struct {
	*internalTx
	identity string
}

func (r *storeSafeTx) storeSafeTxIdentifier() string {
	return r.identity
}

// EmptyStoreSafeTx returns an empty interface usable only for tests
func EmptyStoreSafeTx() StoreSafeTx {
	return &storeSafeTx{}
}

// UpdateSafeTx sealed interface
type UpdateSafeTx interface {
	Tx
	updateSafeTxSealIdentifier() string
}
type updateSafeTx struct {
	*internalTx
	identity string
}

func (r *updateSafeTx) updateSafeTxSealIdentifier() string {
	return r.identity
}

// EmptyUpdateSafeTx returns an empty interface usable only for tests
func EmptyUpdateSafeTx() UpdateSafeTx {
	return &updateSafeTx{}
}
