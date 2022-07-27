package jobsdb

import "database/sql"

// StoreSafeTx sealed interface
type StoreSafeTx interface {
	Tx() *sql.Tx
	storeSafeTxIdentifier() string
	writerQueueToken() *earlyQueueToken
}

type storeSafeTx struct {
	tx       *sql.Tx
	identity string
	eqt      *earlyQueueToken
}

func (r *storeSafeTx) storeSafeTxIdentifier() string {
	return r.identity
}

func (r *storeSafeTx) Tx() *sql.Tx {
	return r.tx
}

func (r *storeSafeTx) writerQueueToken() *earlyQueueToken {
	return r.eqt
}

// EmptyStoreSafeTx returns an empty interface usable only for tests
func EmptyStoreSafeTx() StoreSafeTx {
	return &storeSafeTx{}
}

// UpdateSafeTx sealed interface
type UpdateSafeTx interface {
	Tx() *sql.Tx
	updateSafeTxSealIdentifier() string
	writerQueueToken() *earlyQueueToken
}
type updateSafeTx struct {
	tx       *sql.Tx
	identity string
	eqt      *earlyQueueToken
}

func (r *updateSafeTx) updateSafeTxSealIdentifier() string {
	return r.identity
}

func (r *updateSafeTx) Tx() *sql.Tx {
	return r.tx
}

func (r *updateSafeTx) writerQueueToken() *earlyQueueToken {
	return r.eqt
}

// EmptyUpdateSafeTx returns an empty interface usable only for tests
func EmptyUpdateSafeTx() UpdateSafeTx {
	return &updateSafeTx{}
}
