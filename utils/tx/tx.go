package tx

import (
	"database/sql"

	"github.com/jackc/pgx/v5"
)

// Tx is a wrapper around sql.Tx that supports registering and executing
// post-commit actions, a.k.a. success listeners.
type Tx struct {
	pgx.Tx
	successListeners []func()
}

// AddSuccessListener registers a listener to be executed after the transaction has been committed successfully.
func (tx *Tx) AddSuccessListener(listener func()) {
	tx.successListeners = append(tx.successListeners, listener)
}

// RunSuccessListeners executes all listeners.
func (tx *Tx) RunSuccessListeners() error {
	for _, successListener := range tx.successListeners {
		successListener()
	}
	return nil
}

type TxSql struct {
	*sql.Tx
	successListeners []func()
}

func (tx *TxSql) AddSuccessListener(listener func()) {
	tx.successListeners = append(tx.successListeners, listener)
}

// Commit commits the transaction and executes all listeners.
func (tx *TxSql) Commit() error {
	err := tx.Tx.Commit()
	if err == nil {
		for _, successListener := range tx.successListeners {
			successListener()
		}
	}
	return err
}
