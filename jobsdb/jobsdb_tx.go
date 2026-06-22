package jobsdb

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/lib/pq"
	"github.com/lib/pq/pqerror"

	"github.com/rudderlabs/rudder-go-kit/logger"

	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
)

// StoreSafeTx sealed interface
type StoreSafeTx interface {
	Tx() *Tx
	SqlTx() *sql.Tx
	storeSafeTxIdentifier() string
	getLastDS() dataSetT
}

type storeSafeTx struct {
	tx       *Tx
	identity string
	lastDS   dataSetT
}

func (r *storeSafeTx) storeSafeTxIdentifier() string {
	return r.identity
}

func (r *storeSafeTx) Tx() *Tx {
	return r.tx
}

func (r *storeSafeTx) SqlTx() *sql.Tx {
	return r.tx.Tx
}

func (r *storeSafeTx) getLastDS() dataSetT {
	return r.lastDS
}

// EmptyStoreSafeTx returns an empty interface usable only for tests
func EmptyStoreSafeTx() StoreSafeTx {
	return &storeSafeTx{tx: &Tx{}}
}

// UpdateSafeTx sealed interface
type UpdateSafeTx interface {
	Tx() *Tx
	SqlTx() *sql.Tx
	getDSList() []dataSetT
	getDSRangeList() []dataSetRangeT
	setDSList([]dataSetT, []dataSetRangeT)
	updateSafeTxSealIdentifier() string
}

type updateSafeTx struct {
	tx          *Tx
	identity    string
	dsList      []dataSetT
	dsRangeList []dataSetRangeT
}

func (r *updateSafeTx) updateSafeTxSealIdentifier() string {
	return r.identity
}

func (r *updateSafeTx) getDSList() []dataSetT {
	return r.dsList
}

func (r *updateSafeTx) getDSRangeList() []dataSetRangeT {
	return r.dsRangeList
}

func (r *updateSafeTx) setDSList(dsList []dataSetT, dsRangeList []dataSetRangeT) {
	r.dsList = dsList
	r.dsRangeList = dsRangeList
}

func (r *updateSafeTx) Tx() *Tx {
	return r.tx
}

func (r *updateSafeTx) SqlTx() *sql.Tx {
	return r.tx.Tx
}

// EmptyUpdateSafeTx returns an empty interface usable only for tests
func EmptyUpdateSafeTx() UpdateSafeTx {
	return &updateSafeTx{tx: &Tx{}}
}

type transactionHandler interface {
	Exec(string, ...any) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
	// If required, add other definitions that are common between *sql.DB and *sql.Tx
	// Never include Commit and Rollback in this interface
	// That ensures that whoever is acting on a transactionHandler can't commit or rollback
	// Only the function that passes *sql.Tx should do the commit or rollback based on the error it receives
}

func (jd *Handle) execStmtInTx(tx *sql.Tx, sqlStatement string) {
	_, err := tx.Exec(sqlStatement)
	jd.assertError(err)
}

func (jd *Handle) execStmtInTxAllowMissing(tx *sql.Tx, sqlStatement string) {
	const (
		savepointSql = "SAVEPOINT execStmtInTxAllowMissing"
		rollbackSql  = "ROLLBACK TO " + savepointSql
	)

	_, err := tx.Exec(savepointSql)
	jd.assertError(err)

	_, err = tx.Exec(sqlStatement)
	if err != nil {
		var pqError *pq.Error
		ok := errors.As(err, &pqError)
		if ok && pqError.Code == pqerror.UndefinedTable {
			jd.logger.Infon("sql statement exec failed because table doesn't exist",
				logger.NewStringField("tablePrefix", jd.tablePrefix),
				logger.NewStringField("sqlStatement", sqlStatement),
			)
			_, err = tx.Exec(rollbackSql)
			jd.assertError(err)
		} else {
			jd.assertError(err)
		}
	}
}

func (jd *Handle) WithTx(ctx context.Context, f func(tx *Tx) error) error {
	return jd.withTxOnDB(ctx, jd.getDB(ctx), f)
}

// withMaintenanceTx runs f inside a transaction opened on the maintenance pool
// (or dbHandle if no maintenance pool is configured). Used by jobsdb-internal
// maintenance flows to keep their BeginTx off the main pool.
func (jd *Handle) withMaintenanceTx(ctx context.Context, f func(tx *Tx) error) error {
	return jd.withTxOnDB(ctx, jd.maintenanceDB(), f)
}

func (jd *Handle) withTxOnDB(ctx context.Context, db *sql.DB, f func(tx *Tx) error) error {
	sqltx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	tx := &Tx{Tx: sqltx}
	err = f(tx)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("%w; rollback error: %s", err, rollbackErr)
		}
		return err
	}
	return tx.Commit()
}

func (jd *Handle) getAdvisoryLockForOperation(operation string) int64 {
	key := fmt.Sprintf("%s_%s", jd.tablePrefix, operation)
	h := sha256.New()
	h.Write([]byte(key))
	return int64(binary.BigEndian.Uint32(h.Sum(nil)))
}

// getDB returns the appropriate database handle based on context.
// If the context requests priority pool usage and a priority pool is configured,
// it returns the priority pool. Otherwise, it returns the regular dbHandle.
func (jd *Handle) getDB(ctx context.Context) *sql.DB {
	if usePriorityPool(ctx) && jd.priorityPool != nil {
		return jd.priorityPool
	}
	return jd.dbHandle
}

// maintenanceDB returns the connection pool to use for jobsdb-internal
// maintenance operations (compaction, dsList refresh,
// status-table cleanup/vacuum). Falls back to dbHandle when no dedicated
// maintenance pool was injected via WithMaintenancePoolDB.
func (jd *Handle) maintenanceDB() *sql.DB {
	if jd.maintenancePool != nil {
		return jd.maintenancePool
	}
	return jd.dbHandle
}

func (jd *Handle) withDistributedLock(ctx context.Context, tx *Tx, operation string, f func() error) error {
	advisoryLock := jd.getAdvisoryLockForOperation(operation)
	_, err := tx.ExecContext(ctx, fmt.Sprintf(`SELECT pg_advisory_xact_lock(%d);`, advisoryLock))
	if err != nil {
		return fmt.Errorf("error while acquiring advisory lock %d for operation %s: %w", advisoryLock, operation, err)
	}
	return f()
}

func (jd *Handle) withDistributedSharedLock(ctx context.Context, tx *Tx, operation string, f func() error) error {
	advisoryLock := jd.getAdvisoryLockForOperation(operation)
	_, err := tx.ExecContext(ctx, fmt.Sprintf(`SELECT pg_advisory_xact_lock_shared(%d);`, advisoryLock))
	if err != nil {
		return fmt.Errorf("error while acquiring a shared advisory lock %d for operation %s: %w", advisoryLock, operation, err)
	}
	return f()
}
