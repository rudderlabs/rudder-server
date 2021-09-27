package operationmanager

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	OperationManager        OperationManagerI
	pkgLogger               logger.LoggerI
	enableOperationsManager bool
)

type OperationtT struct {
	ID        int64
	Operation string
	Payload   json.RawMessage
	done      bool
	CreatedAt time.Time
}

type OperationManagerI interface {
	InsertOperation(payload []byte) (int64, error)
	StartProcessLoop(ctx context.Context) error
	GetOperationStatus(opID int64) (bool, string)
}
type OperationManagerT struct {
	dbHandle      *sql.DB
	gatewayDB     jobsdb.JobsDB
	routerDB      jobsdb.JobsDB
	batchRouterDB jobsdb.JobsDB
}

type OperationHandlerI interface {
	Exec(payload []byte) error
}

func Init2() {
	pkgLogger = logger.NewLogger().Child("operationmanager")
	config.RegisterBoolConfigVariable(true, &enableOperationsManager, false, "Operations.enabled")
}

func Setup(gatewayDB, routerDB, batchRouterDB jobsdb.JobsDB) {
	pkgLogger.Info("setting up operation manager.")
	dbHandle, err := sql.Open("postgres", jobsdb.GetConnectionString())
	if err != nil {
		panic(fmt.Errorf("failed to connect to DB. Err: %v", err))
	}
	qm := new(OperationManagerT)
	qm.dbHandle = dbHandle
	qm.gatewayDB = gatewayDB
	qm.routerDB = routerDB
	qm.batchRouterDB = batchRouterDB

	OperationManager = qm
}

func GetOperationManager() OperationManagerI {
	if OperationManager == nil {
		panic("OperationManager is not initialized. Call Setup first.")
	}

	return OperationManager
}

func (om *OperationManagerT) InsertOperation(payload []byte) (int64, error) {
	if !enableOperationsManager {
		return -1, fmt.Errorf("operation manager is disabled")
	}

	sqlStatement := `INSERT INTO operations (operation, payload, done, op_status)
	VALUES ($1, $2, $3, $4)
	RETURNING id`

	stmt, err := om.dbHandle.Prepare(sqlStatement)
	if err != nil {
		return -1, err
	}
	defer stmt.Close()

	row := stmt.QueryRow("CLEAR", payload, false, "queued")
	var opID int64
	err = row.Scan(&opID)
	if err != nil {
		return -1, err
	}

	return opID, nil
}

func (om *OperationManagerT) MarkOperationStart(opID int64) error {
	sqlStatement := `UPDATE operations SET op_status=$3, start_time=$2 WHERE id=$1`
	_, err := om.dbHandle.Exec(sqlStatement, opID, time.Now().UTC(), "executing")
	if err != nil {
		return err
	}
	return nil
}

func (om *OperationManagerT) MarkOperationDone(opID int64, status string) error {
	sqlStatement := `UPDATE operations SET done=$2, op_status=$4, end_time=$3 WHERE id=$1`
	_, err := om.dbHandle.Exec(sqlStatement, opID, true, time.Now().UTC(), status)
	if err != nil {
		return err
	}
	return nil
}

func (om *OperationManagerT) GetOperationStatus(opID int64) (bool, string) {
	sqlStatement := fmt.Sprintf(`SELECT done, op_status
								FROM operations
								WHERE
								id=%d`, opID)

	stmt, err := om.dbHandle.Prepare(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("[GetOperationStatus] Failed to prepare sql: %s", sqlStatement)
		return false, ""
	}
	defer stmt.Close()

	var done bool
	var status string
	row := om.dbHandle.QueryRow(sqlStatement)
	err = row.Scan(&done, &status)
	if err == sql.ErrNoRows {
		pkgLogger.Debugf("[GetOperationStatus] No rows found. Sql: %s,", sqlStatement)
		return false, "op_id not found"
	}
	if err != nil {
		pkgLogger.Errorf("[GetOperationStatus] Failed to scan row. Sql: %s, Err: %w", sqlStatement, err)
		return false, ""
	}

	return done, status
}

func (om *OperationManagerT) StartProcessLoop(ctx context.Context) error {
	if !enableOperationsManager {
		pkgLogger.Infof("operation manager is disabled. Not starting process loop.")
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second * 5):
			om.processOperation()
		}
	}
}

func (om *OperationManagerT) processOperation() {
	status := "succeeded"
	var op OperationtT
	sqlStatement := `SELECT id, operation, payload, done
                                	FROM operations
                                	WHERE
									done=False
									ORDER BY id asc limit 1`

	stmt, err := om.dbHandle.Prepare(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("Failed to prepare sql: %s", sqlStatement)
		return
	}
	defer stmt.Close()

	row := om.dbHandle.QueryRow(sqlStatement)
	err = row.Scan(&op.ID, &op.Operation, &op.Payload, &op.done)
	if err == sql.ErrNoRows {
		pkgLogger.Debugf("No rows found. Sql: %s,", sqlStatement)
		return
	}
	if err != nil {
		pkgLogger.Errorf("Failed to scan row. Sql: %s, Err: %w", sqlStatement, err)
		return
	}

	om.MarkOperationStart(op.ID)

	pkgLogger.Debugf("%#v", op)
	opHandler := om.getHandler(op.Operation)
	if opHandler == nil {
		pkgLogger.Errorf("No handler found for operation: %s", op.Operation)
		status = "dropped"
	} else {
		err := opHandler.Exec(op.Payload)
		if err != nil {
			pkgLogger.Errorf("Operation(%s) execution failed with error: %w", op.Operation, err)
			status = "failed"
		}
	}

	om.MarkOperationDone(op.ID, status)
}

func (om *OperationManagerT) getHandler(operation string) OperationHandlerI {
	if operation == "CLEAR" {
		return GetClearOperationHandlerInstance(om.gatewayDB, om.routerDB, om.batchRouterDB)
	}

	return nil
}
