package queuemanager

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	QueueManager QueueManagerI
	pkgLogger    logger.LoggerI
)

type OperationtT struct {
	ID        int64
	Operation string
	Payload   json.RawMessage
	done      bool
	CreatedAt time.Time
}

type QueueManagerI interface {
	InsertOperation(payload []byte) error
	StartProcessLoop()
}
type QueueManagerT struct {
	dbHandle      *sql.DB
	gatewayDB     jobsdb.JobsDB
	routerDB      jobsdb.JobsDB
	batchRouterDB jobsdb.JobsDB
}

type OperationHandlerI interface {
	Exec(payload []byte) (bool, error)
}

func init() {
	pkgLogger = logger.NewLogger().Child("queuemanager")
}

func Setup(gatewayDB, routerDB, batchRouterDB jobsdb.JobsDB) {
	pkgLogger.Info("setting up queuemanager.")
	dbHandle, err := sql.Open("postgres", jobsdb.GetConnectionString())
	if err != nil {
		panic(fmt.Errorf("failed to connect to DB. Err: %v", err))
	}
	qm := new(QueueManagerT)
	qm.dbHandle = dbHandle
	qm.gatewayDB = gatewayDB
	qm.routerDB = routerDB
	qm.batchRouterDB = batchRouterDB

	QueueManager = qm
}

func GetQueueManager() QueueManagerI {
	if QueueManager == nil {
		panic("QueueManager is not initialized. Call Setup first.")
	}

	return QueueManager
}

func (qm *QueueManagerT) InsertOperation(payload []byte) error {
	sqlStatement := `INSERT INTO operations (operation, payload, done, op_status)
	VALUES ($1, $2, $3, $4)
	RETURNING id`

	stmt, err := qm.dbHandle.Prepare(sqlStatement)
	if err != nil {
		return err
	}

	row := stmt.QueryRow("CLEAR", payload, false, "queued")
	var opID int64
	err = row.Scan(&opID)
	if err != nil {
		return err
	}

	return nil
}

func (qm *QueueManagerT) MarkOperationDone(opID int64, status string) error {
	sqlStatement := `UPDATE operations SET done=$2, op_status=$4, end_time=$3 WHERE id=$1`
	_, err := qm.dbHandle.Exec(sqlStatement, opID, true, time.Now().UTC(), status)
	if err != nil {
		return err
	}
	return nil
}

func (qm *QueueManagerT) StartProcessLoop() {
	for {
		status := "succeeded"
		var op OperationtT
		sqlStatement := `SELECT id, operation, payload, done
                                	FROM operations
                                	WHERE
									done=False
									ORDER BY id asc limit 1`

		stmt, err := qm.dbHandle.Prepare(sqlStatement)
		if err != nil {
			pkgLogger.Errorf("Failed to prepare sql: %s", sqlStatement)
			time.Sleep(5 * time.Second)
			continue
		}

		row := qm.dbHandle.QueryRow(sqlStatement)
		err = row.Scan(&op.ID, &op.Operation, &op.Payload, &op.done)
		if err == sql.ErrNoRows {
			pkgLogger.Infof("No rows found. Sql: %s,", sqlStatement) //TODO change to Debugf
			time.Sleep(5 * time.Second)
			continue
		}
		if err != nil {
			pkgLogger.Errorf("Failed to scan row. Sql: %s, Err: %w", sqlStatement, err)
			time.Sleep(5 * time.Second)
			continue
		}
		stmt.Close()

		pkgLogger.Infof("%#v", op)
		opHandler := qm.getHandler(op.Operation)
		if opHandler == nil {
			pkgLogger.Errorf("No handler found for operation: %s", op.Operation)
			status = "dropped"
		} else {
			retry, err := opHandler.Exec(op.Payload)
			if err != nil {
				pkgLogger.Errorf("Operation(%s) execution failed with error: %w", op.Operation, err)
				if retry {
					time.Sleep(5 * time.Second)
					continue
				} else {
					status = "failed"
				}
			}
		}

		qm.MarkOperationDone(op.ID, status)

		time.Sleep(5 * time.Second)
	}
}

func (qm *QueueManagerT) getHandler(operation string) OperationHandlerI {
	if operation == "CLEAR" {
		return GetClearOperationHandlerInstance(qm.gatewayDB, qm.routerDB, qm.batchRouterDB)
	}

	return nil
}
