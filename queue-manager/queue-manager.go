package queuemanager

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	QueueManager QueueManagerI
	webPort      int
	pkgLogger    logger.LoggerI
	dbHandle     *sql.DB
)

type OperationtT struct {
	ID           int64
	Operation    string
	EventPayload json.RawMessage
	done         bool
	CreatedAt    time.Time
}

type QueueManagerI interface {
	InsertOperation(payload []byte) error
}
type QueueManagerT struct {
}

func loadConfig() {
	//Port where GW is running
	webPort = config.GetInt("Gateway.webPort", 8080)
}

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("queuemanager")
}

func Setup() {
	pkgLogger.Info("setting up queuemanager.")
	var err error
	dbHandle, err = sql.Open("postgres", jobsdb.GetConnectionString())
	if err != nil {
		panic(fmt.Errorf("failed to connect to DB. Err: %v", err))
	}
	QueueManager = new(QueueManagerT)
}

func (qm *QueueManagerT) InsertOperation(payload []byte) error {
	sqlStatement := `INSERT INTO operations (operation, payload, done)
	VALUES ($1, $2, $3)
	RETURNING id`

	stmt, err := dbHandle.Prepare(sqlStatement)
	if err != nil {
		return err
	}

	row := stmt.QueryRow(sqlStatement, "CLEAR", payload, false)
	var opID int64
	err = row.Scan(&opID)
	if err != nil {
		return err
	}

	return nil
}
