package pgnotifier

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	uuid "github.com/satori/go.uuid"
)

var (
	queueName                      string
	maxAttempt                     int
	retriggerInterval              time.Duration
	retriggerCount                 int
	retriggerExecutingTimeLimitInS int
	trackBatchInterval             time.Duration
	pkgLogger                      logger.LoggerI
)

var (
	pgNotifierDBhost, pgNotifierDBuser, pgNotifierDBpassword, pgNotifierDBname, pgNotifierDBsslmode string
	pgNotifierDBport                                                                                int
)

const (
	WaitingState   = "waiting"
	ExecutingState = "executing"
	SucceededState = "succeeded"
	FailedState    = "failed"
	AbortedState   = "aborted"
)

func init() {
	loadPGNotifierConfig()
	queueName = "pg_notifier_queue"
	maxAttempt = config.GetInt("PgNotifier.maxAttempt", 3)
	trackBatchInterval = time.Duration(config.GetInt("PgNotifier.trackBatchIntervalInS", 2)) * time.Second
	retriggerInterval = time.Duration(config.GetInt("PgNotifier.retriggerIntervalInS", 2)) * time.Second
	retriggerCount = config.GetInt("PgNotifier.retriggerCount", 500)
	retriggerExecutingTimeLimitInS = config.GetInt("PgNotifier.retriggerExecutingTimeLimitInS", 120)
	pkgLogger = logger.NewLogger().Child("warehouse").Child("pgnotifier")
}

type PgNotifierT struct {
	URI                 string
	dbHandle            *sql.DB
	workspaceIdentifier string
}

type MessageT struct {
	Payload json.RawMessage
}

type NotificationT struct {
	ID      int64
	BatchID string `json:"batch_id"`
	Status  string
}

type ResponseT struct {
	JobID  int64
	Status string
	Output json.RawMessage
	Error  string
}

type ClaimT struct {
	ID                int64
	BatchID           string
	Status            string
	Payload           json.RawMessage
	ClaimResponseChan chan ClaimResponseT
}

type ClaimResponseT struct {
	Payload json.RawMessage
	Err     error
}

func loadPGNotifierConfig() {
	pgNotifierDBhost = config.GetEnv("PGNOTIFIER_DB_HOST", "localhost")
	pgNotifierDBuser = config.GetEnv("PGNOTIFIER_DB_USER", "ubuntu")
	pgNotifierDBname = config.GetEnv("PGNOTIFIER_DB_NAME", "ubuntu")
	pgNotifierDBport, _ = strconv.Atoi(config.GetEnv("PGNOTIFIER_DB_PORT", "5432"))
	pgNotifierDBpassword = config.GetEnv("PGNOTIFIER_DB_PASSWORD", "ubuntu") // Reading secrets from
	pgNotifierDBsslmode = config.GetEnv("PGNOTIFIER_DB_SSL_MODE", "disable")
}

//New Given default connection info return pg notifiew object from it
func New(workspaceIdentifier string, fallbackConnectionInfo string) (notifier PgNotifierT, err error) {

	// by default connection info is fallback connection info
	connectionInfo := fallbackConnectionInfo

	// if PG Notifier variables are defined then use get values provided in env vars
	if CheckForPGNotifierEnvVars() {
		connectionInfo = GetPGNotifierConnectionString()
	}
	pkgLogger.Infof("PgNotifier: Initializing PgNotifier...")
	dbHandle, err := sql.Open("postgres", connectionInfo)
	if err != nil {
		return
	}
	notifier = PgNotifierT{
		dbHandle:            dbHandle,
		URI:                 connectionInfo,
		workspaceIdentifier: workspaceIdentifier,
	}
	err = notifier.setupQueue()
	return
}

func (notifier PgNotifierT) GetDBHandle() *sql.DB {
	return notifier.dbHandle
}

func (notifier PgNotifierT) AddTopic(topic string) (err error) {

	// clean up all jobs in pgnotifier for same workspace
	// additional safety check to not delete all jobs with empty workspaceIdentifier
	if notifier.workspaceIdentifier != "" {
		stmt := fmt.Sprintf("DELETE FROM %s WHERE workspace='%s' AND topic ='%s'", queueName, notifier.workspaceIdentifier, topic)
		pkgLogger.Infof("PgNotifier: Deleting all jobs on topic: %s", topic)
		_, err = notifier.dbHandle.Exec(stmt)
		if err != nil {
			return
		}
	}
	err = notifier.createTrigger(topic)
	if err != nil {
		return
	}
	rruntime.Go(func() {
		notifier.triggerPending(topic)
	})
	return
}

// CheckForPGNotifierEnvVars Checks if all the required Env Variables for PG Notifier are present
func CheckForPGNotifierEnvVars() bool {
	return config.IsEnvSet("PGNOTIFIER_DB_HOST") &&
		config.IsEnvSet("PGNOTIFIER_DB_USER") &&
		config.IsEnvSet("PGNOTIFIER_DB_NAME") &&
		config.IsEnvSet("PGNOTIFIER_DB_PASSWORD")
}

// GetPGNotifierConnectionString Returns PG Notifier DB Connection Configuration
func GetPGNotifierConnectionString() string {
	pkgLogger.Debugf("WH: All Env variables required for separate PG Notifier are set... Check pg notifier says True...")
	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=%s",
		pgNotifierDBhost, pgNotifierDBport, pgNotifierDBuser,
		pgNotifierDBpassword, pgNotifierDBname, pgNotifierDBsslmode)
}

func (notifier *PgNotifierT) triggerPending(topic string) {
	for {
		time.Sleep(retriggerInterval)
		stmt := fmt.Sprintf(`UPDATE %[1]s SET status='%[3]s',
								updated_at = '%[2]s'
								WHERE id IN (
									SELECT id FROM %[1]s
									WHERE status='%[3]s' OR status='%[4]s' OR (status='%[5]s' AND last_exec_time <= NOW() - INTERVAL '%[6]v seconds')
									AND workspace='%[7]s'
									ORDER BY id
									FOR UPDATE SKIP LOCKED
									LIMIT %[8]v
								) RETURNING id`,
			queueName,
			GetCurrentSQLTimestamp(),
			WaitingState,
			FailedState,
			ExecutingState,
			retriggerExecutingTimeLimitInS,
			notifier.workspaceIdentifier,
			retriggerCount)
		pkgLogger.Debugf("PgNotifier: triggering pending jobs: %v", stmt)
		rows, err := notifier.dbHandle.Query(stmt)
		if err != nil {
			panic(err)
		}
		var ids []int64
		for rows.Next() {
			var id int64
			err := rows.Scan(&id)
			if err != nil {
				pkgLogger.Errorf("PgNotifier: Error scanning returned id from retriggered jobs: %v", err)
				continue
			}
			ids = append(ids, id)
		}
		rows.Close()
		pkgLogger.Debugf("PgNotifier: Retriggerd job ids: %v", ids)
	}
}

func (notifier *PgNotifierT) trackBatch(batchID string, ch *chan []ResponseT) {
	rruntime.Go(func() {
		for {
			time.Sleep(trackBatchInterval)
			// keep polling db for batch status
			// or subscribe to triggers
			stmt := fmt.Sprintf(`SELECT count(*) FROM %s WHERE batch_id='%s' AND status!='%s' AND status!='%s'`, queueName, batchID, SucceededState, AbortedState)
			var count int
			err := notifier.dbHandle.QueryRow(stmt).Scan(&count)
			if err != nil {
				pkgLogger.Errorf("PgNotifier: Failed to query for tracking jobs by batch_id: %s, connInfo: %s", stmt, notifier.URI)
				panic(err)
			}
			if count == 0 {
				stmt = fmt.Sprintf(`SELECT payload->'StagingFileID', payload->'Output', status, error FROM %s WHERE batch_id = '%s'`, queueName, batchID)
				rows, err := notifier.dbHandle.Query(stmt)
				if err != nil {
					panic(err)
				}
				responses := []ResponseT{}
				for rows.Next() {
					var status, jobError, output sql.NullString
					var jobID int64
					err = rows.Scan(&jobID, &output, &status, &jobError)
					if err != nil {
						panic(fmt.Errorf("Failed to scan result from query: %s\nwith Error : %w", stmt, err))
					}
					responses = append(responses, ResponseT{
						JobID:  jobID,
						Output: []byte(output.String),
						Status: status.String,
						Error:  jobError.String,
					})
				}
				rows.Close()
				*ch <- responses
				pkgLogger.Infof("PgNotifier: Completed processing all files  in batch: %s", batchID)
				stmt = fmt.Sprintf(`DELETE FROM %s WHERE batch_id = '%s'`, queueName, batchID)
				_, err = notifier.dbHandle.Exec(stmt)
				if err != nil {
					pkgLogger.Errorf("PgNotifier: Error deleting from %s for batch_id:%s : %v", queueName, batchID, err)
				}
				break
			} else {
				pkgLogger.Debugf("PgNotifier: Pending %d files to process in batch: %s", count, batchID)
			}
		}
	})
}

func (notifier *PgNotifierT) updateClaimedEvent(id int64, ch chan ClaimResponseT) {
	rruntime.Go(func() {
		response := <-ch
		var err error
		if response.Err != nil {
			pkgLogger.Error(response.Err.Error())
			stmt := fmt.Sprintf(`UPDATE %[1]s SET status=(CASE
									WHEN attempt > %[2]d
									THEN CAST ( '%[3]s' AS pg_notifier_status_type)
									ELSE  CAST( '%[4]s' AS pg_notifier_status_type)
									END), attempt = attempt + 1, updated_at = '%[5]s', error = %[6]s
									WHERE id = %[7]v`, queueName, maxAttempt, AbortedState, FailedState, GetCurrentSQLTimestamp(), misc.QuoteLiteral(response.Err.Error()), id)
			_, err = notifier.dbHandle.Exec(stmt)
		} else {
			stmt := fmt.Sprintf(`UPDATE %[1]s SET status='%[2]s', updated_at = '%[3]s', payload = $1 WHERE id = %[4]v`, queueName, SucceededState, GetCurrentSQLTimestamp(), id)
			_, err = notifier.dbHandle.Exec(stmt, response.Payload)
		}

		if err != nil {
			// TODO: abort this job or raise metric and alert
			pkgLogger.Errorf("PgNotifier: Failed to update claimed event: %v", err)
		}
	})
}

func (notifier *PgNotifierT) Claim(workerID string) (claim ClaimT, claimed bool) {
	var claimedID int64
	var batchID, status string
	var payload json.RawMessage
	stmt := fmt.Sprintf(`UPDATE %[1]s SET status='%[2]s',
						updated_at = '%[3]s',
						last_exec_time = '%[3]s',
						worker_id = '%[4]v'
						WHERE id = (
						SELECT id
						FROM %[1]s
						WHERE status='%[5]s' OR status='%[6]s'
						ORDER BY id
						FOR UPDATE SKIP LOCKED
						LIMIT 1
						)
						RETURNING id, batch_id, status, payload;`, queueName, ExecutingState, GetCurrentSQLTimestamp(), workerID, WaitingState, FailedState)

	tx, err := notifier.dbHandle.Begin()
	if err != nil {
		return
	}
	err = tx.QueryRow(stmt).Scan(&claimedID, &batchID, &status, &payload)

	if err != nil {
		pkgLogger.Debugf("PgNotifier: Claim failed: %v, query: %s, connInfo: %s", err, stmt, notifier.URI)
		tx.Rollback()
		return
	}

	err = tx.Commit()

	if err != nil {
		pkgLogger.Errorf("PgNotifier: Error commiting claim txn: %v", err)
		tx.Rollback()
		return
	}

	responseChan := make(chan ClaimResponseT, 1)
	claim = ClaimT{
		ID:                claimedID,
		BatchID:           batchID,
		Status:            status,
		Payload:           payload,
		ClaimResponseChan: responseChan,
	}
	notifier.updateClaimedEvent(claimedID, responseChan)
	return claim, true
}

func (notifier *PgNotifierT) Publish(topic string, messages []MessageT) (ch chan []ResponseT, err error) {
	ch = make(chan []ResponseT)

	//Using transactions for bulk copying
	txn, err := notifier.dbHandle.Begin()
	if err != nil {
		return
	}

	stmt, err := txn.Prepare(pq.CopyIn(queueName, "batch_id", "status", "topic", "payload", "workspace"))
	if err != nil {
		return
	}
	defer stmt.Close()

	batchID := uuid.NewV4().String()
	pkgLogger.Infof("PgNotifier: Inserting %d records into %s as batch: %s", len(messages), queueName, batchID)
	for _, message := range messages {
		_, err = stmt.Exec(batchID, WaitingState, topic, string(message.Payload), notifier.workspaceIdentifier)
		if err != nil {
			return
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		pkgLogger.Errorf("PgNotifier: Error publishing messages: %v", err)
		return
	}
	err = txn.Commit()
	if err != nil {
		pkgLogger.Errorf("PgNotifier: Error in publishing messages: %v", err)
		return
	}
	pkgLogger.Infof("PgNotifier: Inserted %d records into %s as batch: %s", len(messages), queueName, batchID)
	notifier.trackBatch(batchID, &ch)
	return
}

func (notifier *PgNotifierT) Subscribe(topic string) (ch chan NotificationT, err error) {
	//Create a listener & start listening -- TODO: check if panic is required
	listener := pq.NewListener(notifier.URI,
		10*time.Second,
		time.Minute,
		func(ev pq.ListenerEventType, err error) {
			pkgLogger.Debugf("PgNotifier: Event received in pq listener %v", ev)
			if err != nil {
				pkgLogger.Debugf("PgNotifier: Error in pq listener for event type: %v %v ", ev, err)
			}
		})
	err = listener.Listen(topic)
	if err != nil {
		return
	}

	ch = make(chan NotificationT)
	rruntime.Go(func() {
		for {
			select {
			case notification := <-listener.Notify:
				if notification != nil {
					var event NotificationT
					err = json.Unmarshal([]byte(notification.Extra), &event)
					if err != nil {
						panic(err)
					}
					pkgLogger.Debugf("PgNotifier: Received data from channel: %s, data: %v", notification.Channel, event)
					if event.Status == WaitingState || event.Status == FailedState {
						ch <- event
					} else {
						pkgLogger.Debugf("PgNotifier: Not notifying subscriber for event with id: %d type: %s", event.ID, event.Status)
					}
				}
			case <-time.After(90 * time.Second):
				pkgLogger.Debugf("PgNotifier: Received no events for 90 seconds, checking connection")
				go func() {
					listener.Ping()
				}()
			}
		}
	})
	return
}

func (notifier *PgNotifierT) createTrigger(topic string) (err error) {
	//create a postgres function that notifies on the specified channel
	sqlStmt := fmt.Sprintf(`DO $$
							BEGIN
							CREATE OR REPLACE FUNCTION pgnotifier_notify() RETURNS TRIGGER AS '
								DECLARE
									notification json;
								BEGIN
									-- Contruct the notification as a JSON string.
									notification = json_build_object(
													''id'', NEW.id,
													''batch_id'', NEW.batch_id,
													''status'', NEW.status);

									-- Execute pg_notify(channel, notification)
									PERFORM pg_notify(''%s'',notification::text);

									-- Result is ignored since this is an AFTER trigger
									RETURN NULL;
								END;' LANGUAGE plpgsql;
							END $$  `, topic)

	_, err = notifier.dbHandle.Exec(sqlStmt)
	if err != nil {
		pkgLogger.Errorf("PgNotifier: Error creating trigger func: %v", err)
		return
	}

	//create the trigger
	sqlStmt = fmt.Sprintf(`DO $$ BEGIN
									CREATE TRIGGER %[1]s_status_trigger
											AFTER INSERT OR UPDATE OF status
											ON %[1]s
											FOR EACH ROW
										EXECUTE PROCEDURE pgnotifier_notify();
									EXCEPTION
										WHEN others THEN null;
								END $$`, queueName)

	_, err = notifier.dbHandle.Exec(sqlStmt)
	return
}

func (notifier *PgNotifierT) setupQueue() (err error) {
	pkgLogger.Infof("PgNotifier: Creating Job Queue Tables ")

	//create status type
	sqlStmt := `DO $$ BEGIN
						CREATE TYPE pg_notifier_status_type
							AS ENUM(
								'waiting',
								'executing',
								'succeeded',
								'failed',
								'aborted'
									);
							EXCEPTION
								WHEN duplicate_object THEN null;
					END $$;`

	_, err = notifier.dbHandle.Exec(sqlStmt)
	if err != nil {
		return
	}

	//create the job queue table
	sqlStmt = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
										  id BIGSERIAL PRIMARY KEY,
										  batch_id VARCHAR(64) NOT NULL,
										  status pg_notifier_status_type NOT NULL,
										  topic VARCHAR(64) NOT NULL,
										  payload JSONB NOT NULL,
										  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
										  updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
										  last_exec_time TIMESTAMP,
										  attempt SMALLINT DEFAULT 0,
										  error TEXT,
										  worker_id VARCHAR(64));`, queueName)

	_, err = notifier.dbHandle.Exec(sqlStmt)
	if err != nil {
		return
	}

	sqlStmt = fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS workspace VARCHAR(64)`, queueName)
	_, err = notifier.dbHandle.Exec(sqlStmt)
	if err != nil {
		return
	}

	// create index on status
	sqlStmt = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_status_idx ON %[1]s (status);`, queueName)
	_, err = notifier.dbHandle.Exec(sqlStmt)
	if err != nil {
		return
	}

	// create index on batch_id
	sqlStmt = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_batch_id_idx ON %[1]s (batch_id);`, queueName)
	_, err = notifier.dbHandle.Exec(sqlStmt)
	if err != nil {
		return
	}

	// create index on workspace, topic
	sqlStmt = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_workspace_topic_idx ON %[1]s (workspace, topic);`, queueName)
	_, err = notifier.dbHandle.Exec(sqlStmt)
	if err != nil {
		return
	}

	//create status type for worker
	sqlStmt = `DO $$ BEGIN
						CREATE TYPE pg_notifier_subscriber_status
							AS ENUM(
								'busy',
								'free' );
							EXCEPTION
								WHEN duplicate_object THEN null;
					END $$;`

	_, err = notifier.dbHandle.Exec(sqlStmt)
	if err != nil {
		return
	}

	return
}

//GetCurrentSQLTimestamp to get sql complaint current datetime string
func GetCurrentSQLTimestamp() string {
	const SQLTimeFormat = "2006-01-02 15:04:05"
	return time.Now().Format(SQLTimeFormat)
}

//GetSQLTimestamp to get sql complaint current datetime string from the given duration
func GetSQLTimestamp(t time.Time) string {
	const SQLTimeFormat = "2006-01-02 15:04:05"
	return t.Format(SQLTimeFormat)
}
