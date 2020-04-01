package pgnotifier

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	uuid "github.com/satori/go.uuid"
)

var (
	queueName          string
	maxAttempt         int
	retriggerInterval  time.Duration
	retriggerCount     int
	trackBatchInterval time.Duration
)

const (
	WaitingState   = "waiting"
	ExecutingState = "executing"
	SucceededState = "succeeded"
	FailedState    = "failed"
	AbortedState   = "aborted"
)

func init() {
	queueName = "pg_notifier_queue"
	maxAttempt = config.GetInt("PgNotifier.maxAttempt", 3)
	trackBatchInterval = time.Duration(config.GetInt("PgNotifier.trackBatchIntervalInS", 2)) * time.Second
	retriggerInterval = time.Duration(config.GetInt("PgNotifier.retriggerIntervalInS", 2)) * time.Second
	retriggerCount = config.GetInt("PgNotifier.retriggerCount", 100)
}

type PgNotifierT struct {
	URI      string
	dbHandle *sql.DB
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
	Status  string
	Payload json.RawMessage
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

func New(connectionInfo string) (notifier PgNotifierT, err error) {
	logger.Infof("PgNotifier: Initializaing PgNotifier...")
	dbHandle, err := sql.Open("postgres", connectionInfo)
	if err != nil {
		return
	}
	notifier = PgNotifierT{
		dbHandle: dbHandle,
		URI:      connectionInfo,
	}
	err = notifier.setupQueue()
	return
}

func (notifier PgNotifierT) AddTopic(topic string) (err error) {
	stmt := fmt.Sprintf("DELETE FROM %s WHERE topic ='%s'", queueName, topic)
	logger.Infof("PgNotifier: Deleting all jobs on topic: %s", topic)
	_, err = notifier.dbHandle.Exec(stmt)
	if err != nil {
		return
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

func (notifier *PgNotifierT) triggerPending(topic string) {
	for {
		time.Sleep(retriggerInterval)
		stmt := fmt.Sprintf(`UPDATE %[1]s SET status='%[3]s',
								updated_at = '%[2]s'
								WHERE id IN  (
								SELECT id
								FROM %[1]s
								WHERE status='%[3]s' OR status='%[4]s'
								ORDER BY id
								FOR UPDATE SKIP LOCKED
								LIMIT %[5]v
								);`,
			queueName,
			GetCurrentSQLTimestamp(),
			WaitingState,
			FailedState,
			retriggerCount)
		logger.Debugf("PgNotifier: triggering pending jobs")
		_, err := notifier.dbHandle.Exec(stmt)
		if err != nil {
			panic(err)
		}
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
				logger.Errorf("PgNotifier: Failed to query for tracking jobs by batch_id: %s, connInfo: %s", stmt, notifier.URI)
				panic(err)
			}
			if count == 0 {
				stmt = fmt.Sprintf(`SELECT payload, status FROM %s WHERE batch_id = '%s'`, queueName, batchID)
				rows, err := notifier.dbHandle.Query(stmt)
				if err != nil {
					panic(err)
				}
				defer rows.Close()
				responses := []ResponseT{}
				for rows.Next() {
					var payload json.RawMessage
					var status string
					err = rows.Scan(&payload, &status)
					responses = append(responses, ResponseT{
						Status:  status,
						Payload: payload,
					})
				}
				*ch <- responses
				break
			} else {
				logger.Infof("PgNotifier: Pending %d files to process in batch: %s", count, batchID)
			}
		}
	})
}

func (notifier *PgNotifierT) updateClaimedEvent(id int64, tx *sql.Tx, ch chan ClaimResponseT) {
	rruntime.Go(func() {
		response := <-ch
		var err error
		if response.Err != nil {
			stmt := fmt.Sprintf(`UPDATE %[1]s SET status=(CASE
									WHEN attempt > %[2]d
									THEN CAST ( '%[3]s' AS pg_notifier_status_type)
									ELSE  CAST( '%[4]s' AS pg_notifier_status_type)
									END), updated_at = '%[5]s', error = '%[6]s'
									WHERE id = %[7]v`, queueName, maxAttempt, AbortedState, FailedState, GetCurrentSQLTimestamp(), response.Err.Error(), id)
			_, err = tx.Exec(stmt)
		} else {
			stmt := fmt.Sprintf(`UPDATE %[1]s SET status='%[2]s', updated_at = '%[3]s', payload = '%[4]s' WHERE id = %[5]v`, queueName, SucceededState, GetCurrentSQLTimestamp(), response.Payload, id)
			_, err = tx.Exec(stmt)
		}

		if err == nil {
			tx.Commit()
		} else {
			// TODO: verify rollback is necessary on error
			tx.Rollback()
			logger.Errorf("PgNotifier: Failed to update claimed event: %v", err)
		}
	})
}

func (notifier *PgNotifierT) Claim(workerID string) (claim ClaimT, claimed bool) {
	//Begin Transaction
	tx, err := notifier.dbHandle.Begin()
	if err != nil {
		return
	}

	var claimedID int64
	var batchID, status string
	var payload json.RawMessage
	// Dont panic if acquire fails -- Just rollback & return the worker to free state again
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
	err = tx.QueryRow(stmt).Scan(&claimedID, &batchID, &status, &payload)

	if err != nil {
		logger.Errorf("PgNotifier: Claim failed: %v, query: %s, connInfo: %s", err, stmt, notifier.URI)
		// TODO: verify rollback is necessary on error
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
	notifier.updateClaimedEvent(claimedID, tx, responseChan)
	return claim, true
}

func (notifier *PgNotifierT) Publish(topic string, messages []MessageT) (ch chan []ResponseT, err error) {
	ch = make(chan []ResponseT)

	//Using transactions for bulk copying
	txn, err := notifier.dbHandle.Begin()
	if err != nil {
		return
	}

	stmt, err := txn.Prepare(pq.CopyIn(queueName, "batch_id", "status", "topic", "payload"))
	if err != nil {
		return
	}
	defer stmt.Close()

	batchID := uuid.NewV4().String()
	logger.Infof("PgNotifier: Inserting %d records into %s as batch: %s", len(messages), queueName, batchID)
	for _, message := range messages {
		_, err = stmt.Exec(batchID, WaitingState, topic, string(message.Payload))
		if err != nil {
			return
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		logger.Errorf("PgNotifier: Error publishing messages: %v", err)
		return
	}
	err = txn.Commit()
	if err != nil {
		logger.Errorf("PgNotifier: Error in publishing messages: %v", err)
		return
	}
	logger.Infof("PgNotifier: Inserted %d records into %s as batch: %s", len(messages), queueName, batchID)
	notifier.trackBatch(batchID, &ch)
	return
}

func (notifier *PgNotifierT) Subscribe(topic string) (ch chan NotificationT, err error) {
	//Create a listener & start listening -- TODO: check if panic is required
	listener := pq.NewListener(notifier.URI,
		10*time.Second,
		time.Minute,
		func(ev pq.ListenerEventType, err error) {
			logger.Debugf("PgNotifier: Event received in pq listener %v", ev)
			if err != nil {
				logger.Debugf("PgNotifier: Error in pq listener for event type: %v %v ", ev, err)
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
					logger.Debugf("PgNotifier: Received data from channel: %s, data: %v", notification.Channel, event)
					if event.Status == WaitingState || event.Status == FailedState {
						ch <- event
					} else {
						logger.Debugf("PgNotifier: Not notifying subsriber for event with id: %d type: %s", event.ID, event.Status)
					}
				}
			case <-time.After(90 * time.Second):
				logger.Debugf("PgNotifier: Received no events for 90 seconds, checking connection")
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
		logger.Errorf("PgNotifier: Error creatin trigger func: %v", err)
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
	logger.Infof("PgNotifier: Creating Job Queue Tables ")

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
										  error VARCHAR(64),
										  worker_id VARCHAR(64));`, queueName)

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
