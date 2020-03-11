package pgnotifier

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	uuid "github.com/satori/go.uuid"
)

var (
	queueName               string
	subscriberInfoTableName string
)

const (
	WaitingState   = "waiting"
	ExecutingState = "executing"
	SucceededState = "succeeded"
	FailedState    = "failed"
)

func init() {
	queueName = "pg_notifier_queue"
	subscriberInfoTableName = "pg_notifier_subscriber_info"
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
	BatchID string
	Data    json.RawMessage
}

type ResponseT struct {
	Payload json.RawMessage
}

type ClaimResponseT struct {
	Payload json.RawMessage
	Err     error
}

func New(connectionInfo string) (notifier PgNotifierT, err error) {
	logger.Infof("Initializaing PgNotifier...")
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
	err = notifier.createTrigger(topic)
	if err != nil {
		return
	}
	rruntime.Go(func() {
		func() {
			notifier.triggerPending(topic)
		}()
	})
	return
}

func (notifier *PgNotifierT) triggerPending(topic string) {
	for {
		time.Sleep(2 * time.Second)
		stmt := fmt.Sprintf(`UPDATE %[1]s SET status='%[3]s',
								updated_at = '%[2]s'
								WHERE id IN  (
								SELECT id
								FROM %[1]s
								WHERE status='%[3]s'
								ORDER BY id
								FOR UPDATE SKIP LOCKED
								LIMIT %[4]v
								);`,
			queueName,
			GetCurrentSQLTimestamp(),
			WaitingState,
			10)
		fmt.Printf("%+v\n", stmt)
		_, err := notifier.dbHandle.Exec(stmt)
		if err != nil {
			panic(err)
		}
	}
}

func (notifier *PgNotifierT) trackBatch(batchID string, ch *chan []ResponseT) {
	rruntime.Go(func() {
		func() {
			for {
				time.Sleep(2 * time.Second)
				// keep polling db for batch status
				// or subscribe to triggers
				stmt := fmt.Sprintf(`SELECT count(*) FROM %s WHERE batch_id='%s' AND (status='%s' OR status='%s')`, queueName, batchID, WaitingState, ExecutingState)
				var count int
				fmt.Printf("%+v\n", stmt)
				err := notifier.dbHandle.QueryRow(stmt).Scan(&count)
				if err != nil {
					panic(err)
				}
				if count == 0 {
					stmt = fmt.Sprintf(`SELECT payload FROM %s WHERE batch_id = '%s'`, queueName, batchID)
					rows, err := notifier.dbHandle.Query(stmt)
					if err != nil {
						panic(err)
					}
					defer rows.Close()
					responses := []ResponseT{}
					for rows.Next() {
						var payload json.RawMessage
						err = rows.Scan(&payload)
						responses = append(responses, ResponseT{
							Payload: payload,
						})
					}
					*ch <- responses
					break
				}
			}
		}()
	})
}

func (notifier *PgNotifierT) updateClaimedEvent(id int64, tx *sql.Tx, ch chan ClaimResponseT) {
	rruntime.Go(func() {
		func() {
			x := <-ch
			fmt.Println("*******updating claimed event")
			// fmt.Printf("%+v\n", x)
			var err error
			if x.Err != nil {
				stmt := fmt.Sprintf(`UPDATE %[1]s SET status='%[2]s', updated_at = '%[3]s', error = '%[4]s' WHERE id = %[5]v`, queueName, FailedState, GetCurrentSQLTimestamp(), x.Err.Error(), id)
				_, err = tx.Exec(stmt)
			}
			stmt := fmt.Sprintf(`UPDATE %[1]s SET status='%[2]s', updated_at = '%[3]s', payload = '%[4]s' WHERE id = %[5]v`, queueName, SucceededState, GetCurrentSQLTimestamp(), x.Payload, id)
			_, err = tx.Exec(stmt)

			if err == nil {
				tx.Commit()
			}
			fmt.Printf("%+v\n", err)
		}()
	})
}

func (notifier *PgNotifierT) Claim(id int64) (ch chan ClaimResponseT, claimed bool) {
	//Begin Transaction
	tx, err := notifier.dbHandle.Begin()
	if err != nil {
		return
	}

	var claimedID int64
	// Dont panic if acquire fails -- Just rollback & return the worker to free state again
	stmt := fmt.Sprintf(`UPDATE %[1]s SET status='%[2]s',
						updated_at = '%[3]s',
						last_exec_time = '%[3]s'
						WHERE id = (
						SELECT id
						FROM %[1]s
						WHERE status='%[4]s' AND id = %[5]d
						ORDER BY id
						FOR UPDATE SKIP LOCKED
						LIMIT 1
						)
						RETURNING id;`, queueName, ExecutingState, GetCurrentSQLTimestamp(), WaitingState, id)
	fmt.Println(stmt)
	err = tx.QueryRow(stmt).Scan(&claimedID)

	if err != nil {
		fmt.Printf("%+v\n", err)
		fmt.Println("^^^^^^^^^^ not claimed")
		return
	}

	fmt.Println("%%%%%%%")
	fmt.Println("claimedId: ", claimedID)
	fmt.Println("%%%%%%%")
	ch = make(chan ClaimResponseT, 1)
	notifier.updateClaimedEvent(id, tx, ch)
	return ch, true
}

func (notifier *PgNotifierT) Publish(topic string, messages []MessageT) (ch chan []ResponseT, err error) {
	ch = make(chan []ResponseT)

	//Using transactions for bulk copying
	txn, err := notifier.dbHandle.Begin()
	if err != nil {
		return
	}

	stmt, err := txn.Prepare(pq.CopyIn(queueName, "batch_id", "status", "topic", "payload", "created_at", "updated_at"))
	if err != nil {
		fmt.Println("%%%%%%%%%%1")
		return
	}
	defer stmt.Close()

	batchID := uuid.NewV4().String()
	for _, message := range messages {
		_, err = stmt.Exec(batchID, WaitingState, topic, message.Payload, time.Now(), time.Now())
		if err != nil {
			fmt.Println("%%%%%%%%%%2")
			return
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		fmt.Println("%%%%%%%%%%3")
		return
	}
	err = txn.Commit()
	if err != nil {
		fmt.Println("%%%%%%%%%%4")
		return
	}
	notifier.trackBatch(batchID, &ch)
	return
}

func (notifier *PgNotifierT) Subscribe(topic string) (ch chan NotificationT, err error) {
	//Create a listener & start listening -- TODO: check if panic is required
	listener := pq.NewListener(notifier.URI,
		10*time.Second,
		time.Minute,
		func(ev pq.ListenerEventType, err error) {
			if err != nil {
				logger.Errorf("Error in pq listener for event type: %v %v ", ev, err)
			}
		})
	err = listener.Listen(topic)
	if err != nil {
		return
	}

	ch = make(chan NotificationT)

	rruntime.Go(func() {
		func() {
			for {
				select {
				case n := <-listener.Notify:
					fmt.Println("Received data from channel [", n.Channel, "] :")
					var c NotificationT
					err = json.Unmarshal([]byte(n.Extra), &c)
					if err != nil {
						panic(err)
					}
					fmt.Printf("%+v\n", c)
					// fmt.Println(string(prettyJSON.Bytes()))
					ch <- c
					// return

				case <-time.After(90 * time.Second):
					logger.Infof("WH-JQ: Received no events for 90 seconds, checking connection")
					go func() {
						listener.Ping()
					}()
				}
			}
		}()
	})
	return
}

func (notifier *PgNotifierT) createTrigger(topic string) (err error) {
	//create a postgres function that notifies on the specified channel
	// TODO: Use `REPLACE FUNCTION`
	sqlStmt := fmt.Sprintf(`DO $$
							BEGIN
							IF  NOT EXISTS (select  from pg_proc where proname = 'pgnotifier_notify') THEN
								CREATE FUNCTION pgnotifier_notify() RETURNS TRIGGER AS '
								DECLARE
									notification json;
								BEGIN
									-- Contruct the notification as a JSON string.
									notification = json_build_object(
													''id'', NEW.id,
													''batch_id'', NEW.batch_id,
													''data'', NEW.payload);

									-- Execute pg_notify(channel, notification)
									PERFORM pg_notify(''%s'',notification::text);

									-- Result is ignored since this is an AFTER trigger
									RETURN NULL;
								END;' LANGUAGE plpgsql;

							END IF;

							END $$  `, topic)

	_, err = notifier.dbHandle.Exec(sqlStmt)
	if err != nil {
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
	logger.Infof("WH-JQ: Creating Job Queue Tables ")

	//create status type
	sqlStmt := `DO $$ BEGIN
						CREATE TYPE pg_notifier_status_type
							AS ENUM(
								'waiting',
								'executing',
								'succeeded',
								'failed'
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
										  staging_file_id BIGINT,
										  batch_id VARCHAR(64) NOT NULL,
										  status pg_notifier_status_type NOT NULL,
										  topic VARCHAR(64) NOT NULL,
										  worker_id VARCHAR(64),
										  payload JSONB NOT NULL,
										  error_count INT DEFAULT 0,
										  created_at TIMESTAMP NOT NULL,
										  updated_at TIMESTAMP NOT NULL,
										  last_exec_time TIMESTAMP,
										  error VARCHAR(64),
										  last_error VARCHAR(512));`, queueName)

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

	//create the worker info table
	sqlStmt = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id BIGSERIAL PRIMARY KEY,
			subscriber_id VARCHAR(64)  NOT NULL UNIQUE,
			status pg_notifier_subscriber_status NOT NULL,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL);`, subscriberInfoTableName)
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
