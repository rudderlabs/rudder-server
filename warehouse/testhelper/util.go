package testhelper

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/cenkalti/backoff"
)

func JsonEscape(i string) (string, error) {
	b, err := json.Marshal(i)
	if err != nil {
		return "", fmt.Errorf("could not escape big query JSON credentials for workspace config with error: %s", err.Error())
	}
	return strings.Trim(string(b), `"`), nil
}

func ConnectWithBackoff(operation func() error) {
	var err error

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewConstantBackOff(ConnectBackoffDuration), uint64(ConnectBackoffRetryMax))
	if err = backoff.Retry(operation, backoffWithMaxRetry); err != nil {
		log.Panicf("could not connect to warehouse with error: %s", err.Error())
	}
}

func GWJobsForUserIdWriteKey() string {
	return `CREATE OR REPLACE FUNCTION gw_jobs_for_user_id_and_write_key(user_id varchar, write_key varchar)
								RETURNS TABLE
										(
											job_id varchar
										)
							AS
							$$
							DECLARE
								table_record RECORD;
								batch_record jsonb;
							BEGIN
								FOR table_record IN SELECT * FROM gw_jobs_1 where (event_payload ->> 'writeKey') = write_key
									LOOP
										FOR batch_record IN SELECT * FROM jsonb_array_elements((table_record.event_payload ->> 'batch')::jsonb)
											LOOP
												if batch_record ->> 'userId' != user_id THEN
													CONTINUE;
												END IF;
												job_id := table_record.job_id;
												RETURN NEXT;
												EXIT;
											END LOOP;
									END LOOP;
							END;
							$$ LANGUAGE plpgsql`
}

func BRTJobsForUserId() string {
	return `CREATE OR REPLACE FUNCTION brt_jobs_for_user_id(user_id varchar)
									RETURNS TABLE
											(
												job_id varchar
											)
								AS
								$$
								DECLARE
									table_record  RECORD;
									event_payload jsonb;
								BEGIN
									FOR table_record IN SELECT * FROM batch_rt_jobs_1
										LOOP
											event_payload = (table_record.event_payload ->> 'data')::jsonb;
											if event_payload ->> 'user_id' = user_id Or event_payload ->> 'id' = user_id Or event_payload ->> 'USER_ID' = user_id Or event_payload ->> 'ID' = user_id THEN
												job_id := table_record.job_id;
												RETURN NEXT;
											END IF;
										END LOOP;
								END ;
								$$ LANGUAGE plpgsql`
}

func DefaultEventMap() EventsCountMap {
	return EventsCountMap{
		"identifies": 1,
		"users":      1,
		"tracks":     1,
		"pages":      1,
		"screens":    1,
		"aliases":    1,
		"groups":     1,
		"gateway":    6,
		"batchRT":    8,
	}
}
