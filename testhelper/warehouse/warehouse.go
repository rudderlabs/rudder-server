package warehouse_test

import (
	"database/sql"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/mssql"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"math/rand"
)

type EventsCountMap map[string]int

type WareHouseDestinationTest struct {
	DB             *sql.DB
	EventsCountMap EventsCountMap
	WriteKey       string
	UserId         string
	Schema         string
}

type WareHouseTest struct {
	PGTest                     *PostgresTest
	CHTest                     *ClickHouseTest
	CHClusterTest              *ClickHouseClusterTest
	MSSQLTest                  *MSSQLTest
	GatewayJobsSqlFunction     string
	BatchRouterJobsSqlFunction string
}

var (
	Test *WareHouseTest
)

func randString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

// InitWHConfig Initialize warehouse config
func InitWHConfig() {
	Test = &WareHouseTest{}
	Test.GatewayJobsSqlFunction = `CREATE OR REPLACE FUNCTION gw_jobs_for_user_id_and_write_key(user_id varchar, write_key varchar)
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
	Test.BatchRouterJobsSqlFunction = `CREATE OR REPLACE FUNCTION brt_jobs_for_user_id(user_id varchar)
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
											if event_payload ->> 'user_id' = user_id Or event_payload ->> 'id' = user_id THEN
												job_id := table_record.job_id;
												RETURN NEXT;
											END IF;
										END LOOP;
								END ;
								$$ LANGUAGE plpgsql`

	config.Load()
	logger.Init()
	postgres.Init()
	clickhouse.Init()
	mssql.Init()
}
