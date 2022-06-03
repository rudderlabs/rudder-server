package extension

import (
	"context"
	"database/sql"

	"github.com/lib/pq"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

type multitenantExtension struct {
	Extension
	sharedDB *sql.DB
}

var _ Extension = (*multitenantExtension)(nil)

func NewMultitenantExtension(db, readDB *sql.DB) (Extension, error) {
	defExt, err := NewStandardExtension(db)
	if err != nil {
		return nil, err
	}
	multiExt := &multitenantExtension{
		Extension: defExt,
		sharedDB:  readDB,
	}
	err = multiExt.setupStatsTable(context.Background())
	return multiExt, err
}

func (r *multitenantExtension) GetReadDB() *sql.DB {
	return r.sharedDB
}

func (r *multitenantExtension) setupStatsTable(ctx context.Context) error {

	// ## On local DB

	// Create publication, 1 publication for the stats table (ignore already exists error)
	connectionName := config.GetEnv("JOBS_DB_HOST", "localhost") //using hostname
	publicationCreationQuery := `CREATE PUBLICATION "$1" FOR TABLE rsources_stats`
	_, err := r.Extension.(*standardExtension).localDB.ExecContext(ctx, publicationCreationQuery, connectionName)
	if err != nil {
		pqError := err.(pq.Error)
		if pqError.Code != pq.ErrorCode("42710") { // duplicate
			return err
		}
	}

	// ## On remote DB

	// Create table if not exists

	// using this dummy-extension to create table in remote db
	// TODO verify this is safe
	dummyStandardExtension := &standardExtension{
		localDB: r.sharedDB,
	}
	err = dummyStandardExtension.setupStatsTable(ctx) //check if dbname column defaults to proper value(not remote db hostname)
	if err != nil {
		return err
	}

	// Create subscription for the above publication (ignore already exists error)
	subscriptionCreationQuery := `CREATE SUBSCRIPTION $2 CONNECTION $1 PUBLICATION $2`
	_, err = r.sharedDB.ExecContext(ctx, subscriptionCreationQuery, jobsdb.GetConnectionString(), connectionName)
	if err != nil {
		pqError := err.(pq.Error)
		if pqError.Code != pq.ErrorCode("42710") { // duplicate
			return err
		}
	}

	return nil
}

func (r *multitenantExtension) DropStats(ctx context.Context, jobRunId string) error {
	return r.Extension.DropStats(ctx, jobRunId)
}

func (r *multitenantExtension) CleanupLoop(ctx context.Context) error {
	// if all workspaces are to be treated equally we can simply go this way
	return r.Extension.CleanupLoop(ctx)

	// TODO: in case CleanupLoop to be called wrt to a specific workspace(job_run_id)
}
