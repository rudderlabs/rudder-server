package rsources

import (
	"context"
	"database/sql"
)

type multitenantExtension struct {
	*defaultExtension
	sharedDB *sql.DB
}

func (r *multitenantExtension) getReadDB() *sql.DB {
	return r.sharedDB
}

func (r *multitenantExtension) createStatsTable(ctx context.Context, jobRunId string) error {
	err := r.defaultExtension.createStatsTable(ctx, jobRunId)
	if err != nil {
		return err
	}
	// ## On local DB
	// 1. Create publication, 1 publication for all tables (ignore already exists error)
	// 2. Add table to publication if not already (ignore is already member of publication error)

	// ## On remote DB
	// 1. Create subscription, 1 subscription for all tables (ignore already exists error)
	// 2. Create table if not exists
	// 2. Add table to subscription if not already (ignore is already member of subscription error)
	return nil
}

func (r *multitenantExtension) dropTables(ctx context.Context, jobRunId string) error {
	err := r.defaultExtension.dropTables(ctx, jobRunId)
	if err != nil {
		return err
	}
	// ## On local DB
	// 1. Remove table from publication

	// ## On remote DB
	// 1. Remove table from subscription

	return nil
}

func (*multitenantExtension) cleanupLoop(_ context.Context) error {
	// TODO
	return nil
}

func (*multitenantExtension) doCleanupTables(_ context.Context) error {
	// TODO
	return nil

}
