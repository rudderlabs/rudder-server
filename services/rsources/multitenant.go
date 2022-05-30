package rsources

import (
	"context"
	"database/sql"
)

type multitenantExtension struct {
	*defaultExtension
	sharedDB *sql.DB
}

func newMultitenantExtension(db, readDB *sql.DB) (*multitenantExtension, error) {
	defExt, err := newDefaultExtension(db)
	if err != nil {
		return nil, err
	}
	multiExt := &multitenantExtension{
		defaultExtension: defExt,
		sharedDB:         readDB,
	}
	err = multiExt.setupStatsTable(context.Background())
	return multiExt, err
}

func (r *multitenantExtension) getReadDB() *sql.DB {
	return r.sharedDB
}

func (r *multitenantExtension) setupStatsTable(ctx context.Context) error {
	// ## On local DB
	// 0. stats table already created above during `newMultitenantExtension -> newDefaultExtension`
	// 1. Create publication, 1 publication for all tables (ignore already exists error)
	// 2. Add table to publication if not already (ignore is already member of publication error)

	// ## On remote DB
	// 1. Create subscription, 1 subscription for all tables (ignore already exists error)
	// 2. Create table if not exists
	// 2. Add table to subscription if not already (ignore is already member of subscription error)
	return nil
}

func (r *multitenantExtension) dropStats(ctx context.Context, jobRunId string) error {
	err := r.defaultExtension.dropStats(ctx, jobRunId)
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
