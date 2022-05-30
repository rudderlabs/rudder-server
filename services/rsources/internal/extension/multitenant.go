package extension

import (
	"context"
	"database/sql"
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

func (*multitenantExtension) setupStatsTable(_ context.Context) error {
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

func (r *multitenantExtension) DropStats(ctx context.Context, jobRunId string) error {
	err := r.Extension.DropStats(ctx, jobRunId)
	if err != nil {
		return err
	}
	// ## On local DB
	// 1. Remove table from publication

	// ## On remote DB
	// 1. Remove table from subscription

	return nil
}

func (*multitenantExtension) CleanupLoop(_ context.Context) error {
	// TODO
	return nil
}
