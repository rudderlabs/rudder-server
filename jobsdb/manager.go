package jobsdb

import (
	"context"
	"golang.org/x/sync/errgroup"
	"time"
)

type DBs struct {
	gwDBRetention     time.Duration
	routerDBRetention time.Duration
	migrationMode     string
	ClearDB           bool
	GatewayDB         HandleT
	gwDbWG            *errgroup.Group
	RouterDB          HandleT
	rtDbWG            *errgroup.Group
	BatchRouterDB     HandleT
	brtDbWG           *errgroup.Group
	ProcErrDB         HandleT
	prDbWG            *errgroup.Group
	TenantRouterDB    MultiTenantHandleT
}

func (d *DBs) InitiateDBs(gwDBRetention, routerDBRetention time.Duration, migrationMode string, clearDB bool) {
	//dbs := factory()
	d.gwDBRetention = gwDBRetention
	d.routerDBRetention = routerDBRetention
	d.migrationMode = migrationMode
	d.ClearDB = clearDB

	d.GatewayDB.Setup(ReadWrite, d.ClearDB, "gw", d.gwDBRetention, d.migrationMode, true,
		QueryFiltersT{})
	d.RouterDB.Setup(ReadWrite, d.ClearDB, "rt", d.routerDBRetention, d.migrationMode, true,
		QueryFiltersT{})
	d.BatchRouterDB.Setup(ReadWrite, d.ClearDB, "batch_rt", d.routerDBRetention, d.migrationMode,
		true, QueryFiltersT{})
	d.ProcErrDB.Setup(ReadWrite, d.ClearDB, "proc_error", d.routerDBRetention, d.migrationMode, false,
		QueryFiltersT{})
}

func Factory() *DBs {
	db := DBs{
		GatewayDB:      HandleT{},
		RouterDB:       HandleT{},
		BatchRouterDB:  HandleT{},
		ProcErrDB:      HandleT{},
		TenantRouterDB: MultiTenantHandleT{},
	}
	db.TenantRouterDB.HandleT = &db.RouterDB
	return &db
}

func (d *DBs) Halt() {
	d.GatewayDB.HaltDB()
	d.RouterDB.HaltDB()
	d.BatchRouterDB.HaltDB()
	d.ProcErrDB.HaltDB()
	_ = d.haltWait()
}

func (d *DBs)haltWait() error {
	if err := d.gwDbWG.Wait(); err != nil {
		return err
	}
	if err := d.rtDbWG.Wait(); err != nil {
		return err
	}
	if err := d.brtDbWG.Wait(); err != nil {
		return err
	}
	if err := d.prDbWG.Wait(); err != nil {
		return err
	}
	return nil
}

func (d *DBs) Start() {
	var ctx context.Context
	d.gwDbWG, ctx = errgroup.WithContext(d.GatewayDB.mainCtx)
	d.gwDbWG.Go(func() error {
		d.GatewayDB.Start(ctx, ReadWrite)
		return nil
	})

	d.rtDbWG, ctx = errgroup.WithContext(d.RouterDB.mainCtx)
	d.rtDbWG.Go(func() error {
		d.RouterDB.Start(ctx, ReadWrite)
		return nil
	})

	d.brtDbWG, ctx = errgroup.WithContext(d.BatchRouterDB.mainCtx)
	d.brtDbWG.Go(func() error {
		d.BatchRouterDB.Start(ctx, ReadWrite)
		return nil
	})

	d.prDbWG, ctx = errgroup.WithContext(d.ProcErrDB.mainCtx)
	d.prDbWG.Go(func() error {
		d.ProcErrDB.Start(ctx, ReadWrite)
		return nil
	})
}
