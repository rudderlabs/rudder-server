package service

import (
	"context"
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"golang.org/x/exp/slices"
)

// TODO: Remove warehouseutils.POSTGRES once the postgres new implementation is stable
var crashRecoverWarehouses = []string{
	warehouseutils.RS,
	warehouseutils.MSSQL,
	warehouseutils.AZURE_SYNAPSE,
	warehouseutils.DELTALAKE,
	warehouseutils.POSTGRES,
}

type repo interface {
	InterruptedDestinations(ctx context.Context, destinationType string) ([]string, error)
}

type destination interface {
	CrashRecover(warehouse model.Warehouse) (err error)
}

type onceErr struct {
	sync.Once
	err error
}

type Recovery struct {
	detectOnce      sync.Once
	detectErr       error
	destinationType string
	repo            repo
	inRecovery      map[string]*onceErr
}

func NewRecovery(destinationType string, repo repo) *Recovery {
	return &Recovery{
		destinationType: destinationType,
		repo:            repo,
		inRecovery:      make(map[string]*onceErr),
	}
}

// Detect detects if there are any warehouses that need to be recovered.
func (r *Recovery) detect(ctx context.Context) error {
	if !slices.Contains(crashRecoverWarehouses, r.destinationType) {
		return nil
	}

	destIDs, err := r.repo.InterruptedDestinations(ctx, r.destinationType)
	if err != nil {
		return fmt.Errorf("repo interrupted destinations: %w", err)
	}

	for _, destID := range destIDs {
		r.inRecovery[destID] = &onceErr{}
	}

	return nil
}

// Recover recovers a warehouse, for a non-graceful shutdown.
func (r *Recovery) Recover(ctx context.Context, whManager destination, wh model.Warehouse) error {
	r.detectOnce.Do(func() {
		r.detectErr = r.detect(ctx)
	})
	if r.detectErr != nil {
		return r.detectErr
	}

	once, ok := r.inRecovery[wh.Destination.ID]
	if !ok {
		return nil
	}

	once.Do(func() {
		once.err = whManager.CrashRecover(wh)
	})

	return once.err
}
