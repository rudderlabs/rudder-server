package service

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// TODO: Remove warehouseutils.POSTGRES once the postgres new implementation is stable
var crashRecoverWarehouses = []string{
	warehouseutils.RS,
	warehouseutils.MSSQL,
	warehouseutils.AzureSynapse,
	warehouseutils.DELTALAKE,
	warehouseutils.POSTGRES,
}

type repo interface {
	InterruptedDestinations(ctx context.Context, destinationType string) ([]string, error)
}

type destination interface {
	CrashRecover(ctx context.Context) error
}

type Recovery struct {
	detectOnce      sync.Once
	detectErr       error
	destinationType string
	repo            repo
	inRecovery      map[string]*sync.Once
	recoveryErr     map[string]error
}

func NewRecovery(destinationType string, repo repo) *Recovery {
	return &Recovery{
		destinationType: destinationType,
		repo:            repo,
		inRecovery:      make(map[string]*sync.Once),
		recoveryErr:     make(map[string]error),
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
		r.inRecovery[destID] = &sync.Once{}
	}

	return nil
}

// Recover recovers a warehouse, for a non-graceful shutdown.
func (r *Recovery) Recover(ctx context.Context, whManager destination, wh model.Warehouse) error {
	r.detectOnce.Do(func() {
		r.detectErr = r.detect(ctx)
	})
	if r.detectErr != nil {
		return fmt.Errorf("detection: %w", r.detectErr)
	}

	once, ok := r.inRecovery[wh.Destination.ID]
	if !ok {
		return nil
	}
	once.Do(func() {
		err := whManager.CrashRecover(ctx)
		if err != nil {
			r.recoveryErr[wh.Destination.ID] = fmt.Errorf("crash recover: %w", err)
		}
	})

	return r.recoveryErr[wh.Destination.ID]
}
