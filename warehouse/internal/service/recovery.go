package service

import (
	"context"
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"golang.org/x/exp/slices"
)

var crashRecoverWarehouses = []string{
	warehouseutils.RS,
	warehouseutils.POSTGRES,
	warehouseutils.MSSQL,
	warehouseutils.AZURE_SYNAPSE,
	warehouseutils.DELTALAKE,
}

type repo interface {
	InterruptedDestinations(ctx context.Context, destinationType string) ([]string, error)
}

type Recovery struct {
	detectOnce      sync.Once
	detectErr       error
	destinationType string
	repo            repo
	inRecovery      map[string]sync.Once
}

func NewRecovery(destinationType string, repo repo) *Recovery {
	return &Recovery{
		destinationType: destinationType,
		repo:            repo,
		inRecovery:      make(map[string]sync.Once),
	}
}

// Detect detects if there are any warehouses that need to be recovered.
func (r *Recovery) detect(ctx context.Context) error {
	if !slices.Contains(crashRecoverWarehouses, r.destinationType) {
		return nil
	}

	destIDs, err := r.repo.InterruptedDestinations(context.Background(), r.destinationType)
	if err != nil {
		return fmt.Errorf("repo interrupted destinations: %w", err)
	}

	for _, destID := range destIDs {
		r.inRecovery[destID] = sync.Once{}
	}

	return nil
}

// Recover recovers a warehouse, for a non-graceful shutdown.
func (r *Recovery) Recover(ctx context.Context, whManager manager.ManagerI, wh warehouseutils.Warehouse) error {
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

	var err error
	once.Do(func() {
		err = whManager.CrashRecover(wh)
	})

	return err
}
