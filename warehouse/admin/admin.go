package admin

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

type QueryInput struct {
	DestID       string
	SourceID     string
	SQLStatement string
}

type ConfigurationTestInput struct {
	DestID string
}

type ConfigurationTestOutput struct {
	Valid bool
	Error string
}

type Admin struct {
	connectionSources  connectionSourcesFetcher
	createUploadAlways createUploadAlwaysSetter
	logger             logger.Logger
}

type connectionSourcesFetcher interface {
	ConnectionSourcesMap(destID string) (map[string]model.Warehouse, bool)
}

type createUploadAlwaysSetter interface {
	Store(bool)
}

func New(
	connectionSources connectionSourcesFetcher,
	createUploadAlways createUploadAlwaysSetter,
	logger logger.Logger,
) *Admin {
	return &Admin{
		connectionSources:  connectionSources,
		createUploadAlways: createUploadAlways,
		logger:             logger.Child("admin"),
	}
}

// TriggerUpload sets uploads to start without delay
func (a *Admin) TriggerUpload(off bool, reply *string) error {
	a.createUploadAlways.Store(!off)
	if off {
		*reply = "Turned off explicit warehouse upload triggers.\nWarehouse uploads will continue to be done as per schedule in control plane."
	} else {
		*reply = "Successfully set uploads to start always without delay.\nRun same command with -o flag to turn off explicit triggers."
	}
	return nil
}

// Query the underlying warehouse
func (a *Admin) Query(s QueryInput, reply *warehouseutils.QueryResult) error {
	if strings.TrimSpace(s.DestID) == "" {
		return errors.New("please specify the destination ID to query the warehouse")
	}

	srcMap, ok := a.connectionSources.ConnectionSourcesMap(s.DestID)
	if !ok {
		return errors.New("please specify a valid and existing destination ID")
	}

	var warehouse model.Warehouse
	// use the sourceID-destID connection if sourceID is not empty
	if s.SourceID != "" {
		w, ok := srcMap[s.SourceID]
		if !ok {
			return errors.New("please specify a valid (sourceID, destination ID) pair")
		}
		warehouse = w
	} else {
		// use any source connected to the given destination otherwise
		for _, v := range srcMap {
			warehouse = v
			break
		}
	}

	whManager, err := manager.New(warehouse.Type, config.Default, logger.NOP, stats.Default)
	if err != nil {
		return err
	}
	whManager.SetConnectionTimeout(warehouseutils.GetConnectionTimeout(
		warehouse.Type, warehouse.Destination.ID,
	))
	client, err := whManager.Connect(context.TODO(), warehouse)
	if err != nil {
		return err
	}
	defer client.Close()

	a.logger.Infof(`[WH Admin]: Querying warehouse: %s:%s`, warehouse.Type, warehouse.Destination.ID)
	*reply, err = client.Query(s.SQLStatement)
	return err
}

// ConfigurationTest test the underlying warehouse destination
func (a *Admin) ConfigurationTest(s ConfigurationTestInput, reply *ConfigurationTestOutput) error {
	if strings.TrimSpace(s.DestID) == "" {
		return errors.New("please specify the destination ID to query the warehouse")
	}

	var warehouse model.Warehouse
	srcMap, ok := a.connectionSources.ConnectionSourcesMap(s.DestID)
	if !ok {
		return fmt.Errorf("please specify a valid and existing destinationID: %s", s.DestID)
	}

	for _, v := range srcMap {
		warehouse = v
		break
	}

	a.logger.Infof(`[WH Admin]: Validating warehouse destination: %s:%s`, warehouse.Type, warehouse.Destination.ID)

	destinationValidator := validations.NewDestinationValidator()
	res := destinationValidator.Validate(context.TODO(), &warehouse.Destination)

	reply.Valid = res.Success
	reply.Error = res.Error
	return nil
}
