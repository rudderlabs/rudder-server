package router

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/schema"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type maxComputeSchemaHandle struct {
	schema.Handler
	tables model.Schema
}

func (h maxComputeSchemaHandle) GetTableSchema(_ context.Context, table string) model.TableSchema {
	return h.tables[table]
}

type maxComputePartitionManager struct {
	manager.Manager
	catalog map[string]model.PartitionPlan
	plans   map[string]model.PartitionPlan
	err     error
}

func (m *maxComputePartitionManager) CatalogPartition(_ context.Context, table string) (model.PartitionPlan, error) {
	if m.err != nil {
		return model.PartitionPlan{}, m.err
	}
	return m.catalog[table], nil
}

func (m *maxComputePartitionManager) SetPlannedPartitions(plans map[string]model.PartitionPlan) {
	m.plans = plans
}

func TestUploadJobMaxComputeFixedCatalogAndPlannedPartitions(t *testing.T) {
	mgr := &maxComputePartitionManager{catalog: map[string]model.PartitionPlan{
		"existing": {Source: "received_at", Granularity: "day"},
	}}
	job := UploadJob{
		ctx: context.Background(),
		warehouse: model.Warehouse{
			Type:   whutils.MAXCOMPUTE,
			Source: backendconfig.SourceT{ID: "source"},
			Destination: backendconfig.DestinationT{ID: "destination", Config: map[string]any{
				"partitionColumn": "_PARTITIONTIME", "partitionType": "hour",
			}},
		},
		upload: model.Upload{UploadSchema: model.Schema{
			"existing": {"id": "string", "received_at": "datetime"},
			"new":      {"id": "string", "_partitiontime": "datetime"},
		}},
		schemaHandle: maxComputeSchemaHandle{tables: model.Schema{
			"existing": {"id": "string", "received_at": "datetime"},
		}},
		whManager: mgr,
		conf:      config.New(),
	}
	dto, err := job.DTO()
	require.NoError(t, err)
	require.Equal(t, "dynamic", dto.PartitionModes["existing"])
	require.Equal(t, "day", dto.PartitionGranularities["existing"])
	require.Equal(t, "fixed", dto.PartitionModes["new"])
	require.Equal(t, "hour", dto.PartitionGranularities["new"])
	require.Equal(t, model.PartitionPlan{Source: "_partitiontime", Granularity: "hour"}, mgr.plans["new"])
	require.Equal(t, mgr.plans, job.upload.PartitionPlans)

	job.conf.Set("Warehouse.pipeline.source.destination.partitionColumn", "received_at")
	job.conf.Set("Warehouse.pipeline.source.destination.partitionType", "day")
	dto, err = job.DTO()
	require.NoError(t, err)
	require.Equal(t, "dynamic", dto.PartitionModes["existing"])
	require.Equal(t, "day", dto.PartitionGranularities["existing"])
	require.Equal(t, model.PartitionPlan{Source: "_partitiontime", Granularity: "day"}, mgr.plans["new"])

	mgr.err = errors.New("partition metadata unavailable")
	_, err = job.DTO()
	require.ErrorContains(t, err, "partition metadata unavailable")
}

func TestUploadJobMaxComputeNewTableFallbackAndPipelineOverride(t *testing.T) {
	mgr := &maxComputePartitionManager{}
	job := UploadJob{
		ctx: context.Background(),
		warehouse: model.Warehouse{
			Type:   whutils.MAXCOMPUTE,
			Source: backendconfig.SourceT{ID: "source"},
			Destination: backendconfig.DestinationT{ID: "destination", Config: map[string]any{
				"partitionColumn": "received_at", "partitionType": "hour",
			}},
		},
		upload:    model.Upload{UploadSchema: model.Schema{"events": {"id": "string"}}},
		conf:      config.New(),
		whManager: mgr,
	}
	dto, err := job.DTO()
	require.NoError(t, err)
	require.Equal(t, "fixed", dto.PartitionModes["events"])
	require.Equal(t, "day", dto.PartitionGranularities["events"])

	job.upload.UploadSchema["events"]["received_at"] = "datetime"
	dto, err = job.DTO()
	require.NoError(t, err)
	require.Equal(t, "dynamic", dto.PartitionModes["events"])
	require.Equal(t, "hour", dto.PartitionGranularities["events"])

	job.conf.Set("Warehouse.pipeline.source.destination.partitionColumn", "_PARTITIONTIME")
	job.conf.Set("Warehouse.pipeline.source.destination.partitionType", "hour")
	dto, err = job.DTO()
	require.NoError(t, err)
	require.Equal(t, "fixed", dto.PartitionModes["events"])
	require.Equal(t, "hour", dto.PartitionGranularities["events"])

	job.warehouse.Type = whutils.POSTGRES
	dto, err = job.DTO()
	require.NoError(t, err)
	require.Empty(t, dto.PartitionModes)
}
