package router

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
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

func TestUploadJobMaxComputePartitionModes(t *testing.T) {
	job := UploadJob{
		ctx: context.Background(),
		warehouse: model.Warehouse{
			Type:   whutils.MAXCOMPUTE,
			Source: backendconfig.SourceT{ID: "source"},
			Destination: backendconfig.DestinationT{ID: "destination", Config: map[string]any{
				"partitionModes": map[string]any{"fixed": "fixed", "dynamic": "dynamic"},
				"partitionTypes": map[string]any{"fixed": "hour", "dynamic": "day"},
			}},
		},
		upload: model.Upload{UploadSchema: model.Schema{
			"fixed":   {"id": "string", "_partitiontime": "datetime"},
			"dynamic": {"id": "string"},
			"new":     {"id": "string"},
		}},
		schemaHandle: maxComputeSchemaHandle{tables: model.Schema{
			"fixed":   {"id": "string", "_partitiontime": "datetime"},
			"dynamic": {"id": "string"},
		}},
		conf: config.New(),
	}
	job.conf.Set("Warehouse.pipeline.source.destination.partitionMode", "dynamic")
	job.conf.Set("Warehouse.pipeline.source.destination.partitionType", "hour")
	dto := job.DTO()
	require.Equal(t, "dynamic", dto.PartitionModes["new"])
	require.Equal(t, "hour", dto.PartitionGranularities["new"])

	job.conf.Set("Warehouse.pipeline.source.destination.partitionMode.fixed", "dynamic")
	job.conf.Set("Warehouse.pipeline.source.destination.partitionType.fixed", "day")
	job.conf.Set("Warehouse.pipeline.source.destination.partitionMode.dynamic", "fixed")
	job.conf.Set("Warehouse.pipeline.source.destination.partitionType.dynamic", "hour")

	dto = job.DTO()
	require.Equal(t, "fixed", dto.PartitionModes["fixed"])
	require.Equal(t, "dynamic", dto.PartitionModes["dynamic"])
	require.Equal(t, "hour", dto.PartitionGranularities["fixed"])
	require.Equal(t, "day", dto.PartitionGranularities["dynamic"])

	job.conf.Set("Warehouse.pipeline.source.destination.partitionMode.new", "fixed")
	job.conf.Set("Warehouse.pipeline.source.destination.partitionType.new", "day")
	require.Equal(t, "fixed", job.DTO().PartitionModes["new"])
	require.Equal(t, "day", job.DTO().PartitionGranularities["new"])

	job.warehouse.Type = whutils.POSTGRES
	require.Empty(t, job.DTO().PartitionModes)
	require.Empty(t, job.DTO().PartitionGranularities)
}

func TestUploadJobMaxComputeNewTablePartitionDefaults(t *testing.T) {
	job := UploadJob{
		ctx: context.Background(),
		warehouse: model.Warehouse{
			Type:   whutils.MAXCOMPUTE,
			Source: backendconfig.SourceT{ID: "source"},
			Destination: backendconfig.DestinationT{ID: "destination", Config: map[string]any{
				"partitionMode": "fixed", "partitionType": "day",
			}},
		},
		upload: model.Upload{UploadSchema: model.Schema{"events": {"id": "string"}}},
		conf:   config.New(),
	}
	require.Equal(t, "fixed", job.DTO().PartitionModes["events"])
	require.Equal(t, "day", job.DTO().PartitionGranularities["events"])
	job.conf.Set("Warehouse.pipeline.source.destination.partitionType", "hour")
	require.Equal(t, "hour", job.DTO().PartitionGranularities["events"])
}
