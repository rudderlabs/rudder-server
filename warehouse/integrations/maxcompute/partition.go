package maxcompute

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var truncTimeExpression = regexp.MustCompile("(?i)^trunc_time\\(`?([a-z_][a-z0-9_]*)`?, '(day|hour)'\\)$")

var allowedPartitionColumns = map[string]bool{
	"_partitiontime": true, "loaded_at": true, "received_at": true,
	"timestamp": true, "sent_at": true, "original_timestamp": true,
}

var allowedPartitionGranularities = map[string]bool{"day": true, "hour": true}

// PlanPartition applies the transformer's lower-casing, keyword escape, and
// 127-character limit. A source absent at CREATE falls back to ingestion/day.
func PlanPartition(columns model.TableSchema, configuredColumn, configuredType string) (model.PartitionPlan, error) {
	granularity := strings.ToLower(configuredType)
	if granularity == "" {
		granularity = "day"
	}
	if !allowedPartitionGranularities[granularity] {
		return model.PartitionPlan{}, fmt.Errorf("unsupported MaxCompute partition type %q", configuredType)
	}
	if configuredColumn == "" || strings.EqualFold(configuredColumn, "_PARTITIONTIME") {
		return model.PartitionPlan{Source: "_partitiontime", Granularity: granularity}, nil
	}
	source := strings.ToLower(configuredColumn)
	if !allowedPartitionColumns[source] {
		return model.PartitionPlan{}, fmt.Errorf("unsupported MaxCompute partition column %q", configuredColumn)
	}
	if warehouseutils.ReservedKeywords[provider][strings.ToUpper(source)] {
		source = "_" + source
	}
	source = misc.TruncateStr(source, 127)
	if _, ok := columns[source]; !ok {
		return model.PartitionPlan{Source: "_partitiontime", Granularity: "day"}, nil
	}
	plan := model.PartitionPlan{Source: source, Granularity: granularity}
	return plan, validatePartitionPlan(plan, columns)
}

func validatePartitionPlan(plan model.PartitionPlan, columns model.TableSchema) error {
	if _, err := quote(plan.Source); err != nil {
		return err
	}
	if !allowedPartitionGranularities[plan.Granularity] {
		return fmt.Errorf("unsupported MaxCompute partition type %q", plan.Granularity)
	}
	if plan.Source != "_partitiontime" && columns[plan.Source] != "datetime" {
		return fmt.Errorf("MaxCompute auto partition requires datetime column %q", plan.Source)
	}
	if plan.Source == "_partitiontime" && columns[plan.Source] != "" && columns[plan.Source] != "datetime" {
		return fmt.Errorf("MaxCompute auto partition requires datetime column %q", plan.Source)
	}
	return nil
}

func (m *MaxCompute) SetPlannedPartitions(plans map[string]model.PartitionPlan) {
	m.plannedPartitions = plans
}

// CatalogPartition fails closed if ODPS does not describe the fixed expression.
func (m *MaxCompute) CatalogPartition(ctx context.Context, name string) (model.PartitionPlan, error) {
	if m.odps == nil {
		return model.PartitionPlan{}, fmt.Errorf("MaxCompute client is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return model.PartitionPlan{}, err
	}
	if _, err := quote(name); err != nil {
		return model.PartitionPlan{}, err
	}
	table := odps.NewTables(m.odps, m.odps.DefaultProjectName(), m.namespace()).Get(name)
	if err := table.Load(); err != nil {
		return model.PartitionPlan{}, fmt.Errorf("loading MaxCompute partition metadata: %w", client.ODPSSafeError(err))
	}
	for _, column := range table.Schema().PartitionColumns {
		if !strings.EqualFold(column.Name, "pt") {
			continue
		}
		if _, ok := column.GenerateExpression.(*tableschema.TruncTime); !ok {
			break
		}
		parts := truncTimeExpression.FindStringSubmatch(column.GenerateExpression.String())
		if len(parts) == 3 {
			return model.PartitionPlan{Source: strings.ToLower(parts[1]), Granularity: strings.ToLower(parts[2])}, nil
		}
		break
	}
	return model.PartitionPlan{}, fmt.Errorf("MaxCompute table %q has no supported pt partition expression", name)
}
