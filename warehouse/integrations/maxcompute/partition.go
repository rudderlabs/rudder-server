package maxcompute

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"

	"github.com/rudderlabs/rudder-go-kit/logger"

	odpsclient "github.com/rudderlabs/rudder-server/warehouse/integrations/maxcompute/client"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

type partitionPlan struct {
	Source      string
	Granularity string
}

var truncTimeExpression = regexp.MustCompile("(?i)^trunc_time\\(`?([a-z_][a-z0-9_]*)`?, '(day|hour)'\\)$")

var allowedPartitionColumns = map[string]bool{
	"uuid_ts": true, "loaded_at": true, "received_at": true,
	"timestamp": true, "sent_at": true, "original_timestamp": true,
}

var allowedPartitionGranularities = map[string]bool{"day": true, "hour": true}

// PlanPartition applies the transformer's lower-casing and keyword escape.
// A source absent at CREATE falls back to ingestion/day.
func PlanPartition(columns model.TableSchema, configuredColumn, configuredType string) (partitionPlan, error) {
	granularity := strings.ToLower(configuredType)
	if granularity == "" {
		granularity = "day"
	}
	if !allowedPartitionGranularities[granularity] {
		return partitionPlan{}, fmt.Errorf("unsupported MaxCompute partition type %q", configuredType)
	}
	if configuredColumn == "" || strings.EqualFold(configuredColumn, "_PARTITIONTIME") {
		return partitionPlan{Source: "uuid_ts", Granularity: granularity}, nil
	}
	source := strings.ToLower(configuredColumn)
	if !allowedPartitionColumns[source] {
		return partitionPlan{}, fmt.Errorf("unsupported MaxCompute partition column %q", configuredColumn)
	}
	candidates := []string{source}
	if source == "timestamp" {
		// timestamp is reserved: the transformer writes _timestamp unless
		// skipReservedKeywordsEscaping keeps the original name.
		candidates = []string{"_timestamp", "timestamp"}
	}
	source = ""
	for _, candidate := range candidates {
		if _, ok := columns[candidate]; ok {
			source = candidate
			break
		}
	}
	if source == "" {
		return partitionPlan{Source: "uuid_ts", Granularity: "day"}, nil
	}
	plan := partitionPlan{Source: source, Granularity: granularity}
	return plan, validatePartitionPlan(plan, columns)
}

func validatePartitionPlan(plan partitionPlan, columns model.TableSchema) error {
	if _, err := quote(plan.Source); err != nil {
		return err
	}
	if !allowedPartitionGranularities[plan.Granularity] {
		return fmt.Errorf("unsupported MaxCompute partition type %q", plan.Granularity)
	}
	if columns[plan.Source] != "datetime" {
		return fmt.Errorf("MaxCompute auto partition requires datetime column %q", plan.Source)
	}
	return nil
}

func (m *MaxCompute) configuredPartition(columns model.TableSchema) (partitionPlan, error) {
	configuredColumn := m.warehouse.GetStringDestinationConfig(m.conf, model.PartitionColumnSetting)
	configuredType := m.warehouse.GetStringDestinationConfig(m.conf, model.PartitionTypeSetting)
	return PlanPartition(columns, configuredColumn, configuredType)
}

// CatalogPartition fails closed if ODPS does not describe the fixed expression.
func (m *MaxCompute) CatalogPartition(ctx context.Context, name string) (partitionPlan, error) {
	if m.odps == nil {
		return partitionPlan{}, fmt.Errorf("MaxCompute client is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return partitionPlan{}, err
	}
	if _, err := quote(name); err != nil {
		return partitionPlan{}, err
	}
	table := odps.NewTables(m.odps, m.odps.DefaultProjectName(), m.namespace()).Get(name)
	if err := table.Load(); err != nil {
		return partitionPlan{}, fmt.Errorf("loading MaxCompute partition metadata: %w", odpsclient.SafeError(err))
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
			catalog := partitionPlan{Source: strings.ToLower(parts[1]), Granularity: strings.ToLower(parts[2])}
			if m.conf != nil && m.logger != nil {
				columns := make(model.TableSchema)
				for _, c := range table.Schema().Columns {
					if logical, ok := sdkType(c.Type); ok {
						columns[c.Name] = logical
					}
				}
				if configured, err := m.configuredPartition(columns); err == nil && configured != catalog {
					m.logger.Warnn("MaxCompute partition settings differ from table catalog; using catalog expression",
						logger.NewStringField("table", name))
				}
			}
			return catalog, nil
		}
		break
	}
	return partitionPlan{}, fmt.Errorf("MaxCompute table %q has no supported pt partition expression", name)
}
