package maxcompute

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// A bounded pt predicate is mandatory for auto-partitioned tables. In
// particular, never deduplicate across an unbounded historical scan.
func (m *MaxCompute) viewSQL(table string, columns model.TableSchema) (string, error) {
	base, err := m.qualified(table)
	if err != nil {
		return "", err
	}
	view, err := m.qualified(viewName(table))
	if err != nil {
		return "", err
	}
	partitionKeys := []string{"id"}
	if table == warehouseutils.DiscardsTable {
		partitionKeys = []string{"row_id", "column_name", "table_name"}
	}
	for _, key := range partitionKeys {
		if _, ok := columns[key]; !ok {
			return "", fmt.Errorf("MaxCompute view for %q requires %q", table, key)
		}
	}
	quoted := make([]string, 0, len(partitionKeys))
	for _, key := range partitionKeys {
		name, _ := quote(key)
		quoted = append(quoted, name)
	}
	order := ""
	if _, ok := columns["loaded_at"]; ok {
		order = " ORDER BY `loaded_at` DESC"
	}
	filter := boundedViewPartitions
	// Explicitly select the real columns so the window helper does not leak.
	projection := make([]string, 0, len(columns))
	for _, key := range warehouseutils.SortColumnKeysFromColumnMap(columns) {
		name, err := quote(key)
		if err != nil {
			return "", err
		}
		projection = append(projection, name)
	}
	return fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT %s FROM (SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s%s) AS `__rudder_row_number` FROM %s%s) t WHERE `__rudder_row_number` = 1",
		view, strings.Join(projection, ", "), strings.Join(projection, ", "), strings.Join(quoted, ", "), order, base, filter), nil
}

// Partition pruning must be evaluated at query time, not when the view is
// created. Both bounds apply to all auto-partitioned tables.
const boundedViewPartitions = " WHERE `pt` >= TO_CHAR(DATEADD(GETDATE(), -60, 'dd'), 'yyyy-mm-dd')" +
	" AND `pt` < TO_CHAR(DATEADD(GETDATE(), 2, 'dd'), 'yyyy-mm-dd')"

// viewName bounds identifiers to ODPS's 128-character limit while preserving
// uniqueness even when two long table names share the same prefix.
func viewName(table string) string {
	const maxPrefix = 128 - len("_view_") - 16
	hash := sha256.Sum256([]byte(table))
	if len(table) > maxPrefix {
		table = table[:maxPrefix]
	}
	return table + "_view_" + hex.EncodeToString(hash[:8])
}

func (m *MaxCompute) createTableView(ctx context.Context, table string, columns model.TableSchema) error {
	if m.warehouse.GetBoolDestinationConfig(model.SkipViewsSetting) {
		return nil
	}
	// Tables without a deduplication key still get a bounded projection view.
	_, hasID := columns["id"]
	if !hasID && table != warehouseutils.DiscardsTable {
		base, err := m.qualified(table)
		if err != nil {
			return err
		}
		view, err := m.qualified(viewName(table))
		if err != nil {
			return err
		}
		projection := make([]string, 0, len(columns))
		for _, key := range warehouseutils.SortColumnKeysFromColumnMap(columns) {
			name, err := quote(key)
			if err != nil {
				return err
			}
			projection = append(projection, name)
		}
		if len(projection) == 0 {
			return fmt.Errorf("MaxCompute view has no columns")
		}
		return m.run(ctx, "CREATE OR REPLACE VIEW "+view+" AS SELECT "+strings.Join(projection, ", ")+" FROM "+base+boundedViewPartitions)
	}
	statement, err := m.viewSQL(table, columns)
	if err != nil {
		return err
	}
	return m.run(ctx, statement)
}
