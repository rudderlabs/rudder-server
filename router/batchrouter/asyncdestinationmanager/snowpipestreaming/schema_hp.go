package snowpipestreaming

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/snowflake"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// errTableNotFound is returned by a [warehouseSession] when the table (or its schema) doesn't exist.
var errTableNotFound = errors.New("table not found")

// warehouseSession runs the SQL the high-performance path needs: reading a table's columns, and creating schemas,
// tables and columns. None of it needs a running warehouse, so the customer's warehouse is never resumed.
type warehouseSession interface {
	// Columns returns the columns of a table, or [errTableNotFound].
	Columns(ctx context.Context, table string) (whutils.ModelTableSchema, error)
	CreateSchema(ctx context.Context) error
	CreateTable(ctx context.Context, table string, schema whutils.ModelTableSchema) error
	AddColumns(ctx context.Context, table string, columns []whutils.ColumnInfo) error
	Close(ctx context.Context)
}

// sqlWarehouseSession is a [warehouseSession] backed by the Snowflake warehouse integration.
// It connects on first use, so uploads whose schemas are all cached never connect.
type sqlWarehouseSession struct {
	database  string
	namespace string
	connect   func(ctx context.Context) (*snowflake.Snowflake, error)
	mgr       *snowflake.Snowflake
}

func (s *sqlWarehouseSession) manager(ctx context.Context) (*snowflake.Snowflake, error) {
	if s.mgr != nil {
		return s.mgr, nil
	}
	mgr, err := s.connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("connecting to snowflake: %w", err)
	}
	s.mgr = mgr
	return mgr, nil
}

// Columns reads the table's columns with SHOW COLUMNS, which, unlike INFORMATION_SCHEMA, doesn't need a warehouse.
// It reads one table at a time, so Snowflake's cap of 10,000 rows per SHOW never applies in practice.
func (s *sqlWarehouseSession) Columns(ctx context.Context, table string) (whutils.ModelTableSchema, error) {
	sf, err := s.manager(ctx)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf(`SHOW COLUMNS IN TABLE %s.%s.%s`, quoteIdentifier(s.database), quoteIdentifier(s.namespace), quoteIdentifier(table))
	rows, err := sf.DB.QueryContext(ctx, query)
	if err != nil {
		if isObjectNotFound(err) {
			return nil, errTableNotFound
		}
		return nil, fmt.Errorf("showing columns of %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	return scanShowColumns(rows)
}

func (s *sqlWarehouseSession) CreateSchema(ctx context.Context) error {
	mgr, err := s.manager(ctx)
	if err != nil {
		return err
	}
	return mgr.CreateSchema(ctx)
}

func (s *sqlWarehouseSession) CreateTable(ctx context.Context, table string, schema whutils.ModelTableSchema) error {
	mgr, err := s.manager(ctx)
	if err != nil {
		return err
	}
	return mgr.CreateTable(ctx, table, schema)
}

func (s *sqlWarehouseSession) AddColumns(ctx context.Context, table string, columns []whutils.ColumnInfo) error {
	mgr, err := s.manager(ctx)
	if err != nil {
		return err
	}
	return mgr.AddColumns(ctx, table, columns)
}

func (s *sqlWarehouseSession) Close(ctx context.Context) {
	if s.mgr != nil {
		s.mgr.Cleanup(ctx)
		s.mgr = nil
	}
}

// sqlRows is the part of [sql.Rows] that [scanShowColumns] reads.
type sqlRows interface {
	Columns() ([]string, error)
	Next() bool
	Scan(dest ...any) error
	Err() error
}

// unsupportedDataTypePrefix marks columns whose Snowflake type has no data type of ours.
const unsupportedDataTypePrefix = "unsupported:"

// scanShowColumns reads the column_name and data_type of every row of a SHOW COLUMNS result.
// The result has many columns whose order isn't guaranteed, so they are matched by name.
func scanShowColumns(rows sqlRows) (whutils.ModelTableSchema, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("reading show columns result: %w", err)
	}
	nameIdx, typeIdx := -1, -1
	for i, c := range cols {
		switch strings.ToLower(c) {
		case "column_name":
			nameIdx = i
		case "data_type":
			typeIdx = i
		}
	}
	if nameIdx < 0 || typeIdx < 0 {
		return nil, fmt.Errorf("reading show columns result: missing column_name or data_type in %v", cols)
	}

	schema := make(whutils.ModelTableSchema)
	values := make([]sql.NullString, len(cols))
	dest := make([]any, len(cols))
	for i := range values {
		dest[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("scanning show columns result: %w", err)
		}
		dataType, ok := normaliseShowColumnsType(values[typeIdx].String)
		if !ok {
			// A type we can't map (e.g. GEOGRAPHY) is still a column: forgetting it would make every batch try to add it
			// again. Values sent to it fail type conversion and go to the discards table.
			dataType = unsupportedDataTypePrefix + values[typeIdx].String
		}
		schema[values[nameIdx].String] = dataType
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading show columns result: %w", err)
	}
	return schema, nil
}

// normaliseShowColumnsType converts the data_type of SHOW COLUMNS, a JSON object such as
// {"type":"FIXED","precision":38,"scale":0,"nullable":true}, to our data type.
// SHOW COLUMNS reports type families (FIXED for every fixed-point number, OBJECT and ARRAY for semi-structured
// columns) that the shared Snowflake type map doesn't know, so they are mapped here rather than changing that map.
func normaliseShowColumnsType(dataType string) (string, bool) {
	var parsed struct {
		Type  string `json:"type"`
		Scale int64  `json:"scale"`
	}
	if err := jsonrs.Unmarshal([]byte(dataType), &parsed); err != nil {
		return "", false
	}
	columnType := strings.ToUpper(parsed.Type)
	switch columnType {
	case "FIXED":
		columnType = "NUMBER"
	case "OBJECT", "ARRAY":
		columnType = "VARIANT"
	}
	return snowflake.CalculateDataType(columnType, parsed.Scale)
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

// isObjectNotFound reports whether a Snowflake error says that an object doesn't exist (error 002003).
func isObjectNotFound(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "002003") || strings.Contains(msg, "does not exist or not authorized")
}

// hpSchemas caches table schemas per namespace and table, and keeps them complete for the columns of each batch.
type hpSchemas struct {
	cache map[string]cachedSchema // by namespace + "." + table
}

func schemaKey(namespace, table string) string { return namespace + "." + table }

// ensureSchema returns a schema of the table holding every column of eventSchema.
//
// The high-performance architecture silently drops fields with no matching column, so a stale schema means silent
// data loss. The schema is therefore read again whenever the batch has a column the cache doesn't know, and at least
// every schemaCacheTTL to notice columns dropped or changed outside RudderStack. Missing tables and columns are
// created, and if a column is still missing after that, the batch is not sent.
func (m *ManagerV2) ensureSchema(ctx context.Context, session warehouseSession, namespace, table string, eventSchema whutils.ModelTableSchema) (whutils.ModelTableSchema, error) {
	key := schemaKey(namespace, table)
	log := m.logger.Withn(logger.NewStringField("table", table), logger.NewStringField("namespace", namespace))

	cached, isCached := m.schemas.cache[key]
	expired := isCached && m.now().Sub(cached.fetchedAt) > m.config.schemaCacheTTL.Load()
	if isCached && !expired && len(findNewColumns(eventSchema, cached.schema)) == 0 {
		return cached.schema, nil
	}

	trigger := "cold"
	switch {
	case expired:
		trigger = "ttl"
	case isCached:
		trigger = "unknown_column"
	}
	m.stats.schemaCacheRefresh(trigger).Increment()
	delete(m.schemas.cache, key)

	schema, err := session.Columns(ctx, table)
	switch {
	case errors.Is(err, errTableNotFound):
		log.Infon("Table not found, creating it")
		if err := m.createTable(ctx, session, table, eventSchema); err != nil {
			return nil, err
		}
		if schema, err = session.Columns(ctx, table); err != nil {
			return nil, fmt.Errorf("reading columns after creating table %s: %w", table, err)
		}
	case err != nil:
		return nil, fmt.Errorf("reading columns of %s: %w", table, err)
	}

	if newColumns := findNewColumns(eventSchema, schema); len(newColumns) > 0 {
		log.Infon("Adding columns", logger.NewIntField("count", int64(len(newColumns))))
		addErr := session.AddColumns(ctx, table, newColumns)
		if schema, err = session.Columns(ctx, table); err != nil {
			return nil, fmt.Errorf("reading columns after adding them to %s: %w", table, err)
		}
		// Another pod may have added the same columns at the same time: what matters is that they exist now.
		if addErr != nil && len(findNewColumns(eventSchema, schema)) > 0 {
			return nil, fmt.Errorf("adding columns to %s: %w, %w", table, errAbort, addErr)
		}
	}

	if missing := findNewColumns(eventSchema, schema); len(missing) > 0 {
		m.stats.schemaCacheMissOnSend.Count(len(missing))
		log.Errorn("Columns still missing after adding them, not sending the batch",
			logger.NewIntField("missing", int64(len(missing))),
			logger.NewStringField("column", missing[0].Name),
		)
		return nil, fmt.Errorf("columns of %s still missing after adding them: %d, e.g. %s", table, len(missing), missing[0].Name)
	}
	m.schemas.cache[key] = cachedSchema{schema: schema, fetchedAt: m.now()}
	return schema, nil
}

// createTable creates the table, and its schema if that is missing too.
func (m *ManagerV2) createTable(ctx context.Context, session warehouseSession, table string, eventSchema whutils.ModelTableSchema) error {
	if err := session.CreateTable(ctx, table, eventSchema); err != nil {
		m.logger.Infon("Creating table failed, creating schema first",
			logger.NewStringField("table", table),
			obskit.Error(err),
		)
		if err := session.CreateSchema(ctx); err != nil {
			return fmt.Errorf("creating schema: %w, %w", errAbort, err)
		}
		if err := session.CreateTable(ctx, table, eventSchema); err != nil {
			return fmt.Errorf("creating table %s: %w, %w", table, errAbort, err)
		}
	}
	return nil
}
