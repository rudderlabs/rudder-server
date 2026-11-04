package maxcompute

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"

	"github.com/rudderlabs/rudder-go-kit/filemanager"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// A 60-day window matches the bounded, query-time predicate used by the views.
// Derive *exact* partition values before staging: a range on the partition key
// would also scan gaps, and a filter only on timestamps would not prune pt.
func (m *MaxCompute) userPartitionValues(ctx context.Context, table string) ([]string, error) {
	if m.partitions != nil {
		return m.partitions(ctx, table)
	}
	if m.odps == nil {
		return nil, errors.New("MaxCompute client is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	values, err := odps.NewTable(m.odps, m.odps.DefaultProjectName(), m.namespace(), table).GetPartitionValues()
	if err != nil {
		return nil, errors.New("listing MaxCompute user partitions failed")
	}
	return values, ctx.Err()
}

func (m *MaxCompute) boundedUserPartitions(ctx context.Context, table string, now time.Time) ([]string, error) {
	plan, err := m.CatalogPartition(ctx, table)
	if err != nil {
		return nil, err
	}
	granularity := plan.Granularity
	values, err := m.userPartitionValues(ctx, table)
	if err != nil {
		return nil, err
	}
	format, step := "2006-01-02", 24*time.Hour
	if granularity == "hour" {
		format, step = "2006-01-02 15:00:00", time.Hour
	}
	current := now.UTC().Truncate(step)
	oldest := now.UTC().AddDate(0, 0, -60).Truncate(step)
	selected := make([]string, 0, len(values))
	for _, value := range values {
		// Fail closed on unexpected partition metadata; never substitute a
		// range or an unpartitioned scan for an unparseable listing.
		if !strings.HasPrefix(value, "pt=") {
			return nil, errors.New("unexpected MaxCompute user partition")
		}
		pt := strings.TrimPrefix(value, "pt=")
		parsed, err := time.ParseInLocation(format, pt, time.UTC)
		if err != nil || parsed.Format(format) != pt {
			return nil, errors.New("invalid MaxCompute user partition")
		}
		if !parsed.Before(oldest) && !parsed.After(current) {
			selected = append(selected, pt)
		}
	}
	sort.Strings(selected)
	return selected, nil
}

// userStagePartitionSource uses the existing users table's immutable auto-partition
// expression. A destination setting alone cannot describe an already-created table.
// The SDK exposes that expression on the generated pt column. Fail closed if it
// cannot be verified, rather than creating a stage with a different partition key.
func (m *MaxCompute) userStagePartitionSource(ctx context.Context, users model.TableSchema) (string, string, error) {
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	if m.odps == nil {
		return "", "", errors.New("MaxCompute client is not initialized")
	}
	const table = warehouseutils.UsersTable
	catalogTable := odps.NewTable(m.odps, m.odps.DefaultProjectName(), m.namespace(), table)
	if err := catalogTable.Load(); err != nil {
		return "", "", errors.New("loading MaxCompute users partition metadata failed")
	}
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	parts := catalogTable.Schema().PartitionColumns
	if len(parts) != 1 || parts[0].Name != "pt" || parts[0].GenerateExpression == nil {
		return "", "", errors.New("MaxCompute users auto partition source unavailable")
	}
	expr, ok := parts[0].GenerateExpression.(*tableschema.TruncTime)
	if !ok {
		return "", "", errors.New("unsupported MaxCompute users auto partition source")
	}
	source, granularity, err := userStageSourceFromExpression(expr.String(), users)
	if err != nil {
		return "", "", err
	}
	// The uploader snapshot alone cannot prove that the existing source is datetime.
	for _, col := range catalogTable.Schema().Columns {
		if col.Name == source {
			if logical, ok := sdkType(col.Type); !ok || logical != "datetime" {
				return "", "", errors.New("MaxCompute users partition source is not datetime in catalog")
			}
			return source, granularity, nil
		}
	}
	return "", "", errors.New("MaxCompute users partition source unavailable in catalog")
}

// Match the catalog expression only against validated datetime columns. The
// catalog, not a mutable destination setting, determines the stage partitioning.
func userStageSourceFromExpression(expression string, users model.TableSchema) (string, string, error) {
	for column, logical := range users {
		if logical != "datetime" || column == "pt" {
			continue
		}
		quoted, err := quote(column)
		if err != nil {
			return "", "", err
		}
		for _, granularity := range []string{"day", "hour"} {
			if strings.EqualFold(expression, "trunc_time("+quoted+", '"+granularity+"')") {
				return column, granularity, nil
			}
		}
	}
	return "", "", errors.New("MaxCompute users auto partition requires an immutable datetime column source")
}

func userStagePartitionValues(parts []string, granularity string) ([]string, error) {
	format := "2006-01-02"
	if granularity == "hour" {
		format = "2006-01-02 15:00:00"
	} else if granularity != "day" {
		return nil, errors.New("unsupported MaxCompute users upload partition type")
	}
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if !strings.HasPrefix(part, "pt=") {
			return nil, errors.New("unexpected MaxCompute users upload partition")
		}
		value := strings.TrimPrefix(part, "pt=")
		parsed, err := time.ParseInLocation(format, value, time.UTC)
		if err != nil || parsed.Format(format) != value {
			return nil, errors.New("invalid MaxCompute users upload partition")
		}
		values = append(values, value)
	}
	sort.Strings(values)
	return values, nil
}

// No staging reads are allowed without an explicit finite set of pt values.
func userPartitionPredicate(values []string) (string, error) {
	if len(values) == 0 {
		return "", errors.New("no MaxCompute user partitions to stage")
	}
	literals := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" || strings.ContainsAny(value, "'\n\r\\") {
			return "", errors.New("invalid MaxCompute user partition")
		}
		literals = append(literals, "'"+value+"'")
	}
	return "`pt` IN (" + strings.Join(literals, ", ") + ")", nil
}

// currentUserFiles returns only objects belonging to this upload. Validate all
// locations and authenticated object sizes before issuing any stage LOAD.
func (m *MaxCompute) currentUserFiles(ctx context.Context) ([]string, error) {
	if m.uploader == nil {
		return nil, errors.New("MaxCompute uploader is not initialized")
	}
	if m.uploader.GetLoadFileType() != warehouseutils.LoadFileTypeJson {
		return nil, errors.New("MaxCompute requires JSON load files")
	}
	files, err := m.uploader.GetLoadFilesMetadata(ctx, warehouseutils.GetLoadFilesOptions{Table: warehouseutils.UsersTable})
	if err != nil {
		return nil, errors.New("getting MaxCompute users load files failed")
	}
	if len(files) == 0 {
		return nil, nil
	}
	if err := ValidateSettings(m.warehouse.Destination.Config, m.warehouse.Destination.WorkspaceID); err != nil {
		return nil, err
	}
	converted := misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
		Provider: warehouseutils.AlibabaOSS, Config: m.warehouse.Destination.Config,
		WorkspaceID: m.warehouse.Destination.WorkspaceID,
	})
	fm, err := filemanager.New(&filemanager.Settings{Provider: warehouseutils.AlibabaOSS, Config: converted, Conf: m.conf})
	if err != nil {
		return nil, errors.New("creating MaxCompute OSS filemanager failed")
	}
	bucket, _ := converted["bucketName"].(string)
	if !ossBucketPattern.MatchString(bucket) {
		return nil, errors.New("invalid MaxCompute OSS source")
	}
	paths := make([]string, 0, len(files))
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := validateLocation(file.Location, m.warehouse.Destination.Config); err != nil {
			return nil, err
		}
		key, err := fm.GetObjectNameFromLocation(file.Location)
		if err != nil || key == "" || strings.ContainsAny(key, "'\n\r\\") || strings.Contains(key, "..") {
			return nil, errors.New("invalid MaxCompute OSS source")
		}
		parsed, _ := url.Parse(file.Location) // validated above
		if strings.TrimPrefix(parsed.Path, "/") != key {
			return nil, errors.New("invalid MaxCompute OSS source")
		}
		length, err := metadataSize(ctx, m.conf, converted, key)
		if err != nil {
			return nil, err
		}
		if length <= 0 || length > maxOSSLoadBytes {
			return nil, errors.New("MaxCompute OSS object length missing or exceeds 3GB")
		}
		paths = append(paths, "oss://"+bucket+"/"+key)
	}
	return paths, nil
}

func (m *MaxCompute) loadUsersWithCarryForward(ctx context.Context) (err error) {
	users := m.uploader.GetTableSchemaInWarehouse(warehouseutils.UsersTable)
	identifies := m.uploader.GetTableSchemaInWarehouse(warehouseutils.IdentifiesTable)
	if len(users) == 0 || users["id"] != "string" || users["received_at"] != "datetime" || identifies["user_id"] != "string" || identifies["received_at"] != "datetime" {
		return errors.New("MaxCompute users carry-forward requires warehouse users.id, users.received_at and identifies.user_id, identifies.received_at")
	}
	if _, err := m.CatalogPartition(ctx, warehouseutils.UsersTable); err != nil {
		return err
	}
	if _, err := m.CatalogPartition(ctx, warehouseutils.IdentifiesTable); err != nil {
		return err
	}
	for _, key := range warehouseutils.SortColumnKeysFromColumnMap(users) {
		if _, err := quote(key); err != nil {
			return err
		}
	}
	for _, key := range warehouseutils.SortColumnKeysFromColumnMap(identifies) {
		if _, err := quote(key); err != nil {
			return err
		}
	}
	// Current-upload files, not bounded historical identifies, define the set
	// of users eligible for carry-forward. An empty upload has nothing to do.
	paths, err := m.currentUserFiles(ctx)
	if err != nil || len(paths) == 0 {
		return err
	}
	usersTable, err := m.qualified(warehouseutils.UsersTable)
	if err != nil {
		return err
	}
	partitionSource, granularity, err := m.userStagePartitionSource(ctx, users)
	if err != nil {
		return err
	}
	stage := warehouseutils.StagingTableName(provider, "users_upload", 128)
	stageTable, err := m.qualified(stage)
	if err != nil {
		return err
	}
	definitions := make([]string, 0, len(users))
	columns := warehouseutils.SortColumnKeysFromColumnMap(users)
	for _, key := range columns {
		col, _ := quote(key)
		kind, err := columnType(users[key])
		if err != nil {
			return fmt.Errorf("MaxCompute users column %q: %w", key, err)
		}
		definitions = append(definitions, col+" "+kind)
	}
	partitionField, _ := quote(partitionSource) // validated against catalog and schema above
	createStage := "CREATE TABLE " + stageTable + " (" + strings.Join(definitions, ", ") + ") AUTO PARTITIONED BY (TRUNC_TIME(" + partitionField + ", '" + granularity + "') AS `pt`) TBLPROPERTIES ('table.format.version'='2')"
	if err := m.run(ctx, createStage); err != nil {
		return fmt.Errorf("creating MaxCompute users upload staging table: %w", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if dropErr := m.run(cleanupCtx, "DROP TABLE IF EXISTS "+stageTable); dropErr != nil {
			err = errors.Join(err, fmt.Errorf("dropping MaxCompute users staging table: %w", dropErr))
		}
	}()
	// This stage has a fresh, unique name and receives only this upload's files.
	// Capture its exact pt values after LOAD; neither a time range nor the
	// warehouse users table's historical partitions identify these rows.
	for _, path := range paths {
		if err := m.run(ctx, "LOAD DATA INPATH '"+path+"' INTO TABLE "+stageTable); err != nil {
			return fmt.Errorf("loading MaxCompute users upload staging table: %w", err)
		}
	}
	stageParts, err := m.userPartitionValues(ctx, stage)
	if err != nil {
		return err
	}
	if len(stageParts) == 0 {
		return nil // no rows loaded; do not insert
	}
	stageValues, err := userStagePartitionValues(stageParts, granularity)
	if err != nil {
		return err
	}
	stageFilter, err := userPartitionPredicate(stageValues)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	userParts, err := m.boundedUserPartitions(ctx, warehouseutils.UsersTable, now)
	if err != nil {
		return err
	}
	identifyParts, err := m.boundedUserPartitions(ctx, warehouseutils.IdentifiesTable, now)
	if err != nil {
		return err
	}
	traitColumns := make([]string, 0, len(columns)-1)
	projection := []string{"`id`"}
	for _, key := range columns {
		if key == "id" {
			continue
		}
		col, _ := quote(key)
		traitColumns = append(traitColumns, col)
		// Timestamp values must come from this upload, not history or wall
		// clock time. All other traits use the latest non-null value.
		if key == "loaded_at" || key == "uuid_ts" || key == "received_at" {
			projection = append(projection, "FIRST_VALUE(CASE WHEN `__current_upload` = 1 THEN "+col+" END, true) OVER (PARTITION BY `id` ORDER BY `__current_upload` DESC, `received_at` DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "+col)
		} else {
			projection = append(projection, "FIRST_VALUE("+col+", true) OVER (PARTITION BY `id` ORDER BY `received_at` DESC, `__current_upload` DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "+col)
		}
	}
	branches := []string{"SELECT `id`, " + strings.Join(traitColumns, ", ") + ", 1 AS `__current_upload` FROM " + stageTable + " WHERE " + stageFilter + " AND `id` IS NOT NULL"}
	idSet := "SELECT DISTINCT `id` FROM " + stageTable + " WHERE " + stageFilter + " AND `id` IS NOT NULL"
	if len(userParts) != 0 {
		userFilter, err := userPartitionPredicate(userParts)
		if err != nil {
			return err
		}
		branches = append(branches, "SELECT `id`, "+strings.Join(traitColumns, ", ")+", 0 AS `__current_upload` FROM "+usersTable+" WHERE "+userFilter+" AND `id` IN ("+idSet+")")
	}
	if len(identifyParts) != 0 {
		identifyFilter, err := userPartitionPredicate(identifyParts)
		if err != nil {
			return err
		}
		identifiesTable, err := m.qualified(warehouseutils.IdentifiesTable)
		if err != nil {
			return err
		}
		identifyColumns := make([]string, 0, len(traitColumns))
		for _, key := range columns {
			if key == "id" {
				continue
			}
			col, _ := quote(key)
			if key == "loaded_at" || key == "uuid_ts" {
				identifyColumns = append(identifyColumns, "NULL AS "+col)
			} else if identifies[key] != "" && identifies[key] == users[key] {
				identifyColumns = append(identifyColumns, col)
			} else {
				identifyColumns = append(identifyColumns, "NULL AS "+col)
			}
		}
		branches = append(branches, "SELECT `user_id` AS `id`, "+strings.Join(identifyColumns, ", ")+", 0 AS `__current_upload` FROM "+identifiesTable+" WHERE "+identifyFilter+" AND `user_id` IN ("+idSet+")")
	}
	insertColumns := append([]string{"`id`"}, traitColumns...)
	statement := "INSERT INTO " + usersTable + " (" + strings.Join(insertColumns, ", ") + ") SELECT DISTINCT " + strings.Join(projection, ", ") + " FROM (" + strings.Join(branches, " UNION ALL ") + ") u"
	if err := m.run(ctx, statement); err != nil {
		return fmt.Errorf("inserting MaxCompute users carry-forward: %w", err)
	}
	return nil
}
