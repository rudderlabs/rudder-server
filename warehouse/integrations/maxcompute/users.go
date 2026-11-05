package maxcompute

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// currentUserFiles returns only objects belonging to this upload. Validate all
// locations and persisted object sizes before issuing any stage LOAD.
func (m *MaxCompute) currentUserFiles(ctx context.Context) ([]loadFolder, error) {
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
	return m.preparedLoadFiles(ctx, files)
}

func (m *MaxCompute) loadUsersWithCarryForward(ctx context.Context) (err error) {
	users := m.uploader.GetTableSchemaInWarehouse(warehouseutils.UsersTable)
	if len(users) == 0 || users["id"] != "string" || users["received_at"] != "datetime" || users["uuid_ts"] != "datetime" || users["loaded_at"] != "datetime" {
		return errors.New("MaxCompute users carry-forward requires users.id, users.received_at, users.uuid_ts and users.loaded_at")
	}
	// The existing users table is always auto-partitioned. Its immutable
	// expression is independent of this unpartitioned upload stage.
	if _, err := m.CatalogPartition(ctx, warehouseutils.UsersTable); err != nil {
		return err
	}
	folders, err := m.currentUserFiles(ctx)
	if err != nil || len(folders) == 0 {
		return err
	}
	usersTable, err := m.qualified(warehouseutils.UsersTable)
	if err != nil {
		return err
	}
	stage := warehouseutils.StagingTableName(provider, "users_upload", 128)
	stageTable, err := m.qualified(stage)
	if err != nil {
		return err
	}
	columns := warehouseutils.SortColumnKeysFromColumnMap(users)
	definitions := make([]string, 0, len(columns))
	traits := make([]string, 0, len(columns)-1)
	for _, key := range columns {
		col, err := quote(key)
		if err != nil {
			return err
		}
		kind, err := columnType(users[key])
		if err != nil {
			return fmt.Errorf("MaxCompute users column %q: %w", key, err)
		}
		definitions = append(definitions, col+" "+kind)
		if key != "id" {
			traits = append(traits, col)
		}
	}
	createStage := "CREATE TABLE " + stageTable + " (" + strings.Join(definitions, ", ") + ") TBLPROPERTIES ('table.format.version'='2')"
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
	for _, folder := range folders {
		if _, err := m.loadFolder(ctx, stageTable, folder); err != nil {
			return fmt.Errorf("loading MaxCompute users upload staging table: %w", err)
		}
	}
	// Reconcile the view with the current catalog before inserting users.
	if !m.warehouse.GetBoolDestinationConfig(model.SkipViewsSetting) {
		schema, err := m.FetchSchema(ctx)
		if err != nil {
			return err
		}
		if len(schema[warehouseutils.UsersTable]) == 0 {
			return errors.New("MaxCompute users view schema unavailable in catalog")
		}
		if err := m.createTableView(ctx, warehouseutils.UsersTable, schema[warehouseutils.UsersTable]); err != nil {
			return err
		}
	}

	projection := []string{"`id`"}
	for _, key := range columns {
		if key == "id" {
			continue
		}
		col, _ := quote(key)
		value := col
		if key == "uuid_ts" || key == "loaded_at" {
			value = "CASE WHEN `__current_upload` = 1 THEN " + col + " END"
		}
		projection = append(projection, "FIRST_VALUE("+value+", true) OVER (PARTITION BY `id` ORDER BY `received_at` DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "+col)
	}
	selectColumns := "`id`, " + strings.Join(traits, ", ")
	idSet := "SELECT `id` FROM " + stageTable + " WHERE `id` IS NOT NULL"
	ptFilter := boundedPTWindow(time.Now())
	branches := "SELECT " + selectColumns + ", 1 AS `__current_upload` FROM " + stageTable + " WHERE `id` IS NOT NULL" +
		" UNION ALL SELECT " + selectColumns + ", 0 AS `__current_upload` FROM " + usersTable + " WHERE " + ptFilter + " AND `id` IN (" + idSet + ")"
	statement := "INSERT INTO " + usersTable + " (" + selectColumns + ") SELECT DISTINCT " + strings.Join(projection, ", ") + " FROM (" + branches + ") u"
	if err := m.run(ctx, statement); err != nil {
		return fmt.Errorf("inserting MaxCompute users carry-forward: %w", err)
	}
	return nil
}
