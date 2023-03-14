package repo_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/stretchr/testify/require"
)

func TestTableUploadRepo(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Second).UTC()
	db := setupDB(t)

	r := repo.NewTableUploads(db, repo.WithNow(func() time.Time {
		return now
	}))
	l := repo.NewLoadFiles(db, repo.WithNow(func() time.Time {
		return now
	}))

	var (
		uploadID    int64 = 1
		tables            = []string{"table_name", "table_name_2", "table_name_3", "table_name_4", "table_name_5", "table_name_6", "table_name_7", "table_name_8", "table_name_9", "table_name_10"}
		randomTable       = "random_table_name"
	)

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("Insert", func(t *testing.T) {
		t.Log("success")
		err := r.Insert(ctx, uploadID, tables)
		require.NoError(t, err)

		t.Log("duplicate upload id")
		err = r.Insert(ctx, uploadID, tables)
		require.NoError(t, err)

		t.Log("cancelled context")
		err = r.Insert(cancelledCtx, uploadID, tables)
		require.EqualError(t, err, fmt.Errorf("begin transaction: context canceled").Error())
	})

	t.Run("GetByUploadID", func(t *testing.T) {
		t.Log("valid upload id")
		tableUploads, err := r.GetByUploadID(ctx, uploadID)
		require.NoError(t, err)
		require.Len(t, tableUploads, len(tables))

		for i := range tableUploads {
			tableUploads[i].ID = int64(i + 1)
			tableUploads[i].UploadID = uploadID
			tableUploads[i].TableName = tables[i]
			tableUploads[i].CreatedAt = now
			tableUploads[i].UpdatedAt = now
		}

		t.Log("invalid upload id")
		tableUploads, err = r.GetByUploadID(ctx, int64(-1))
		require.NoError(t, err)
		require.Empty(t, tableUploads)

		t.Log("cancelled context")
		tableUploads, err = r.GetByUploadID(cancelledCtx, uploadID)
		require.EqualError(t, err, fmt.Errorf("querying table uploads: context canceled").Error())
		require.Empty(t, tableUploads)
	})

	t.Run("GetByUploadIDAndTableName", func(t *testing.T) {
		t.Log("valid upload id and table name")
		for _, table := range tables {
			tableUpload, err := r.GetByUploadIDAndTableName(ctx, uploadID, table)
			require.NoError(t, err)
			require.Equal(t, uploadID, tableUpload.UploadID)
			require.Equal(t, table, tableUpload.TableName)
		}

		t.Log("invalid upload id")
		tableUpload, err := r.GetByUploadIDAndTableName(ctx, int64(-1), "table_name")
		require.Error(t, err)
		require.Empty(t, tableUpload)

		t.Log("invalid table name")
		tableUpload, err = r.GetByUploadIDAndTableName(ctx, uploadID, randomTable)
		require.Error(t, err)
		require.Empty(t, tableUpload)

		t.Log("context cancelled")
		tableUpload, err = r.GetByUploadIDAndTableName(cancelledCtx, uploadID, "table_name")
		require.EqualError(t, err, fmt.Errorf("querying table uploads: context canceled").Error())
		require.Empty(t, tableUpload)
	})

	t.Run("ExistsForUploadID", func(t *testing.T) {
		t.Log("upload id exists")
		exists, err := r.ExistsForUploadID(ctx, uploadID)
		require.NoError(t, err)
		require.True(t, exists)

		t.Log("upload id does not exist")
		exists, err = r.ExistsForUploadID(ctx, int64(-1))
		require.NoError(t, err)
		require.False(t, exists)

		t.Log("context cancelled")
		exists, err = r.ExistsForUploadID(cancelledCtx, uploadID)
		require.EqualError(t, err, fmt.Errorf("checking if table upload exists: context canceled").Error())
		require.False(t, exists)
	})

	t.Run("Set", func(t *testing.T) {
		var (
			errorStatus  = errors.New("test error").Error()
			status       = model.TableUploadWaiting
			lastExecTime = now.UTC()
			location     = "test_location"
			table        = tables[0]
			totalEvents  = int64(100)
		)

		t.Run("no set options", func(t *testing.T) {
			err := r.Set(ctx, uploadID, table, repo.TableUploadSetOptions{})
			require.EqualError(t, err, "no set options provided")
		})

		t.Run("context cancelled", func(t *testing.T) {
			err := r.Set(cancelledCtx, uploadID, table, repo.TableUploadSetOptions{
				Status: &status,
			})
			require.EqualError(t, err, fmt.Errorf("set: context canceled").Error())
		})

		t.Run("set status", func(t *testing.T) {
			statuses := []string{
				model.TableUploadWaiting,
				model.TableUploadExecuting,
				model.TableUploadUpdatingSchema,
				model.TableUploadUpdatingSchemaFailed,
				model.TableUploadUpdatedSchema,
				model.TableUploadExporting,
				model.TableUploadExportingFailed,
				model.TableUploadExported,
			}

			for _, status := range statuses {
				err := r.Set(ctx, uploadID, table, repo.TableUploadSetOptions{
					Status: &status,
				})
				require.NoError(t, err)

				tableUpload, err := r.GetByUploadIDAndTableName(ctx, uploadID, table)
				require.NoError(t, err)
				require.Equal(t, status, tableUpload.Status)
				require.Equal(t, now, tableUpload.UpdatedAt)
			}
		})

		t.Run("no rows affected", func(t *testing.T) {
			err := r.Set(ctx, uploadID, randomTable, repo.TableUploadSetOptions{
				Status: &status,
			})
			require.EqualError(t, err, errors.New("no rows affected").Error())
		})

		t.Run("set error", func(t *testing.T) {
			err := r.Set(ctx, uploadID, table, repo.TableUploadSetOptions{
				Error: &errorStatus,
			})
			require.NoError(t, err)

			tableUpload, err := r.GetByUploadIDAndTableName(ctx, uploadID, table)
			require.NoError(t, err)
			require.Equal(t, errorStatus, tableUpload.Error)
			require.Equal(t, now, tableUpload.UpdatedAt)
		})

		t.Run("set last exec time", func(t *testing.T) {
			err := r.Set(ctx, uploadID, table, repo.TableUploadSetOptions{
				LastExecTime: &lastExecTime,
			})
			require.NoError(t, err)

			tableUpload, err := r.GetByUploadIDAndTableName(ctx, uploadID, table)
			require.NoError(t, err)
			require.Equal(t, lastExecTime, tableUpload.LastExecTime)
			require.Equal(t, now, tableUpload.UpdatedAt)
		})

		t.Run("set location", func(t *testing.T) {
			err := r.Set(ctx, uploadID, table, repo.TableUploadSetOptions{
				Location: &location,
			})
			require.NoError(t, err)

			tableUpload, err := r.GetByUploadIDAndTableName(ctx, uploadID, table)
			require.NoError(t, err)
			require.Equal(t, location, tableUpload.Location)
			require.Equal(t, now, tableUpload.UpdatedAt)
		})

		t.Run("set total events", func(t *testing.T) {
			err := r.Set(ctx, uploadID, table, repo.TableUploadSetOptions{
				TotalEvents: &totalEvents,
			})
			require.NoError(t, err)

			tableUpload, err := r.GetByUploadIDAndTableName(ctx, uploadID, table)
			require.NoError(t, err)
			require.Equal(t, totalEvents, tableUpload.TotalEvents)
			require.Equal(t, now, tableUpload.UpdatedAt)
		})

		t.Run("set all", func(t *testing.T) {
			err := r.Set(ctx, uploadID, table, repo.TableUploadSetOptions{
				Status:       &status,
				Error:        &errorStatus,
				LastExecTime: &lastExecTime,
				Location:     &location,
				TotalEvents:  &totalEvents,
			})
			require.NoError(t, err)

			tableUpload, err := r.GetByUploadIDAndTableName(ctx, uploadID, table)
			require.NoError(t, err)
			require.Equal(t, uploadID, tableUpload.UploadID)
			require.Equal(t, status, tableUpload.Status)
			require.Equal(t, errorStatus, tableUpload.Error)
			require.Equal(t, lastExecTime, tableUpload.LastExecTime)
			require.Equal(t, location, tableUpload.Location)
			require.Equal(t, now, tableUpload.UpdatedAt)
		})
	})

	t.Run("TotalEvents", func(t *testing.T) {
		t.Run("PopulateTotalEventsFromStagingFileIDs", func(t *testing.T) {
			var (
				loadFiles  []model.LoadFile
				stagingIDs []int64
				tablename  = tables[0]
			)

			t.Log("insert load files")
			for i, table := range tables {
				loadFile := model.LoadFile{
					TableName:     table,
					TotalRows:     i + 1,
					StagingFileID: int64(i + 1),
				}

				stagingIDs = append(stagingIDs, loadFile.StagingFileID)
				loadFiles = append(loadFiles, loadFile)
			}
			err := l.Insert(ctx, loadFiles)
			require.NoError(t, err)

			t.Log("populate total events")
			for i, table := range tables {
				err = r.PopulateTotalEventsFromStagingFileIDs(ctx, uploadID, table, stagingIDs)
				require.NoError(t, err)

				tableUpload, err := r.GetByUploadIDAndTableName(ctx, uploadID, table)
				require.NoError(t, err)
				require.Equal(t, uploadID, tableUpload.UploadID)
				require.Equal(t, table, tableUpload.TableName)
				require.Equal(t, int64(i+1), tableUpload.TotalEvents)
			}

			t.Run("cancelled context", func(t *testing.T) {
				err = r.PopulateTotalEventsFromStagingFileIDs(cancelledCtx, uploadID, tablename, stagingIDs)
				require.EqualError(t, err, fmt.Errorf("set total events: context canceled").Error())
			})

			t.Run("no rows affected", func(t *testing.T) {
				err = r.PopulateTotalEventsFromStagingFileIDs(ctx, int64(-1), tablename, stagingIDs)
				require.EqualError(t, err, fmt.Errorf("no rows affected").Error())
			})
		})

		t.Run("TotalExportedEvents", func(t *testing.T) {
			t.Run("all exported tables", func(t *testing.T) {
				status := model.TableUploadExported

				for _, table := range tables {
					err := r.Set(ctx, uploadID, table, repo.TableUploadSetOptions{
						Status: &status,
					})
					require.NoError(t, err)
				}

				totalEvents, err := r.TotalExportedEvents(ctx, uploadID, []string{})
				require.NoError(t, err)

				expectedTotalEvents := int64(0)
				for i := range tables {
					expectedTotalEvents += int64(i + 1)
				}

				require.Equal(t, expectedTotalEvents, totalEvents)
			})

			t.Run("skip tables", func(t *testing.T) {
				totalEvents, err := r.TotalExportedEvents(ctx, uploadID, tables)
				require.NoError(t, err)
				require.Equal(t, int64(0), totalEvents)
			})

			t.Run("cancelled context", func(t *testing.T) {
				totalEvents, err := r.TotalExportedEvents(cancelledCtx, uploadID, []string{})
				require.EqualError(t, err, fmt.Errorf("counting total exported events: context canceled").Error())
				require.Equal(t, int64(0), totalEvents)
			})

			t.Run("skip specific tables", func(t *testing.T) {
				var (
					skipTables          []string
					expectedTotalEvents = int64(0)
				)

				for i := range tables {
					if i%2 == 0 {
						skipTables = append(skipTables, tables[i])
						continue
					}
					expectedTotalEvents += int64(i + 1)
				}

				totalEvents, err := r.TotalExportedEvents(ctx, uploadID, skipTables)
				require.NoError(t, err)

				require.Equal(t, expectedTotalEvents, totalEvents)
			})
		})
	})
}
