package repo_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestStagingFilesRepo(t *testing.T) {
	testCases := []struct {
		name                 string
		enableJsonbToText    bool
		enableLz4Compression bool
	}{
		{
			name:                 "default",
			enableJsonbToText:    false,
			enableLz4Compression: false,
		},
		{
			name:              "enable jsonb to text",
			enableJsonbToText: true,
		},
		{
			name:                 "enable lz4 compression",
			enableLz4Compression: true,
		},
		{
			name:                 "enable jsonb to text and lz4 compression",
			enableJsonbToText:    true,
			enableLz4Compression: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defaultConfig := func() *config.Config {
				conf := config.New()
				conf.Set("Warehouse.enableJsonbToText", tc.enableJsonbToText)
				conf.Set("Warehouse.enableLz4Compression", tc.enableLz4Compression)
				return conf
			}

			t.Run("Few", func(t *testing.T) {
				conf := defaultConfig()
				db, ctx := setupDBWithConfig(t, conf), context.Background()
				now := time.Now().Truncate(time.Second).UTC()
				r := repo.NewStagingFiles(db, conf, repo.WithNow(func() time.Time {
					return now
				}))

				testcases := []struct {
					name        string
					stagingFile model.StagingFileWithSchema
				}{
					{
						name: "create staging file",
						stagingFile: model.StagingFile{
							WorkspaceID:           "workspace_id",
							Location:              "s3://bucket/path/to/file",
							SourceID:              "source_id",
							DestinationID:         "destination_id",
							Status:                warehouseutils.StagingFileWaitingState,
							Error:                 fmt.Errorf("dummy error"),
							FirstEventAt:          now.Add(time.Second),
							LastEventAt:           now,
							UseRudderStorage:      true,
							DestinationRevisionID: "destination_revision_id",
							TotalEvents:           100,
							BytesPerTable:         map[string]int64{"table1": 1000, "table2": 2000},
							SourceTaskRunID:       "source_task_run_id",
							SourceJobID:           "source_job_id",
							SourceJobRunID:        "source_job_run_id",
							TimeWindow:            time.Date(1993, 8, 1, 3, 0, 0, 0, time.UTC),
							ServerInstanceID:      "test-instance-id-v1",
						}.WithSchema([]byte(`{"type": "object"}`)),
					},
					{
						name: "create staging file with empty BytesPerTable",
						stagingFile: model.StagingFile{
							WorkspaceID:           "workspace_id",
							Location:              "s3://bucket/path/to/file",
							SourceID:              "source_id",
							DestinationID:         "destination_id",
							Status:                warehouseutils.StagingFileWaitingState,
							Error:                 fmt.Errorf("dummy error"),
							FirstEventAt:          now.Add(time.Second),
							LastEventAt:           now,
							UseRudderStorage:      true,
							DestinationRevisionID: "destination_revision_id",
							TotalEvents:           100,
							BytesPerTable:         map[string]int64{},
							SourceTaskRunID:       "source_task_run_id",
							SourceJobID:           "source_job_id",
							SourceJobRunID:        "source_job_run_id",
							TimeWindow:            time.Date(1993, 8, 1, 3, 0, 0, 0, time.UTC),
						}.WithSchema([]byte(`{"type": "object"}`)),
					},
					{
						name: "missing FirstEventAt",
						stagingFile: model.StagingFile{
							WorkspaceID:           "workspace_id",
							Location:              "s3://bucket/path/to/file",
							SourceID:              "source_id",
							DestinationID:         "destination_id",
							Status:                warehouseutils.StagingFileWaitingState,
							Error:                 fmt.Errorf("dummy error"),
							LastEventAt:           now,
							UseRudderStorage:      true,
							DestinationRevisionID: "destination_revision_id",
							TotalEvents:           100,
							SourceTaskRunID:       "source_task_run_id",
							SourceJobID:           "source_job_id",
							SourceJobRunID:        "source_job_run_id",
							TimeWindow:            time.Date(1993, 8, 1, 3, 0, 0, 0, time.UTC),
							ServerInstanceID:      "test-instance-id-v2",
						}.WithSchema([]byte(`{"type": "object"}`)),
					},
					{
						name: "missing LastEventAt",
						stagingFile: model.StagingFile{
							WorkspaceID:           "workspace_id",
							Location:              "s3://bucket/path/to/file",
							SourceID:              "source_id",
							DestinationID:         "destination_id",
							Status:                warehouseutils.StagingFileWaitingState,
							Error:                 fmt.Errorf("dummy error"),
							FirstEventAt:          now.Add(time.Second),
							UseRudderStorage:      true,
							DestinationRevisionID: "destination_revision_id",
							TotalEvents:           100,
							SourceTaskRunID:       "source_task_run_id",
							SourceJobID:           "source_job_id",
							SourceJobRunID:        "source_job_run_id",
							TimeWindow:            time.Date(1993, 8, 1, 3, 0, 0, 0, time.UTC),
						}.WithSchema([]byte(`{"type": "object"}`)),
					},
				}

				for _, tc := range testcases {
					t.Run("insert and get: "+tc.name, func(t *testing.T) {
						id, err := r.Insert(ctx, &tc.stagingFile)
						require.NoError(t, err)
						require.NotZero(t, id)

						retrieved, err := r.GetByID(ctx, id)
						require.NoError(t, err)

						expected := tc.stagingFile
						expected.ID = id
						expected.Error = nil
						expected.CreatedAt = now
						expected.UpdatedAt = now

						require.Equal(t, expected.StagingFile, retrieved)
					})
				}

				t.Run("schema snapshot and patch", func(t *testing.T) {
					snapshotRepo := repo.NewStagingFileSchemaSnapshots(db)

					originalSchema := []byte(`{"event1": {"key1": "string"}, "event2": {"key2": "string"}, "event3": {"key3": "string"}, "event4": {"key4": "string"}, "event5": {"key5": "string"}, "event6": {"key6": "string"}, "event7": {"key7": "string"}, "event8": {"key8": "string"}}`)
					modifiedSchema := []byte(`{"event1": {"key1": "string"}, "event2": {"key2": "string"}, "event3": {"key3": "string"}, "event4": {"key4": "string"}, "event5": {"key5": "string"}, "event6": {"key6": "string"}, "event7": {"key7": "string"}, "event8": {"key8": "string"}, "event9": {"key9": "string"}}`)

					snapshotID, err := snapshotRepo.Insert(ctx, "source_id", "destination_id", "workspace_id", originalSchema)
					require.NoError(t, err)

					patchSchema, err := warehouseutils.GenerateJSONPatch(originalSchema, modifiedSchema)
					require.NoError(t, err)

					stagingFile := model.StagingFile{
						WorkspaceID:           "workspace_id",
						Location:              "s3://bucket/path/to/file",
						SourceID:              "source_id",
						DestinationID:         "destination_id",
						Status:                warehouseutils.StagingFileWaitingState,
						Error:                 fmt.Errorf("dummy error"),
						FirstEventAt:          now.Add(time.Second),
						LastEventAt:           now,
						UseRudderStorage:      true,
						DestinationRevisionID: "destination_revision_id",
						TotalEvents:           100,
						SourceTaskRunID:       "source_task_run_id",
						SourceJobID:           "source_job_id",
						SourceJobRunID:        "source_job_run_id",
						TimeWindow:            time.Date(1993, 8, 1, 3, 0, 0, 0, time.UTC),
						ServerInstanceID:      "test-instance-id-v4",
					}
					stagingFileWithSchema := stagingFile.
						WithSchema(modifiedSchema).
						WithSnapshotIDAndPatch(snapshotID, patchSchema)
					id, err := r.Insert(ctx, &stagingFileWithSchema)
					require.NoError(t, err)
					require.NotZero(t, id)

					retrieved, err := r.GetByID(ctx, id)
					require.NoError(t, err)

					expected := stagingFile
					expected.ID = id
					expected.Error = nil
					expected.CreatedAt = now
					expected.UpdatedAt = now
					require.Equal(t, expected, retrieved)

					var schema json.RawMessage
					var schemaSnapshotID string
					var schemaSnapshotPatch []byte
					var snapshotPatchSize int
					var snapshotPatchCompressionRatio float64

					err = db.QueryRowContext(ctx, `
						SELECT schema::jsonb, schema_snapshot_id, schema_snapshot_patch, metadata->>'snapshot_patch_size', metadata->>'snapshot_patch_compression_ratio'
						FROM wh_staging_files
						WHERE id = $1
						`, id,
					).Scan(&schema, &schemaSnapshotID, &schemaSnapshotPatch, &snapshotPatchSize, &snapshotPatchCompressionRatio)
					require.NoError(t, err)
					require.Equal(t, snapshotID.String(), schemaSnapshotID)
					require.JSONEq(t, "{}", string(schema))
					require.JSONEq(t, string(patchSchema), string(schemaSnapshotPatch))
					require.Equal(t, len(stagingFileWithSchema.SnapshotPatch), snapshotPatchSize)
					require.Equal(t, float64(len(stagingFileWithSchema.SnapshotPatch))/float64(len(stagingFileWithSchema.Schema)), snapshotPatchCompressionRatio)
				})

				t.Run("get missing id", func(t *testing.T) {
					_, err := r.GetByID(ctx, -1)
					require.EqualError(t, err, "no staging file found with id: -1")
				})
			})

			t.Run("Many", func(t *testing.T) {
				conf := defaultConfig()
				db, ctx := setupDBWithConfig(t, conf), context.Background()
				now := time.Now().Truncate(time.Second).UTC()
				r := repo.NewStagingFiles(db, conf, repo.WithNow(func() time.Time {
					return now
				}))

				snapshotsRepo := repo.NewStagingFileSchemaSnapshots(db)
				snapshotID, err := snapshotsRepo.Insert(ctx, "", "", "", []byte(`{"table": {"column": "type"} }`))
				require.Nil(t, err)

				stagingFiles := manyStagingFiles(10, now)
				for i := range stagingFiles {
					file := stagingFiles[i].WithSchema([]byte(`{"table": {"column": "type"} }`)).WithSnapshotIDAndPatch(snapshotID, []byte("[]"))
					id, err := r.Insert(ctx, &file)
					require.NoError(t, err)
					stagingFiles[i].ID = id
					stagingFiles[i].Error = nil
					stagingFiles[i].CreatedAt = now
					stagingFiles[i].UpdatedAt = now
				}

				t.Run("GetForUploadID", func(t *testing.T) {
					u := repo.NewUploads(db)
					uploadId, err := u.CreateWithStagingFiles(ctx, model.Upload{}, stagingFiles)
					require.NoError(t, err)

					retrieved, err := r.GetForUploadID(ctx, uploadId)
					require.NoError(t, err)
					require.Equal(t, stagingFiles, retrieved)
				})

				t.Run("GetSchemasByIDs", func(t *testing.T) {
					t.Run("get all", func(t *testing.T) {
						stagingIDs := repo.StagingFileIDs(stagingFiles)
						expectedSchemas, err := r.GetSchemasByIDs(ctx, stagingIDs)
						require.NoError(t, err)
						require.Len(t, expectedSchemas, len(stagingFiles))

						for _, es := range expectedSchemas {
							require.EqualValues(t, model.Schema{
								"table": model.TableSchema{
									"column": "type",
								},
							}, es)
						}
					})

					t.Run("missing id", func(t *testing.T) {
						expectedSchemas, err := r.GetSchemasByIDs(ctx, []int64{1, 2, 3, 101, 102, 103})
						require.EqualError(t, err, "cannot get schemas by ids: not all schemas were found")
						require.Nil(t, expectedSchemas)
					})

					t.Run("context canceled", func(t *testing.T) {
						ctx, cancel := context.WithCancel(context.Background())
						cancel()

						stagingIDs := repo.StagingFileIDs(stagingFiles)
						expectedSchemas, err := r.GetSchemasByIDs(ctx, stagingIDs)
						require.ErrorIs(t, err, context.Canceled)
						require.Nil(t, expectedSchemas)
					})

					t.Run("invalid JSON", func(t *testing.T) {
						db := setupDBWithConfig(t, conf)

						_, err := db.ExecContext(ctx, `
				INSERT INTO wh_staging_files (
				  location, source_id, destination_id,
				  schema, created_at, updated_at, workspace_id
				)
				VALUES
				  (
					's3://bucket/path/to/file', 'source_id',
					'destination_id', '1', NOW(), NOW(),
					'workspace_id'
				  );
			`)
						require.NoError(t, err)

						r := repo.NewStagingFiles(db, conf)

						expectedSchemas, err := r.GetSchemasByIDs(ctx, []int64{1})
						require.Error(t, err)
						require.Nil(t, expectedSchemas)
					})

					t.Run("with schema snapshot and patch", func(t *testing.T) {
						testCases := []struct {
							name           string
							originalSchema model.Schema
							updatedSchema  model.Schema
							schemaPatch    []byte
						}{
							{
								name: "no changes (original == updated)",
								originalSchema: model.Schema{
									"table": model.TableSchema{"column": "type"},
								},
								updatedSchema: model.Schema{
									"table": model.TableSchema{"column": "type"},
								},
								schemaPatch: []byte(`[]`),
							},
							{
								name: "updated has extra table",
								originalSchema: model.Schema{
									"table": model.TableSchema{"column": "type"},
								},
								updatedSchema: model.Schema{
									"table":  model.TableSchema{"column": "type"},
									"table2": model.TableSchema{"column2": "type2"},
								},
								schemaPatch: []byte(`[{"op": "add", "path": "/table2", "value": {"column2": "type2"}}]`),
							},
							{
								name: "updated has extra column",
								originalSchema: model.Schema{
									"table": model.TableSchema{"column": "type"},
								},
								updatedSchema: model.Schema{
									"table": model.TableSchema{"column": "type", "column2": "type2"},
								},
								schemaPatch: []byte(`[{"op": "add", "path": "/table/column2", "value": "type2"}]`),
							},
							{
								name: "updated has less table",
								originalSchema: model.Schema{
									"table":  model.TableSchema{"column": "type"},
									"table2": model.TableSchema{"column2": "type2"},
								},
								updatedSchema: model.Schema{
									"table": model.TableSchema{"column": "type"},
								},
								schemaPatch: []byte(`[{"op": "remove", "path": "/table2"}]`),
							},
							{
								name: "updated has less column",
								originalSchema: model.Schema{
									"table": model.TableSchema{"column": "type", "column2": "type2"},
								},
								updatedSchema: model.Schema{
									"table": model.TableSchema{"column": "type"},
								},
								schemaPatch: []byte(`[{"op": "remove", "path": "/table/column2"}]`),
							},
							{
								name: "updated has changed column type",
								originalSchema: model.Schema{
									"table": model.TableSchema{"column": "type"},
								},
								updatedSchema: model.Schema{
									"table": model.TableSchema{"column": "type2"},
								},
								schemaPatch: []byte(`[{"op": "replace", "path": "/table/column", "value": "type2"}]`),
							},
						}

						for _, tc := range testCases {
							t.Run(tc.name, func(t *testing.T) {
								originalSchemaBytes, err := jsonrs.Marshal(tc.originalSchema)
								require.NoError(t, err)
								updatedSchemaBytes, err := jsonrs.Marshal(tc.updatedSchema)
								require.NoError(t, err)

								snapshotRepo := repo.NewStagingFileSchemaSnapshots(db)
								snapshotID, err := snapshotRepo.Insert(ctx, "source_id", "destination_id", "workspace_id", originalSchemaBytes)
								require.NoError(t, err)
								require.NotEqual(t, uuid.Nil, snapshotID)

								patchSchemaBytes, err := warehouseutils.GenerateJSONPatch(originalSchemaBytes, updatedSchemaBytes)
								require.NoError(t, err)
								require.JSONEq(t, string(tc.schemaPatch), string(patchSchemaBytes))

								file := model.StagingFile{
									WorkspaceID:           "workspace_id",
									Location:              "s3://bucket/path/to/file-patch",
									SourceID:              "source_id",
									DestinationID:         "destination_id",
									Status:                warehouseutils.StagingFileWaitingState,
									Error:                 nil,
									FirstEventAt:          now.Add(time.Second),
									LastEventAt:           now,
									UseRudderStorage:      true,
									DestinationRevisionID: "destination_revision_id",
									TotalEvents:           100,
									SourceTaskRunID:       "source_task_run_id",
									SourceJobID:           "source_job_id",
									SourceJobRunID:        "source_job_run_id",
									TimeWindow:            time.Date(1993, 8, 1, 3, 0, 0, 0, time.UTC),
									ServerInstanceID:      "test-instance-id-patch",
								}.
									WithSchema(updatedSchemaBytes).
									WithSnapshotIDAndPatch(snapshotID, patchSchemaBytes)

								c := defaultConfig()
								rs := repo.NewStagingFiles(db, c, repo.WithNow(func() time.Time {
									return now
								}))
								id, err := rs.Insert(ctx, &file)
								require.NoError(t, err)

								retrievedSchemas, err := rs.GetSchemasByIDs(ctx, []int64{id})
								require.NoError(t, err)
								require.Len(t, retrievedSchemas, 1)
								require.Equal(t, tc.updatedSchema, retrievedSchemas[0])
							})
						}
					})
				})

				t.Run("GetEventTimeRangesByUploadID", func(t *testing.T) {
					t.Run("get all", func(t *testing.T) {
						u := repo.NewUploads(db)
						uploadId, err := u.CreateWithStagingFiles(ctx, model.Upload{}, stagingFiles)
						require.NoError(t, err)

						eventTimeRanges, err := r.GetEventTimeRangesByUploadID(ctx, uploadId)
						require.NoError(t, err)
						require.Len(t, eventTimeRanges, len(stagingFiles))

						for ind, etr := range eventTimeRanges {
							require.Equal(t, stagingFiles[ind].FirstEventAt, etr.FirstEventAt.UTC())
							require.Equal(t, stagingFiles[ind].LastEventAt, etr.LastEventAt.UTC())
						}
					})

					t.Run("empty staging files", func(t *testing.T) {
						eventTimeRanges, err := r.GetEventTimeRangesByUploadID(ctx, 100)
						require.NoError(t, err)
						require.Empty(t, eventTimeRanges)
					})
				})
			})

			t.Run("Pending", func(t *testing.T) {
				conf := defaultConfig()
				db, ctx := setupDBWithConfig(t, conf), context.Background()
				now := time.Now().Truncate(time.Second).UTC()
				r := repo.NewStagingFiles(db, conf, repo.WithNow(func() time.Time {
					return now
				}))
				uploadRepo := repo.NewUploads(db)

				inputData := []struct {
					SourceID      string
					DestinationID string
					Files         int
				}{
					{
						SourceID:      "source_id_1",
						DestinationID: "destination_id_1",
						Files:         10,
					},
					{
						SourceID:      "source_id_2",
						DestinationID: "destination_id_2",
						Files:         20,
					},
					{
						SourceID:      "source_id_2",
						DestinationID: "destination_id_3",
						Files:         16,
					},
				}

				for _, input := range inputData {
					stagingFiles := manyStagingFiles(input.Files, now)
					for i := range stagingFiles {
						stagingFiles[i].DestinationID = input.DestinationID
						stagingFiles[i].SourceID = input.SourceID

						file := stagingFiles[i].WithSchema([]byte(`{"type": "object"}`))

						id, err := r.Insert(ctx, &file)
						require.NoError(t, err)

						stagingFiles[i].ID = id
						stagingFiles[i].Error = nil
						stagingFiles[i].CreatedAt = now
						stagingFiles[i].UpdatedAt = now
					}
					pending, err := r.Pending(ctx, input.SourceID, input.DestinationID)
					require.NoError(t, err)
					require.Equal(t, stagingFiles, pending)

					countByDestID, err := r.CountPendingForDestination(ctx, input.DestinationID)
					require.NoError(t, err)
					require.Equal(t, int64(input.Files), countByDestID)

					countBySrcID, err := r.CountPendingForSource(ctx, input.SourceID)
					require.NoError(t, err)
					require.Equal(t, int64(input.Files), countBySrcID)

					uploadID, err := uploadRepo.CreateWithStagingFiles(ctx, model.Upload{
						SourceID:      input.SourceID,
						DestinationID: input.DestinationID,
					}, pending)
					require.NoError(t, err)

					pending, err = r.Pending(ctx, input.SourceID, input.DestinationID)
					require.NoError(t, err)
					require.Empty(t, pending)

					countByDestID, err = r.CountPendingForDestination(ctx, input.DestinationID)
					require.NoError(t, err)
					require.Zero(t, countByDestID)

					countBySrcID, err = r.CountPendingForSource(ctx, input.SourceID)
					require.NoError(t, err)
					require.Zero(t, countBySrcID)

					t.Run("Uploads", func(t *testing.T) {
						upload, err := uploadRepo.Get(ctx, uploadID)
						require.NoError(t, err)

						events, err := r.TotalEventsForUploadID(ctx, upload.ID)
						require.NoError(t, err)
						require.Equal(t, int64(input.Files)*100, events)

						revisionIDs, err := r.DestinationRevisionIDsForUploadID(ctx, upload.ID)
						require.NoError(t, err)
						require.Equal(t, []string{"destination_revision_id"}, revisionIDs)
					})
				}
			})

			t.Run("Status", func(t *testing.T) {
				conf := defaultConfig()
				db, ctx := setupDBWithConfig(t, conf), context.Background()
				now := time.Now().Truncate(time.Second).UTC()
				r := repo.NewStagingFiles(db, conf, repo.WithNow(func() time.Time {
					return now
				}))

				err := (&migrator.Migrator{
					Handle:          db.DB,
					MigrationsTable: "warehouse_runalways_migrations",
				}).MigrateFromTemplates("warehouse_always", map[string]interface{}{
					"config": conf,
				})
				require.NoError(t, err)

				n := 10
				for i := 0; i < n; i++ {
					file := model.StagingFile{
						WorkspaceID:   "workspace_id",
						Location:      fmt.Sprintf("s3://bucket/path/to/file-%d", i),
						SourceID:      "source_id",
						DestinationID: "destination_id",
						Status:        warehouseutils.StagingFileWaitingState,
						Error:         nil,
						FirstEventAt:  now.Add(time.Second),
						LastEventAt:   now,
					}.WithSchema([]byte(`{"type": "object"}`))

					id, err := r.Insert(ctx, &file)
					require.NoError(t, err)

					file.ID = id
					file.Error = nil
					file.CreatedAt = now
					file.UpdatedAt = now
				}

				t.Run("SetStatuses", func(t *testing.T) {
					statuses := []string{
						warehouseutils.StagingFileSucceededState,
						warehouseutils.StagingFileFailedState,
						warehouseutils.StagingFileExecutingState,
						warehouseutils.StagingFileWaitingState,
						warehouseutils.StagingFileAbortedState,
					}

					for _, status := range statuses {
						status := status
						t.Run(status, func(t *testing.T) {
							now = now.Add(time.Second)

							err := r.SetStatuses(ctx,
								[]int64{1, 2, 3},
								status,
							)
							require.NoError(t, err)

							files, err := r.GetForUploadID(ctx, 1)
							require.NoError(t, err)

							for _, file := range files {
								require.Equal(t, status, file.Status)
								require.Equal(t, now, file.UpdatedAt)
							}
						})
					}

					err := r.SetStatuses(ctx,
						[]int64{-1, 2, 3}, warehouseutils.StagingFileExecutingState)
					require.EqualError(t, err, "not all rows were updated: 2 != 3")

					err = r.SetStatuses(ctx,
						[]int64{}, warehouseutils.StagingFileExecutingState)
					require.EqualError(t, err, "no staging files to update")
				})
			})
		})
	}
}

func setupDB(t testing.TB) *sqlmiddleware.DB {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pgResource, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	err = (&migrator.Migrator{
		Handle:          pgResource.DB,
		MigrationsTable: "wh_schema_migrations",
	}).Migrate("warehouse")
	require.NoError(t, err)

	return sqlmiddleware.New(pgResource.DB)
}

func setupDBWithConfig(t testing.TB, conf *config.Config) *sqlmiddleware.DB {
	t.Helper()

	db := setupDB(t)

	err := (&migrator.Migrator{
		Handle:          db.DB,
		MigrationsTable: "warehouse_runalways_migrations",
	}).MigrateFromTemplates("warehouse_always", map[string]interface{}{
		"config": conf,
	})
	require.NoError(t, err)

	return sqlmiddleware.New(db.DB)
}

func manyStagingFiles(size int, now time.Time) []*model.StagingFile {
	files := make([]*model.StagingFile, size)
	for i := range files {
		files[i] = &model.StagingFile{
			WorkspaceID:           "workspace_id",
			Location:              fmt.Sprintf("s3://bucket/path/to/file-%d", i),
			SourceID:              "source_id",
			DestinationID:         "destination_id",
			Status:                warehouseutils.StagingFileWaitingState,
			Error:                 fmt.Errorf("dummy error"),
			FirstEventAt:          now.Add(time.Second),
			LastEventAt:           now,
			UseRudderStorage:      true,
			DestinationRevisionID: "destination_revision_id",
			TotalEvents:           100,
			SourceTaskRunID:       "source_task_run_id",
			SourceJobID:           "source_job_id",
			SourceJobRunID:        "source_job_run_id",
			TimeWindow:            time.Date(1993, 8, 1, 3, 0, 0, 0, time.UTC),
		}
	}
	return files
}

func TestStagingFileIDs(t *testing.T) {
	sfs := []*model.StagingFile{
		{
			ID: 1,
		},
		{
			ID: 2,
		},
		{
			ID: 3,
		},
	}
	ids := repo.StagingFileIDs(sfs)
	require.Equal(t, []int64{1, 2, 3}, ids)
}

func BenchmarkFiles(b *testing.B) {
	ctx := context.Background()
	db := setupDB(b)
	stagingRepo := repo.NewStagingFiles(db, config.New())
	uploadRepo := repo.NewUploads(db)

	size := 100000
	pending := 2

	for i := 0; i < size; i++ {
		file := model.StagingFile{
			WorkspaceID:   "workspace_id",
			Location:      fmt.Sprintf("s3://bucket/path/to/file-%d", i),
			SourceID:      "source_id",
			DestinationID: "destination_id",
			Status:        warehouseutils.StagingFileWaitingState,
			Error:         nil,
			FirstEventAt:  time.Now(),
			LastEventAt:   time.Now(),
		}.WithSchema([]byte(`{"type": "object"}`))

		id, err := stagingRepo.Insert(ctx, &file)
		require.NoError(b, err)

		if i >= (size - pending) {
			continue
		}

		_, err = uploadRepo.CreateWithStagingFiles(ctx, model.Upload{
			SourceID:      "source_id",
			DestinationID: "destination_id",
		}, []*model.StagingFile{
			{
				ID:            id,
				SourceID:      "source_id",
				DestinationID: "destination_id",
			},
		})
		require.NoError(b, err)
	}

	b.ResetTimer()

	b.Run("GetStagingFiles", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ff, err := stagingRepo.Pending(ctx, "source_id", "destination_id")
			require.NoError(b, err)
			require.Equal(b, pending, len(ff))
		}
	})
}
