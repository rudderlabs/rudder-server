package datalake_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/trinodb/trino-go-client/trino"

	"cloud.google.com/go/storage"

	"google.golang.org/api/option"

	"github.com/rudderlabs/compose-test/compose"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/rudderlabs/compose-test/testcompose"

	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"

	whth "github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"

	"github.com/stretchr/testify/require"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	_ "github.com/trinodb/trino-go-client/trino"
)

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}

	misc.Init()
	validations.Init()
	whutils.Init()

	azContainerName := "azure-datalake-test"
	s3BucketName := "some-bucket"
	gcsBucketName := "gcs-datalake-test"
	azAccountName := "MYACCESSKEY"
	azAccountKey := "TVlTRUNSRVRLRVk="
	s3Region := "us-east-1"
	s3AccessKeyID := "MYACCESSKEY"
	s3AccessKey := "MYSECRETKEY"

	t.Run("Events flow", func(t *testing.T) {
		httpPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.azure.yml", "testdata/docker-compose.gcs.yml", "../testdata/docker-compose.jobsdb.yml", "../testdata/docker-compose.minio.yml", "../testdata/docker-compose.transformer.yml"}))
		c.Start(context.Background())

		workspaceID := whutils.RandHex()
		jobsDBPort := c.Port("jobsDb", 5432)
		azEndPoint := fmt.Sprintf("localhost:%d", c.Port("azure", 10000))
		s3EndPoint := fmt.Sprintf("localhost:%d", c.Port("minio", 9000))
		gcsEndPoint := fmt.Sprintf("http://localhost:%d/storage/v1/", c.Port("gcs", 4443))
		transformerURL := fmt.Sprintf("http://localhost:%d", c.Port("transformer", 9090))

		jobsDB := whth.JobsDB(t, jobsDBPort)

		testCases := []struct {
			name           string
			tables         []string
			destType       string
			conf           map[string]interface{}
			prerequisite   func(t testing.TB, ctx context.Context)
			configOverride map[string]any
		}{
			{
				name:     "S3Datalake",
				tables:   []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				destType: whutils.S3Datalake,
				conf: map[string]interface{}{
					"region":           s3Region,
					"bucketName":       s3BucketName,
					"accessKeyID":      s3AccessKeyID,
					"accessKey":        s3AccessKey,
					"endPoint":         s3EndPoint,
					"enableSSE":        false,
					"s3ForcePathStyle": true,
					"disableSSL":       true,
					"prefix":           "some-prefix",
					"syncFrequency":    "30",
				},
				prerequisite: func(t testing.TB, ctx context.Context) {
					createMinioBucket(t, ctx, s3EndPoint, s3AccessKeyID, s3AccessKey, s3BucketName, s3Region)
				},
				configOverride: map[string]any{
					"region":           s3Region,
					"bucketName":       s3BucketName,
					"accessKeyID":      s3AccessKeyID,
					"accessKey":        s3AccessKey,
					"endPoint":         s3EndPoint,
					"enableSSE":        false,
					"s3ForcePathStyle": true,
					"disableSSL":       true,
				},
			},
			{
				name:     "GCSDatalake",
				tables:   []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases"},
				destType: whutils.GCSDatalake,
				conf: map[string]interface{}{
					"bucketName":    gcsBucketName,
					"prefix":        "",
					"endPoint":      gcsEndPoint,
					"disableSSL":    true,
					"jsonReads":     true,
					"syncFrequency": "30",
				},
				prerequisite: func(t testing.TB, ctx context.Context) {
					createGCSBucket(t, ctx, gcsEndPoint, gcsBucketName)
				},
				configOverride: map[string]any{
					"bucketName": gcsBucketName,
					"endPoint":   gcsEndPoint,
					"disableSSL": true,
					"jsonReads":  true,
				},
			},
			{
				name:     "AzureDatalake",
				tables:   []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				destType: whutils.AzureDatalake,
				conf: map[string]interface{}{
					"containerName":  azContainerName,
					"prefix":         "",
					"accountName":    azAccountName,
					"accountKey":     azAccountKey,
					"endPoint":       azEndPoint,
					"syncFrequency":  "30",
					"forcePathStyle": true,
					"disableSSL":     true,
				},
				configOverride: map[string]any{
					"containerName":  azContainerName,
					"accountName":    azAccountName,
					"accountKey":     azAccountKey,
					"endPoint":       azEndPoint,
					"forcePathStyle": true,
					"disableSSL":     true,
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				var (
					sourceID      = whutils.RandHex()
					destinationID = whutils.RandHex()
					writeKey      = whutils.RandHex()
					namespace     = whth.RandSchema(tc.destType)
				)

				destinationBuilder := backendconfigtest.NewDestinationBuilder(tc.destType).
					WithID(destinationID).
					WithRevisionID(destinationID).
					WithConfigOption("namespace", namespace).
					WithConfigOption("syncFrequency", "30")
				for k, v := range tc.configOverride {
					destinationBuilder = destinationBuilder.WithConfigOption(k, v)
				}
				destination := destinationBuilder.Build()

				workspaceConfig := backendconfigtest.NewConfigBuilder().
					WithSource(
						backendconfigtest.NewSourceBuilder().
							WithID(sourceID).
							WithWriteKey(writeKey).
							WithWorkspaceID(workspaceID).
							WithConnection(destination).
							Build(),
					).
					WithWorkspaceID(workspaceID).
					Build()

				t.Setenv("STORAGE_EMULATOR_HOST", fmt.Sprintf("localhost:%d", c.Port("gcs", 4443)))
				t.Setenv("RSERVER_WORKLOAD_IDENTITY_TYPE", "GKE")

				whth.BootstrapSvc(t, workspaceConfig, httpPort, jobsDBPort)

				ctx := context.Background()

				if tc.prerequisite != nil {
					tc.prerequisite(t, ctx)
				}

				t.Log("verifying test case 1")
				ts1 := whth.TestConfig{
					WriteKey:        writeKey,
					Schema:          namespace,
					Tables:          tc.tables,
					SourceID:        sourceID,
					DestinationID:   destinationID,
					DestinationType: tc.destType,
					Config:          tc.conf,
					WorkspaceID:     workspaceID,
					JobsDB:          jobsDB,
					HTTPPort:        httpPort,
					UserID:          whth.GetUserId(tc.destType),
					SkipWarehouse:   true,
					EventsFilePath:  "../testdata/upload-job.events-1.json",
					TransformerURL:  transformerURL,
					Destination:     destination,
				}
				ts1.VerifyEvents(t)

				t.Log("verifying test case 2")
				ts2 := whth.TestConfig{
					WriteKey:        writeKey,
					Schema:          namespace,
					Tables:          tc.tables,
					SourceID:        sourceID,
					DestinationID:   destinationID,
					DestinationType: tc.destType,
					Config:          tc.conf,
					WorkspaceID:     workspaceID,
					JobsDB:          jobsDB,
					HTTPPort:        httpPort,
					UserID:          whth.GetUserId(tc.destType),
					SkipWarehouse:   true,
					EventsFilePath:  "../testdata/upload-job.events-2.json",
					TransformerURL:  transformerURL,
					Destination:     destination,
				}
				ts2.VerifyEvents(t)
			})
		}
	})

	t.Run("Validations", func(t *testing.T) {
		t.Run("S3 DataLake", func(t *testing.T) {
			c := testcompose.New(t, compose.FilePaths([]string{"../testdata/docker-compose.minio.yml"}))
			c.Start(context.Background())

			s3EndPoint := fmt.Sprintf("localhost:%d", c.Port("minio", 9000))

			ctx := context.Background()
			namespace := whth.RandSchema(whutils.S3Datalake)

			createMinioBucket(t, ctx, s3EndPoint, s3AccessKeyID, s3AccessKey, s3BucketName, s3Region)

			dest := backendconfig.DestinationT{
				ID: "test_destination_id",
				Config: map[string]interface{}{
					"region":           s3Region,
					"bucketName":       s3BucketName,
					"accessKeyID":      s3AccessKeyID,
					"accessKey":        s3AccessKey,
					"endPoint":         s3EndPoint,
					"namespace":        namespace,
					"enableSSE":        false,
					"s3ForcePathStyle": true,
					"disableSSL":       true,
					"prefix":           "some-prefix",
					"syncFrequency":    "30",
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:          "1xAu2vuR0scUwkBivf6VhqwWgcS",
					Name:        "S3_DATALAKE",
					DisplayName: "S3 Datalake",
				},
				Name:       "s3-datalake-demo",
				Enabled:    true,
				RevisionID: "29HgOWobnr0RYZLpaSwPINb2987",
			}
			whth.VerifyConfigurationTest(t, dest)
		})

		t.Run("GCS DataLake", func(t *testing.T) {
			c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.gcs.yml"}))
			c.Start(context.Background())

			gcsEndPoint := fmt.Sprintf("http://localhost:%d/storage/v1/", c.Port("gcs", 4443))

			ctx := context.Background()
			namespace := whth.RandSchema(whutils.GCSDatalake)

			t.Setenv("STORAGE_EMULATOR_HOST", fmt.Sprintf("localhost:%d", c.Port("gcs", 4443)))
			t.Setenv("RSERVER_WORKLOAD_IDENTITY_TYPE", "GKE")

			createGCSBucket(t, ctx, gcsEndPoint, gcsBucketName)

			dest := backendconfig.DestinationT{
				ID: "test_destination_id",
				Config: map[string]interface{}{
					"bucketName":    gcsBucketName,
					"prefix":        "",
					"endPoint":      gcsEndPoint,
					"namespace":     namespace,
					"disableSSL":    true,
					"jsonReads":     true,
					"syncFrequency": "30",
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:          "20lzWVRwzEimkq87sNQuz1or2GA",
					Name:        "GCS_DATALAKE",
					DisplayName: "Google Cloud Storage Datalake",
				},
				Name:       "gcs-datalake-demo",
				Enabled:    true,
				RevisionID: "29HgOWobnr0RYZpLASwPINb2987",
			}
			whth.VerifyConfigurationTest(t, dest)
		})

		t.Run("Azure DataLake", func(t *testing.T) {
			c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.azure.yml"}))
			c.Start(context.Background())

			azEndPoint := fmt.Sprintf("localhost:%d", c.Port("azure", 10000))

			namespace := whth.RandSchema(whutils.AzureDatalake)

			dest := backendconfig.DestinationT{
				ID: "test_destination_id",
				Config: map[string]interface{}{
					"containerName":  azContainerName,
					"prefix":         "",
					"accountName":    azAccountName,
					"accountKey":     azAccountKey,
					"endPoint":       azEndPoint,
					"namespace":      namespace,
					"syncFrequency":  "30",
					"forcePathStyle": true,
					"disableSSL":     true,
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:          "20lzXg0c5kCBRxGoOoKjCSyZ3AC",
					Name:        "AZURE_DATALAKE",
					DisplayName: "Azure Datalake",
				},
				Name:       "azure-datalake-demo",
				Enabled:    true,
				RevisionID: "29HgOWobnr0RYZLpaSwPIbN2987",
			}
			whth.VerifyConfigurationTest(t, dest)
		})
	})

	t.Run("Trino", func(t *testing.T) {
		httpPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.trino.yml", "testdata/docker-compose.hive-metastore.yml", "../testdata/docker-compose.jobsdb.yml", "../testdata/docker-compose.minio.yml", "../testdata/docker-compose.transformer.yml"}))
		c.Start(context.Background())

		destType := whutils.S3Datalake

		workspaceID := whutils.RandHex()
		jobsDBPort := c.Port("jobsDb", 5432)
		s3EndPoint := fmt.Sprintf("localhost:%d", c.Port("minio", 9000))
		transformerURL := fmt.Sprintf("http://localhost:%d", c.Port("transformer", 9090))

		jobsDB := whth.JobsDB(t, jobsDBPort)

		ctx := context.Background()
		sourceID := whutils.RandHex()
		destinationID := whutils.RandHex()
		writeKey := whutils.RandHex()
		namespace := whth.RandSchema(destType)

		destination := backendconfigtest.NewDestinationBuilder(destType).
			WithID(destinationID).
			WithRevisionID(destinationID).
			WithConfigOption("namespace", namespace).
			WithConfigOption("syncFrequency", "30").
			WithConfigOption("region", s3Region).
			WithConfigOption("bucketName", s3BucketName).
			WithConfigOption("accessKeyID", s3AccessKeyID).
			WithConfigOption("accessKey", s3AccessKey).
			WithConfigOption("endPoint", s3EndPoint).
			WithConfigOption("enableSSE", false).
			WithConfigOption("s3ForcePathStyle", true).
			WithConfigOption("disableSSL", true).
			WithConfigOption("prefix", "some-prefix").
			Build()
		workspaceConfig := backendconfigtest.NewConfigBuilder().
			WithSource(
				backendconfigtest.NewSourceBuilder().
					WithID(sourceID).
					WithWriteKey(writeKey).
					WithWorkspaceID(workspaceID).
					WithConnection(destination).
					Build(),
			).
			WithWorkspaceID(workspaceID).
			Build()

		whth.BootstrapSvc(t, workspaceConfig, httpPort, jobsDBPort)

		createMinioBucket(t, ctx, s3EndPoint, s3AccessKeyID, s3AccessKey, s3BucketName, s3Region)

		ts := whth.TestConfig{
			WriteKey:        writeKey,
			Schema:          namespace,
			Tables:          []string{"tracks"},
			SourceID:        sourceID,
			DestinationID:   destinationID,
			DestinationType: destType,
			Config: map[string]interface{}{
				"region":           s3Region,
				"bucketName":       s3BucketName,
				"accessKeyID":      s3AccessKeyID,
				"accessKey":        s3AccessKey,
				"endPoint":         s3EndPoint,
				"enableSSE":        false,
				"s3ForcePathStyle": true,
				"disableSSL":       true,
				"prefix":           "some-prefix",
				"syncFrequency":    "30",
			},
			WorkspaceID:    workspaceID,
			JobsDB:         jobsDB,
			HTTPPort:       httpPort,
			UserID:         whth.GetUserId(destType),
			SkipWarehouse:  true,
			EventsFilePath: "../testdata/upload-job.events-1.json",
			TransformerURL: transformerURL,
			Destination:    destination,
		}
		ts.VerifyEvents(t)

		dsn := fmt.Sprintf("http://user@localhost:%d?catalog=minio&schema=default&session_properties=minio.parquet_use_column_index=true",
			c.Port("trino", 8080),
		)
		db, err := sql.Open("trino", dsn)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = db.Close()
		})

		require.Eventually(t, func() bool {
			_, err := db.ExecContext(ctx, `SELECT 1`)
			return err == nil
		},
			60*time.Second,
			100*time.Millisecond,
		)
		require.Eventually(t, func() bool {
			_, err = db.ExecContext(ctx, `
				CREATE SCHEMA IF NOT EXISTS minio.rudderstack WITH (
				location = 's3a://`+s3BucketName+`/')
			`)
			if err != nil {
				t.Log("create schema: ", err)
				return false
			}
			return true
		},
			60*time.Second,
			1*time.Second,
		)
		require.Eventually(t, func() bool {
			_, err = db.ExecContext(ctx, `
				CREATE TABLE IF NOT EXISTS minio.rudderstack.product_track (
					"_timestamp" TIMESTAMP,
					context_destination_id VARCHAR,
					context_destination_type VARCHAR,
					context_ip VARCHAR,
					context_request_ip VARCHAR,
					context_source_id VARCHAR,
					context_source_type VARCHAR,
					event VARCHAR,
					event_text VARCHAR,
					id VARCHAR,
					original_timestamp TIMESTAMP,
					received_at TIMESTAMP,
					sent_at TIMESTAMP,
					"timestamp" TIMESTAMP,
					user_id VARCHAR,
					uuid_ts TIMESTAMP,
					_as VARCHAR,
					review_body VARCHAR,
					_between VARCHAR,
					product_id VARCHAR,
					review_id VARCHAR,
					rating VARCHAR
				)
				WITH (
					external_location = 's3a://`+s3BucketName+`/some-prefix/rudder-datalake/`+namespace+`/product_track/2023/05/12/04/',
					format = 'PARQUET'
				)
			`)
			if err != nil {
				t.Log("create table: ", err)
				return false
			}
			return true
		},
			60*time.Second,
			1*time.Second,
		)

		var count int64
		require.Eventually(t, func() bool {
			err := db.QueryRowContext(ctx, `
				select
				    count(*)
				from
				     minio.rudderstack.product_track
			`).Scan(&count)
			if err != nil {
				t.Log("select count: ", err)
				return false
			}
			return true
		},
			60*time.Second,
			1*time.Second,
		)
		require.Equal(t, int64(4), count)

		require.Eventually(t, func() bool {
			err := db.QueryRowContext(ctx, `
				select
					count(*)
				from
					minio.rudderstack.product_track
				where
					context_destination_id = '`+destinationID+`'
			`).Scan(&count)
			if err != nil {
				t.Log("select count with where clause: ", err)
				return false
			}
			return true
		},
			60*time.Second,
			1*time.Second,
		)
		require.Equal(t, int64(4), count)

		t.Log("By default parquet_use_column_index=true and parquet_ignore_statistics=false")
		t.Log("parquet_use_column_index=true")
		require.Eventually(t, func() bool {
			err := db.QueryRowContext(ctx, `
				select
					count(*)
				from
					minio.rudderstack.product_track
				where
					_as = 'non escaped column'
			`).Scan(&count)
			if err != nil {
				t.Log("select count with where clause: ", err)
			}

			var e *trino.ErrQueryFailed
			if err != nil && errors.As(err, &e) && e.StatusCode == 200 {
				var ei *trino.ErrTrino
				if errors.As(e.Reason, &ei) && ei.ErrorName == "HIVE_CURSOR_ERROR" {
					return true
				}
			}
			return false
		},
			60*time.Second,
			1*time.Second,
		)

		t.Log("parquet_use_column_index=false")
		dsnWithoutIndex := fmt.Sprintf("http://user@localhost:%d?catalog=minio&schema=default&session_properties=minio.parquet_use_column_index=false",
			c.Port("trino", 8080),
		)
		dbWithoutIndex, err := sql.Open("trino", dsnWithoutIndex)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = dbWithoutIndex.Close()
		})

		require.Eventually(t, func() bool {
			err := dbWithoutIndex.QueryRowContext(ctx, `
				select
					count(*)
				from
					minio.rudderstack.product_track
				where
					_as = 'non escaped column'
			`).Scan(&count)
			if err != nil {
				t.Log("select count with where clause: ", err)
				return false
			}
			return true
		},
			60*time.Second,
			1*time.Second,
		)
		require.Equal(t, int64(1), count)

		t.Logf("parquet_ignore_statistics=true")
		dsnIgnoreStatistics := fmt.Sprintf("http://user@localhost:%d?catalog=minio&schema=default&session_properties=minio.parquet_ignore_statistics=true",
			c.Port("trino", 8080),
		)
		dbIgnoreStatistics, err := sql.Open("trino", dsnIgnoreStatistics)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = dbIgnoreStatistics.Close()
		})

		require.Eventually(t, func() bool {
			err := dbIgnoreStatistics.QueryRowContext(ctx, `
				select
					count(*)
				from
					minio.rudderstack.product_track
				where
					_as = 'non escaped column'
			`).Scan(&count)
			if err != nil {
				t.Log("select count with where clause: ", err)
				return false
			}
			return true
		},
			60*time.Second,
			1*time.Second,
		)
		require.Equal(t, int64(1), count)
	})

	t.Run("Spark", func(t *testing.T) {
		httpPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.spark.yml", "testdata/docker-compose.hive-metastore.yml", "../testdata/docker-compose.jobsdb.yml", "../testdata/docker-compose.minio.yml", "../testdata/docker-compose.transformer.yml"}))
		c.Start(context.Background())

		destType := whutils.S3Datalake

		workspaceID := whutils.RandHex()
		jobsDBPort := c.Port("jobsDb", 5432)
		s3EndPoint := fmt.Sprintf("localhost:%d", c.Port("minio", 9000))
		transformerURL := fmt.Sprintf("http://localhost:%d", c.Port("transformer", 9090))

		jobsDB := whth.JobsDB(t, jobsDBPort)

		ctx := context.Background()
		sourceID := whutils.RandHex()
		destinationID := whutils.RandHex()
		writeKey := whutils.RandHex()
		namespace := whth.RandSchema(destType)

		destination := backendconfigtest.NewDestinationBuilder(destType).
			WithID(destinationID).
			WithRevisionID(destinationID).
			WithConfigOption("namespace", namespace).
			WithConfigOption("syncFrequency", "30").
			WithConfigOption("region", s3Region).
			WithConfigOption("bucketName", s3BucketName).
			WithConfigOption("accessKeyID", s3AccessKeyID).
			WithConfigOption("accessKey", s3AccessKey).
			WithConfigOption("endPoint", s3EndPoint).
			WithConfigOption("enableSSE", false).
			WithConfigOption("s3ForcePathStyle", true).
			WithConfigOption("disableSSL", true).
			WithConfigOption("prefix", "some-prefix").
			Build()
		workspaceConfig := backendconfigtest.NewConfigBuilder().
			WithSource(
				backendconfigtest.NewSourceBuilder().
					WithID(sourceID).
					WithWriteKey(writeKey).
					WithWorkspaceID(workspaceID).
					WithConnection(destination).
					Build(),
			).
			WithWorkspaceID(workspaceID).
			Build()

		whth.BootstrapSvc(t, workspaceConfig, httpPort, jobsDBPort)

		createMinioBucket(t, ctx, s3EndPoint, s3AccessKeyID, s3AccessKey, s3BucketName, s3Region)

		ts := whth.TestConfig{
			WriteKey:        writeKey,
			Schema:          namespace,
			Tables:          []string{"tracks"},
			SourceID:        sourceID,
			DestinationID:   destinationID,
			DestinationType: destType,
			Config: map[string]interface{}{
				"region":           s3Region,
				"bucketName":       s3BucketName,
				"accessKeyID":      s3AccessKeyID,
				"accessKey":        s3AccessKey,
				"endPoint":         s3EndPoint,
				"enableSSE":        false,
				"s3ForcePathStyle": true,
				"disableSSL":       true,
				"prefix":           "some-prefix",
				"syncFrequency":    "30",
			},
			WorkspaceID:    workspaceID,
			JobsDB:         jobsDB,
			HTTPPort:       httpPort,
			UserID:         whth.GetUserId(destType),
			SkipWarehouse:  true,
			EventsFilePath: "../testdata/upload-job.events-2.json",
			TransformerURL: transformerURL,
			Destination:    destination,
		}
		ts.VerifyEvents(t)

		_ = c.Exec(ctx,
			"spark-master",
			"spark-sql",
			"-e",
			`
			CREATE EXTERNAL TABLE product_track (
			  	_timestamp timestamp,
				context_destination_id string,
			  	context_destination_type string,
			  	context_ip string,
				context_request_ip string,
			  	context_source_id string,
				context_source_type string,
			  	event string,
				event_text string,
                id string,
			  	original_timestamp timestamp,
				received_at timestamp,
			  	sent_at timestamp,
				timestamp timestamp,
			  	user_id string,
			  	uuid_ts timestamp,
				_as string,
				review_body string,
				_between string,
				product_id string,
				review_id string,
				rating string
			)
			STORED AS PARQUET
			location "s3a://`+s3BucketName+`/some-prefix/rudder-datalake/`+namespace+`/product_track/2023/05/12/04/";
		`,
			"-S",
		)

		countOutput := c.Exec(ctx,
			"spark-master",
			"spark-sql",
			"-e",
			`
				select
					count(*)
				from
					product_track;
			`,
			"-S",
		)
		countOutput = strings.ReplaceAll(strings.ReplaceAll(countOutput, "\n", ""), "\r", "") // remove trailing newline
		require.NotEmpty(t, countOutput)
		require.Equal(t, string(countOutput[len(countOutput)-1]), "4", countOutput) // last character is the count

		filteredCountOutput := c.Exec(ctx,
			"spark-master",
			"spark-sql",
			"-e",
			`
				select
					count(*)
				from
					product_track
				where
					context_destination_id = '`+destinationID+`';
			`,
			"-S",
		)
		filteredCountOutput = strings.ReplaceAll(strings.ReplaceAll(filteredCountOutput, "\n", ""), "\r", "") // remove trailing newline
		require.NotEmpty(t, filteredCountOutput)
		require.Equal(t, string(filteredCountOutput[len(filteredCountOutput)-1]), "4", filteredCountOutput) // last character is the count
	})
}

func createGCSBucket(t testing.TB, ctx context.Context, endpoint, bucketName string) {
	t.Helper()

	require.Eventually(t, func() bool {
		client, err := storage.NewClient(ctx, option.WithEndpoint(endpoint))
		if err != nil {
			t.Logf("create GCS client: %v", err)
			return false
		}

		bucket := client.Bucket(bucketName)

		_, err = bucket.Attrs(ctx)
		if err == nil {
			return true
		}
		if !errors.Is(err, storage.ErrBucketNotExist) {
			t.Log("bucket attrs: ", err)
			return false
		}

		err = bucket.Create(ctx, "test", &storage.BucketAttrs{
			Location: "US",
			Name:     bucketName,
		})
		if err != nil {
			t.Log("create bucket: ", err)
			return false
		}
		return true
	},
		30*time.Second,
		1*time.Second,
	)
}

func createMinioBucket(t testing.TB, ctx context.Context, endpoint, accessKeyId, accessKey, bucketName, region string) {
	t.Helper()

	require.Eventually(t, func() bool {
		minioClient, err := minio.New(endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(accessKeyId, accessKey, ""),
			Secure: false,
		})
		if err != nil {
			t.Log("create minio client: ", err)
			return false
		}

		exists, err := minioClient.BucketExists(ctx, bucketName)
		if err != nil {
			t.Log("check if bucket exists: ", err)
			return false
		}
		if exists {
			return true
		}

		err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: region})
		if err != nil {
			t.Log("make bucket: ", err)
			return false
		}
		return true
	},
		30*time.Second,
		1*time.Second,
	)
}
