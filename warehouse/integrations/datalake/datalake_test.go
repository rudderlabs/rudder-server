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

	"cloud.google.com/go/storage"
	"github.com/minio/minio-go/v7"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"github.com/trinodb/trino-go-client/trino"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/types"

	"github.com/rudderlabs/rudder-go-kit/filemanager"

	"google.golang.org/api/option"

	"github.com/rudderlabs/compose-test/compose"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
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
			verifySchema   func(*testing.T, filemanager.FileManager, string)
			verifyRecords  func(*testing.T, filemanager.FileManager, string, string, string)
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
					t.Helper()
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
				verifySchema: func(t *testing.T, fm filemanager.FileManager, namespace string) {
					t.Helper()
					schema := model.Schema{
						"aliases":       {"PARGO_PREFIX__timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Original_timestamp": "INT64", "Previous_id": "BYTE_ARRAY", "Received_at": "INT64", "Sent_at": "INT64", "User_id": "BYTE_ARRAY", "Uuid_ts": "INT64"},
						"groups":        {"PARGO_PREFIX__as": "BYTE_ARRAY", "PARGO_PREFIX__between": "BYTE_ARRAY", "PARGO_PREFIX__timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Employees": "INT64", "Group_id": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Industry": "BYTE_ARRAY", "Name": "BYTE_ARRAY", "Original_timestamp": "INT64", "Plan": "BYTE_ARRAY", "Received_at": "INT64", "Sent_at": "INT64", "User_id": "BYTE_ARRAY", "Uuid_ts": "INT64"},
						"identifies":    {"PARGO_PREFIX__as": "BYTE_ARRAY", "PARGO_PREFIX__between": "BYTE_ARRAY", "PARGO_PREFIX__timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Context_traits_as": "BYTE_ARRAY", "Context_traits_between": "BYTE_ARRAY", "Context_traits_email": "BYTE_ARRAY", "Context_traits_logins": "INT64", "Context_traits_name": "BYTE_ARRAY", "Email": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Logins": "INT64", "Name": "BYTE_ARRAY", "Original_timestamp": "INT64", "Received_at": "INT64", "Sent_at": "INT64", "User_id": "BYTE_ARRAY", "Uuid_ts": "INT64"},
						"pages":         {"PARGO_PREFIX__as": "BYTE_ARRAY", "PARGO_PREFIX__between": "BYTE_ARRAY", "PARGO_PREFIX__timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Name": "BYTE_ARRAY", "Original_timestamp": "INT64", "Received_at": "INT64", "Sent_at": "INT64", "Title": "BYTE_ARRAY", "Url": "BYTE_ARRAY", "User_id": "BYTE_ARRAY", "Uuid_ts": "INT64"},
						"product_track": {"PARGO_PREFIX__as": "BYTE_ARRAY", "PARGO_PREFIX__between": "BYTE_ARRAY", "PARGO_PREFIX__timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Event": "BYTE_ARRAY", "Event_text": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Original_timestamp": "INT64", "Product_id": "BYTE_ARRAY", "Rating": "INT64", "Received_at": "INT64", "Review_body": "BYTE_ARRAY", "Review_id": "BYTE_ARRAY", "Sent_at": "INT64", "User_id": "BYTE_ARRAY", "Uuid_ts": "INT64"},
						"screens":       {"PARGO_PREFIX__as": "BYTE_ARRAY", "PARGO_PREFIX__between": "BYTE_ARRAY", "PARGO_PREFIX__timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Name": "BYTE_ARRAY", "Original_timestamp": "INT64", "Received_at": "INT64", "Sent_at": "INT64", "Title": "BYTE_ARRAY", "Url": "BYTE_ARRAY", "User_id": "BYTE_ARRAY", "Uuid_ts": "INT64"},
						"tracks":        {"PARGO_PREFIX__timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Event": "BYTE_ARRAY", "Event_text": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Original_timestamp": "INT64", "Received_at": "INT64", "Sent_at": "INT64", "User_id": "BYTE_ARRAY", "Uuid_ts": "INT64"},
						"users":         {"PARGO_PREFIX__as": "BYTE_ARRAY", "PARGO_PREFIX__between": "BYTE_ARRAY", "PARGO_PREFIX__timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Context_traits_as": "BYTE_ARRAY", "Context_traits_between": "BYTE_ARRAY", "Context_traits_email": "BYTE_ARRAY", "Context_traits_logins": "INT64", "Context_traits_name": "BYTE_ARRAY", "Email": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Logins": "INT64", "Name": "BYTE_ARRAY", "Original_timestamp": "INT64", "Received_at": "INT64", "Sent_at": "INT64", "Uuid_ts": "INT64"},
					}

					fs := filesSchema(t, fm, "rudder-datalake/"+namespace+"/")
					for fileName, fileSchema := range fs {
						fileNameSplits := strings.Split(fileName, "/")
						require.GreaterOrEqual(t, len(fileNameSplits), 2)
						tableName := fileNameSplits[2]
						require.EqualValues(t, schema[tableName], lo.SliceToMap(fileSchema, func(col []string) (string, string) {
							return col[0], col[1]
						}))
					}
				},
				verifyRecords: func(t *testing.T, fm filemanager.FileManager, sourceID, destinationID, namespace string) {
					t.Helper()
					outputFormat := map[string][]string{
						"identifies":    {"User_id", "Uuid_ts", "Context_traits_logins", "PARGO_PREFIX__as", "Name", "Logins", "Email", "Original_timestamp", "Context_ip", "Context_traits_as", "PARGO_PREFIX__timestamp", "Received_at", "Context_destination_type", "Sent_at", "Context_source_type", "Context_traits_between", "Context_source_id", "Context_traits_name", "Context_request_ip", "PARGO_PREFIX__between", "Context_traits_email", "Context_destination_id", "Id"},
						"users":         {"Context_source_id", "Context_destination_type", "Context_request_ip", "Context_traits_name", "Context_traits_between", "PARGO_PREFIX__as", "Logins", "Sent_at", "Context_traits_logins", "Context_ip", "PARGO_PREFIX__between", "Context_traits_email", "PARGO_PREFIX__timestamp", "Context_destination_id", "Email", "Context_traits_as", "Context_source_type", "Id", "Uuid_ts", "Received_at", "Name", "Original_timestamp"},
						"tracks":        {"Original_timestamp", "Context_destination_id", "Context_destination_type", "Uuid_ts", "Context_source_type", "PARGO_PREFIX__timestamp", "Id", "Event", "Sent_at", "Context_ip", "Event_text", "Context_source_id", "Context_request_ip", "Received_at", "User_id"},
						"product_track": {"PARGO_PREFIX__timestamp", "User_id", "Product_id", "Received_at", "Context_source_id", "Sent_at", "Context_source_type", "Context_ip", "Context_destination_type", "Original_timestamp", "Context_request_ip", "Context_destination_id", "Uuid_ts", "PARGO_PREFIX__as", "Review_body", "PARGO_PREFIX__between", "Review_id", "Event_text", "Id", "Event", "Rating"},
						"pages":         {"User_id", "Context_source_id", "Id", "Title", "PARGO_PREFIX__timestamp", "Context_source_type", "PARGO_PREFIX__as", "Received_at", "Context_destination_id", "Context_ip", "Context_destination_type", "Name", "Original_timestamp", "PARGO_PREFIX__between", "Context_request_ip", "Sent_at", "Url", "Uuid_ts"},
						"screens":       {"Context_destination_type", "Url", "Context_source_type", "Title", "Original_timestamp", "User_id", "PARGO_PREFIX__between", "Context_ip", "Name", "Context_request_ip", "Uuid_ts", "Context_source_id", "Id", "Received_at", "Context_destination_id", "PARGO_PREFIX__timestamp", "Sent_at", "PARGO_PREFIX__as"},
						"aliases":       {"Context_source_id", "Context_destination_id", "Context_ip", "Sent_at", "Id", "User_id", "Uuid_ts", "Previous_id", "Original_timestamp", "Context_source_type", "Received_at", "Context_destination_type", "Context_request_ip", "PARGO_PREFIX__timestamp"},
						"groups":        {"Context_destination_type", "Id", "PARGO_PREFIX__between", "Plan", "Original_timestamp", "Uuid_ts", "Context_source_id", "Sent_at", "User_id", "Group_id", "Industry", "Context_request_ip", "Context_source_type", "PARGO_PREFIX__timestamp", "Employees", "PARGO_PREFIX__as", "Context_destination_id", "Received_at", "Name", "Context_ip"},
					}

					userIDFormat := "userId_s3_datalake"
					uuidFormat := "2023-01-01"

					er := eventRecords(t, fm, namespace, outputFormat, userIDFormat, uuidFormat)

					require.ElementsMatch(t, er["identifies"], whth.UploadJobIdentifiesRecords(userIDFormat, sourceID, destinationID, whutils.S3Datalake))
					require.ElementsMatch(t, er["users"], whth.UploadJobUsersRecordsForDatalake(userIDFormat, sourceID, destinationID, whutils.S3Datalake))
					require.ElementsMatch(t, er["tracks"], whth.UploadJobTracksRecords(userIDFormat, sourceID, destinationID, whutils.S3Datalake))
					require.ElementsMatch(t, er["product_track"], whth.UploadJobProductTrackRecords(userIDFormat, sourceID, destinationID, whutils.S3Datalake))
					require.ElementsMatch(t, er["pages"], whth.UploadJobPagesRecords(userIDFormat, sourceID, destinationID, whutils.S3Datalake))
					require.ElementsMatch(t, er["screens"], whth.UploadJobScreensRecords(userIDFormat, sourceID, destinationID, whutils.S3Datalake))
					require.ElementsMatch(t, er["aliases"], whth.UploadJobAliasesRecords(userIDFormat, sourceID, destinationID, whutils.S3Datalake))
					require.ElementsMatch(t, er["groups"], whth.UploadJobGroupsRecords(userIDFormat, sourceID, destinationID, whutils.S3Datalake))
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
					t.Helper()
					createGCSBucket(t, ctx, gcsEndPoint, gcsBucketName)
				},
				configOverride: map[string]any{
					"bucketName": gcsBucketName,
					"endPoint":   gcsEndPoint,
					"disableSSL": true,
					"jsonReads":  true,
				},
				verifySchema: func(t *testing.T, fm filemanager.FileManager, namespace string) {
					t.Helper()
					schema := model.Schema{
						"aliases":       {"Timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Original_timestamp": "INT64", "Previous_id": "BYTE_ARRAY", "Received_at": "INT64", "Sent_at": "INT64", "User_id": "BYTE_ARRAY", "Uuid_ts": "INT64"},
						"_groups":       {"PARGO_PREFIX__as": "BYTE_ARRAY", "PARGO_PREFIX__between": "BYTE_ARRAY", "Timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Employees": "INT64", "Group_id": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Industry": "BYTE_ARRAY", "Name": "BYTE_ARRAY", "Original_timestamp": "INT64", "Plan": "BYTE_ARRAY", "Received_at": "INT64", "Sent_at": "INT64", "User_id": "BYTE_ARRAY", "Uuid_ts": "INT64"},
						"identifies":    {"PARGO_PREFIX__as": "BYTE_ARRAY", "PARGO_PREFIX__between": "BYTE_ARRAY", "Timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Context_traits_as": "BYTE_ARRAY", "Context_traits_between": "BYTE_ARRAY", "Context_traits_email": "BYTE_ARRAY", "Context_traits_logins": "INT64", "Context_traits_name": "BYTE_ARRAY", "Email": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Logins": "INT64", "Name": "BYTE_ARRAY", "Original_timestamp": "INT64", "Received_at": "INT64", "Sent_at": "INT64", "User_id": "BYTE_ARRAY", "Uuid_ts": "INT64"},
						"pages":         {"PARGO_PREFIX__as": "BYTE_ARRAY", "PARGO_PREFIX__between": "BYTE_ARRAY", "Timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Name": "BYTE_ARRAY", "Original_timestamp": "INT64", "Received_at": "INT64", "Sent_at": "INT64", "Title": "BYTE_ARRAY", "Url": "BYTE_ARRAY", "User_id": "BYTE_ARRAY", "Uuid_ts": "INT64"},
						"product_track": {"PARGO_PREFIX__as": "BYTE_ARRAY", "PARGO_PREFIX__between": "BYTE_ARRAY", "Timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Event": "BYTE_ARRAY", "Event_text": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Original_timestamp": "INT64", "Product_id": "BYTE_ARRAY", "Rating": "INT64", "Received_at": "INT64", "Review_body": "BYTE_ARRAY", "Review_id": "BYTE_ARRAY", "Sent_at": "INT64", "User_id": "BYTE_ARRAY", "Uuid_ts": "INT64"},
						"screens":       {"PARGO_PREFIX__as": "BYTE_ARRAY", "PARGO_PREFIX__between": "BYTE_ARRAY", "Timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Name": "BYTE_ARRAY", "Original_timestamp": "INT64", "Received_at": "INT64", "Sent_at": "INT64", "Title": "BYTE_ARRAY", "Url": "BYTE_ARRAY", "User_id": "BYTE_ARRAY", "Uuid_ts": "INT64"},
						"tracks":        {"Timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Event": "BYTE_ARRAY", "Event_text": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Original_timestamp": "INT64", "Received_at": "INT64", "Sent_at": "INT64", "User_id": "BYTE_ARRAY", "Uuid_ts": "INT64"},
						"users":         {"PARGO_PREFIX__as": "BYTE_ARRAY", "PARGO_PREFIX__between": "BYTE_ARRAY", "Timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Context_traits_as": "BYTE_ARRAY", "Context_traits_between": "BYTE_ARRAY", "Context_traits_email": "BYTE_ARRAY", "Context_traits_logins": "INT64", "Context_traits_name": "BYTE_ARRAY", "Email": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Logins": "INT64", "Name": "BYTE_ARRAY", "Original_timestamp": "INT64", "Received_at": "INT64", "Sent_at": "INT64", "Uuid_ts": "INT64"},
					}

					fs := filesSchema(t, fm, "rudder-datalake/"+namespace+"/")
					for fileName, fileSchema := range fs {
						fileNameSplits := strings.Split(fileName, "/")
						require.GreaterOrEqual(t, len(fileNameSplits), 2)
						tableName := fileNameSplits[2]
						require.EqualValues(t, schema[tableName], lo.SliceToMap(fileSchema, func(col []string) (string, string) {
							return col[0], col[1]
						}))
					}
				},
				verifyRecords: func(t *testing.T, fm filemanager.FileManager, sourceID, destinationID, namespace string) {
					t.Helper()
					outputFormat := map[string][]string{
						"identifies":    {"User_id", "Uuid_ts", "Context_traits_logins", "PARGO_PREFIX__as", "Name", "Logins", "Email", "Original_timestamp", "Context_ip", "Context_traits_as", "Timestamp", "Received_at", "Context_destination_type", "Sent_at", "Context_source_type", "Context_traits_between", "Context_source_id", "Context_traits_name", "Context_request_ip", "PARGO_PREFIX__between", "Context_traits_email", "Context_destination_id", "Id"},
						"users":         {"Context_source_id", "Context_destination_type", "Context_request_ip", "Context_traits_name", "Context_traits_between", "PARGO_PREFIX__as", "Logins", "Sent_at", "Context_traits_logins", "Context_ip", "PARGO_PREFIX__between", "Context_traits_email", "Timestamp", "Context_destination_id", "Email", "Context_traits_as", "Context_source_type", "Id", "Uuid_ts", "Received_at", "Name", "Original_timestamp"},
						"tracks":        {"Original_timestamp", "Context_destination_id", "Context_destination_type", "Uuid_ts", "Context_source_type", "Timestamp", "Id", "Event", "Sent_at", "Context_ip", "Event_text", "Context_source_id", "Context_request_ip", "Received_at", "User_id"},
						"product_track": {"Timestamp", "User_id", "Product_id", "Received_at", "Context_source_id", "Sent_at", "Context_source_type", "Context_ip", "Context_destination_type", "Original_timestamp", "Context_request_ip", "Context_destination_id", "Uuid_ts", "PARGO_PREFIX__as", "Review_body", "PARGO_PREFIX__between", "Review_id", "Event_text", "Id", "Event", "Rating"},
						"pages":         {"User_id", "Context_source_id", "Id", "Title", "Timestamp", "Context_source_type", "PARGO_PREFIX__as", "Received_at", "Context_destination_id", "Context_ip", "Context_destination_type", "Name", "Original_timestamp", "PARGO_PREFIX__between", "Context_request_ip", "Sent_at", "Url", "Uuid_ts"},
						"screens":       {"Context_destination_type", "Url", "Context_source_type", "Title", "Original_timestamp", "User_id", "PARGO_PREFIX__between", "Context_ip", "Name", "Context_request_ip", "Uuid_ts", "Context_source_id", "Id", "Received_at", "Context_destination_id", "Timestamp", "Sent_at", "PARGO_PREFIX__as"},
						"aliases":       {"Context_source_id", "Context_destination_id", "Context_ip", "Sent_at", "Id", "User_id", "Uuid_ts", "Previous_id", "Original_timestamp", "Context_source_type", "Received_at", "Context_destination_type", "Context_request_ip", "Timestamp"},
						"_groups":       {"Context_destination_type", "Id", "PARGO_PREFIX__between", "Plan", "Original_timestamp", "Uuid_ts", "Context_source_id", "Sent_at", "User_id", "Group_id", "Industry", "Context_request_ip", "Context_source_type", "Timestamp", "Employees", "PARGO_PREFIX__as", "Context_destination_id", "Received_at", "Name", "Context_ip"},
					}

					userIDFormat := "userId_gcs_datalake"
					uuidFormat := "2023-01-01"

					er := eventRecords(t, fm, namespace, outputFormat, userIDFormat, uuidFormat)

					require.ElementsMatch(t, er["identifies"], whth.UploadJobIdentifiesRecords(userIDFormat, sourceID, destinationID, whutils.GCSDatalake))
					require.ElementsMatch(t, er["users"], whth.UploadJobUsersRecordsForDatalake(userIDFormat, sourceID, destinationID, whutils.GCSDatalake))
					require.ElementsMatch(t, er["tracks"], whth.UploadJobTracksRecords(userIDFormat, sourceID, destinationID, whutils.GCSDatalake))
					require.ElementsMatch(t, er["product_track"], whth.UploadJobProductTrackRecords(userIDFormat, sourceID, destinationID, whutils.GCSDatalake))
					require.ElementsMatch(t, er["pages"], whth.UploadJobPagesRecords(userIDFormat, sourceID, destinationID, whutils.GCSDatalake))
					require.ElementsMatch(t, er["screens"], whth.UploadJobScreensRecords(userIDFormat, sourceID, destinationID, whutils.GCSDatalake))
					require.ElementsMatch(t, er["aliases"], whth.UploadJobAliasesRecords(userIDFormat, sourceID, destinationID, whutils.GCSDatalake))
					require.ElementsMatch(t, er["_groups"], whth.UploadJobGroupsRecords(userIDFormat, sourceID, destinationID, whutils.GCSDatalake))
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
				verifySchema: func(t *testing.T, fm filemanager.FileManager, namespace string) {
					t.Helper()
					schema := model.Schema{
						"aliases":       {"Timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Original_timestamp": "INT64", "Previous_id": "BYTE_ARRAY", "Received_at": "INT64", "Sent_at": "INT64", "User_id": "BYTE_ARRAY", "Uuid_ts": "INT64"},
						"groups":        {"PARGO_PREFIX__as": "BYTE_ARRAY", "PARGO_PREFIX__between": "BYTE_ARRAY", "Timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Employees": "INT64", "Group_id": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Industry": "BYTE_ARRAY", "Name": "BYTE_ARRAY", "Original_timestamp": "INT64", "PARGO_PREFIX__plan": "BYTE_ARRAY", "Received_at": "INT64", "Sent_at": "INT64", "User_id": "BYTE_ARRAY", "Uuid_ts": "INT64"},
						"identifies":    {"PARGO_PREFIX__as": "BYTE_ARRAY", "PARGO_PREFIX__between": "BYTE_ARRAY", "Timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Context_traits_as": "BYTE_ARRAY", "Context_traits_between": "BYTE_ARRAY", "Context_traits_email": "BYTE_ARRAY", "Context_traits_logins": "INT64", "Context_traits_name": "BYTE_ARRAY", "Email": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Logins": "INT64", "Name": "BYTE_ARRAY", "Original_timestamp": "INT64", "Received_at": "INT64", "Sent_at": "INT64", "User_id": "BYTE_ARRAY", "Uuid_ts": "INT64"},
						"pages":         {"PARGO_PREFIX__as": "BYTE_ARRAY", "PARGO_PREFIX__between": "BYTE_ARRAY", "Timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Name": "BYTE_ARRAY", "Original_timestamp": "INT64", "Received_at": "INT64", "Sent_at": "INT64", "Title": "BYTE_ARRAY", "Url": "BYTE_ARRAY", "User_id": "BYTE_ARRAY", "Uuid_ts": "INT64"},
						"product_track": {"PARGO_PREFIX__as": "BYTE_ARRAY", "PARGO_PREFIX__between": "BYTE_ARRAY", "Timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Event": "BYTE_ARRAY", "Event_text": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Original_timestamp": "INT64", "Product_id": "BYTE_ARRAY", "Rating": "INT64", "Received_at": "INT64", "Review_body": "BYTE_ARRAY", "Review_id": "BYTE_ARRAY", "Sent_at": "INT64", "User_id": "BYTE_ARRAY", "Uuid_ts": "INT64"},
						"screens":       {"PARGO_PREFIX__as": "BYTE_ARRAY", "PARGO_PREFIX__between": "BYTE_ARRAY", "Timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Name": "BYTE_ARRAY", "Original_timestamp": "INT64", "Received_at": "INT64", "Sent_at": "INT64", "Title": "BYTE_ARRAY", "Url": "BYTE_ARRAY", "User_id": "BYTE_ARRAY", "Uuid_ts": "INT64"},
						"tracks":        {"Timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Event": "BYTE_ARRAY", "Event_text": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Original_timestamp": "INT64", "Received_at": "INT64", "Sent_at": "INT64", "User_id": "BYTE_ARRAY", "Uuid_ts": "INT64"},
						"users":         {"PARGO_PREFIX__as": "BYTE_ARRAY", "PARGO_PREFIX__between": "BYTE_ARRAY", "Timestamp": "INT64", "Context_destination_id": "BYTE_ARRAY", "Context_destination_type": "BYTE_ARRAY", "Context_ip": "BYTE_ARRAY", "Context_request_ip": "BYTE_ARRAY", "Context_source_id": "BYTE_ARRAY", "Context_source_type": "BYTE_ARRAY", "Context_traits_as": "BYTE_ARRAY", "Context_traits_between": "BYTE_ARRAY", "Context_traits_email": "BYTE_ARRAY", "Context_traits_logins": "INT64", "Context_traits_name": "BYTE_ARRAY", "Email": "BYTE_ARRAY", "Id": "BYTE_ARRAY", "Logins": "INT64", "Name": "BYTE_ARRAY", "Original_timestamp": "INT64", "Received_at": "INT64", "Sent_at": "INT64", "Uuid_ts": "INT64"},
					}

					fs := filesSchema(t, fm, "rudder-datalake/"+namespace+"/")
					for fileName, fileSchema := range fs {
						fileNameSplits := strings.Split(fileName, "/")
						require.GreaterOrEqual(t, len(fileNameSplits), 2)
						tableName := fileNameSplits[2]
						require.EqualValues(t, schema[tableName], lo.SliceToMap(fileSchema, func(col []string) (string, string) {
							return col[0], col[1]
						}))
					}
				},
				verifyRecords: func(t *testing.T, fm filemanager.FileManager, sourceID, destinationID, namespace string) {
					t.Helper()
					outputFormat := map[string][]string{
						"identifies":    {"User_id", "Uuid_ts", "Context_traits_logins", "PARGO_PREFIX__as", "Name", "Logins", "Email", "Original_timestamp", "Context_ip", "Context_traits_as", "Timestamp", "Received_at", "Context_destination_type", "Sent_at", "Context_source_type", "Context_traits_between", "Context_source_id", "Context_traits_name", "Context_request_ip", "PARGO_PREFIX__between", "Context_traits_email", "Context_destination_id", "Id"},
						"users":         {"Context_source_id", "Context_destination_type", "Context_request_ip", "Context_traits_name", "Context_traits_between", "PARGO_PREFIX__as", "Logins", "Sent_at", "Context_traits_logins", "Context_ip", "PARGO_PREFIX__between", "Context_traits_email", "Timestamp", "Context_destination_id", "Email", "Context_traits_as", "Context_source_type", "Id", "Uuid_ts", "Received_at", "Name", "Original_timestamp"},
						"tracks":        {"Original_timestamp", "Context_destination_id", "Context_destination_type", "Uuid_ts", "Context_source_type", "Timestamp", "Id", "Event", "Sent_at", "Context_ip", "Event_text", "Context_source_id", "Context_request_ip", "Received_at", "User_id"},
						"product_track": {"Timestamp", "User_id", "Product_id", "Received_at", "Context_source_id", "Sent_at", "Context_source_type", "Context_ip", "Context_destination_type", "Original_timestamp", "Context_request_ip", "Context_destination_id", "Uuid_ts", "PARGO_PREFIX__as", "Review_body", "PARGO_PREFIX__between", "Review_id", "Event_text", "Id", "Event", "Rating"},
						"pages":         {"User_id", "Context_source_id", "Id", "Title", "Timestamp", "Context_source_type", "PARGO_PREFIX__as", "Received_at", "Context_destination_id", "Context_ip", "Context_destination_type", "Name", "Original_timestamp", "PARGO_PREFIX__between", "Context_request_ip", "Sent_at", "Url", "Uuid_ts"},
						"screens":       {"Context_destination_type", "Url", "Context_source_type", "Title", "Original_timestamp", "User_id", "PARGO_PREFIX__between", "Context_ip", "Name", "Context_request_ip", "Uuid_ts", "Context_source_id", "Id", "Received_at", "Context_destination_id", "Timestamp", "Sent_at", "PARGO_PREFIX__as"},
						"aliases":       {"Context_source_id", "Context_destination_id", "Context_ip", "Sent_at", "Id", "User_id", "Uuid_ts", "Previous_id", "Original_timestamp", "Context_source_type", "Received_at", "Context_destination_type", "Context_request_ip", "Timestamp"},
						"groups":        {"Context_destination_type", "Id", "PARGO_PREFIX__between", "PARGO_PREFIX__plan", "Original_timestamp", "Uuid_ts", "Context_source_id", "Sent_at", "User_id", "Group_id", "Industry", "Context_request_ip", "Context_source_type", "Timestamp", "Employees", "PARGO_PREFIX__as", "Context_destination_id", "Received_at", "Name", "Context_ip"},
					}

					userIDFormat := "userId_azure_datalake"
					uuidFormat := "2023-01-01"

					er := eventRecords(t, fm, namespace, outputFormat, userIDFormat, uuidFormat)

					require.ElementsMatch(t, er["identifies"], whth.UploadJobIdentifiesRecords(userIDFormat, sourceID, destinationID, whutils.AzureDatalake))
					require.ElementsMatch(t, er["users"], whth.UploadJobUsersRecordsForDatalake(userIDFormat, sourceID, destinationID, whutils.AzureDatalake))
					require.ElementsMatch(t, er["tracks"], whth.UploadJobTracksRecords(userIDFormat, sourceID, destinationID, whutils.AzureDatalake))
					require.ElementsMatch(t, er["product_track"], whth.UploadJobProductTrackRecords(userIDFormat, sourceID, destinationID, whutils.AzureDatalake))
					require.ElementsMatch(t, er["pages"], whth.UploadJobPagesRecords(userIDFormat, sourceID, destinationID, whutils.AzureDatalake))
					require.ElementsMatch(t, er["screens"], whth.UploadJobScreensRecords(userIDFormat, sourceID, destinationID, whutils.AzureDatalake))
					require.ElementsMatch(t, er["aliases"], whth.UploadJobAliasesRecords(userIDFormat, sourceID, destinationID, whutils.AzureDatalake))
					require.ElementsMatch(t, er["groups"], whth.UploadJobGroupsRecords(userIDFormat, sourceID, destinationID, whutils.AzureDatalake))
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
					WithConfigOption("syncFrequency", "30").
					WithConfigOption("allowUsersContextTraits", true).
					WithConfigOption("underscoreDivideNumbers", true)
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

				storageProvider := whutils.ObjectStorageType(tc.destType, tc.conf, false)
				fm, err := filemanager.New(&filemanager.Settings{
					Provider: storageProvider,
					Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
						Provider:         storageProvider,
						Config:           tc.conf,
						UseRudderStorage: misc.IsConfiguredToUseRudderObjectStorage(tc.conf),
						WorkspaceID:      workspaceID,
					}),
				})
				require.NoError(t, err)

				t.Log("verifying schema")
				tc.verifySchema(t, fm, namespace)

				t.Log("verifying records")
				tc.verifyRecords(t, fm, sourceID, destinationID, namespace)
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
		t.Skip("skipping for 1.34.0-rc.3") //	TODO
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
			WithConfigOption("allowUsersContextTraits", true).
			WithConfigOption("underscoreDivideNumbers", true).
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
		t.Skip("skipping for 1.34.0-rc.3") //	TODO
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
			WithConfigOption("allowUsersContextTraits", true).
			WithConfigOption("underscoreDivideNumbers", true).
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

func filesSchema(t testing.TB, fm filemanager.FileManager, prefix string) map[string][][]string {
	t.Helper()

	ctx := context.Background()
	schemasMap := make(map[string][][]string)

	fileIter := fm.ListFilesWithPrefix(ctx, "", prefix, 1000)
	files, err := getAllFileNames(fileIter)
	require.NoError(t, err)

	for _, file := range files {
		f, err := os.CreateTemp(t.TempDir(), "")
		require.NoError(t, err)
		require.NoError(t, fm.Download(ctx, f, file))
		t.Cleanup(func() {
			_ = f.Close()
			_ = os.Remove(f.Name())
		})

		pf, err := local.NewLocalFileReader(f.Name())
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = pf.Close()
		})

		pr, err := reader.NewParquetReader(pf, nil, int64(100))
		require.NoError(t, err)
		t.Cleanup(func() {
			pr.ReadStop()
		})

		schemasMap[file] = lo.Map(pr.SchemaHandler.SchemaElements, func(item *parquet.SchemaElement, index int) []string {
			if item.Type == nil {
				return []string{item.Name, ""}
			}
			return []string{item.Name, item.Type.String()}
		})

		// Remove the root element from the schema
		schemasMap[file] = lo.Filter(schemasMap[file], func(item []string, index int) bool {
			return item[0] != "Parquet_go_root"
		})
	}
	return schemasMap
}

func eventRecords(t testing.TB, fm filemanager.FileManager, namespace string, outputFormat map[string][]string, userIDFormat, uuidFormat string) map[string][][]string {
	t.Helper()

	prefix := "rudder-datalake/" + namespace + "/"
	ctx := context.Background()
	recordsMap := make(map[string][][]string)

	fs := filesSchema(t, fm, prefix)

	for file, schema := range fs {
		fileNameSplits := strings.Split(file, "/")
		require.GreaterOrEqual(t, len(fileNameSplits), 2)
		tableName := fileNameSplits[2]

		f, err := os.CreateTemp(t.TempDir(), "")
		require.NoError(t, err)
		require.NoError(t, fm.Download(ctx, f, file))
		t.Cleanup(func() {
			_ = f.Close()
			_ = os.Remove(f.Name())
		})

		pf, err := local.NewLocalFileReader(f.Name())
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = pf.Close()
		})

		pr, err := reader.NewParquetReader(pf, nil, int64(100))
		require.NoError(t, err)
		t.Cleanup(func() {
			pr.ReadStop()
		})

		data := make([][]string, pr.GetNumRows())
		for i := 0; i < int(pr.GetNumRows()); i++ {
			data[i] = make([]string, len(pr.SchemaHandler.SchemaElements)-1)
		}

		for i := 0; i < len(pr.SchemaHandler.SchemaElements)-1; i++ {
			// Read the column data
			d, _, _, err := pr.ReadColumnByIndex(int64(i), pr.GetNumRows())
			require.NoError(t, err)

			// Map the column data to a string slice
			nd := lo.Map(d, func(item any, index int) string {
				switch item := item.(type) {
				case time.Time:
					return item.Format(time.RFC3339)
				case string:
					if t, err := time.Parse(time.RFC3339Nano, item); err == nil {
						return t.Format(time.RFC3339)
					}
					return item
				case int64:
					// Check if the int64 value is a plausible Unix timestamp
					if item > 1_000_000_000 && item < 10_000_000_000_000_000 {
						return types.TIMESTAMP_MICROSToTime(item, true).UTC().Format(time.RFC3339)
					}
					return cast.ToString(item) // Otherwise, handle it as a normal int64
				default:
					return cast.ToString(item)
				}
			})

			// Assign the column data to the data slice
			for j := 0; j < int(pr.GetNumRows()); j++ {
				data[j][i] = nd[j]
			}
		}

		// Truncate the user ID and UUID to the length of the format
		for _, record := range data {
			if tableName == whutils.UsersTable {
				if _, idx, found := lo.FindIndexOf(schema, func(item []string) bool {
					return item[0] == "Id"
				}); found {
					record[idx] = record[idx][0:len(userIDFormat)]
				}
			} else {
				if _, idx, found := lo.FindIndexOf(schema, func(item []string) bool {
					return item[0] == "User_id"
				}); found {
					record[idx] = record[idx][0:len(userIDFormat)]
				}
			}
			if _, idx, found := lo.FindIndexOf(schema, func(item []string) bool {
				return item[0] == "Uuid_ts"
			}); found {
				record[idx] = record[idx][0:len(uuidFormat)]
			}
		}

		// Reorder the data based on the output format
		outputData := make([][]string, len(data))
		for i := range data {
			outputData[i] = make([]string, len(data[i]))
		}

		// Find the index of the field in the schema and assign the data to the output data
		for i, field := range outputFormat[tableName] {
			_, idx, found := lo.FindIndexOf(schema, func(item []string) bool {
				return item[0] == field
			})
			require.True(t, found)

			for j := range data {
				outputData[j][i] = data[j][idx]
			}
		}

		recordsMap[tableName] = append(recordsMap[tableName], outputData...)
	}
	return recordsMap
}

func getAllFileNames(fileIter filemanager.ListSession) ([]string, error) {
	files := make([]string, 0)
	for {
		fileInfo, err := fileIter.Next()
		if err != nil {
			return files, err
		}
		if len(fileInfo) == 0 {
			break
		}
		for _, file := range fileInfo {
			files = append(files, file.Key)
		}
	}
	return files, nil
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
