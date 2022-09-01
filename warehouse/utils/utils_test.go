package warehouseutils_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/misc"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	. "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestDestinationConfigKeys(t *testing.T) {
	for _, whType := range WarehouseDestinations {
		t.Run(whType, func(t *testing.T) {
			require.Contains(t, WHDestNameMap, whType)

			whName := WHDestNameMap[whType]
			configKey := fmt.Sprintf("Warehouse.%s.feature", whName)
			got := config.TransformKey(configKey)
			expected := fmt.Sprintf("RSERVER_WAREHOUSE_%s_FEATURE", strings.ToUpper(whName))
			require.Equal(t, got, expected)
		})
	}
}

func TestGetS3Location(t *testing.T) {
	inputs := []struct {
		location   string
		s3Location string
		region     string
	}{
		{
			location:   "https://test-bucket.s3.amazonaws.com/test-object.csv",
			s3Location: "s3://test-bucket/test-object.csv",
			region:     "",
		},
		{
			location:   "https://test-bucket.s3.any-region.amazonaws.com/test-object.csv",
			s3Location: "s3://test-bucket/test-object.csv",
			region:     "any-region",
		},
		{
			location:   "https://my.test-bucket.s3.amazonaws.com/test-object.csv",
			s3Location: "s3://my.test-bucket/test-object.csv",
			region:     "",
		},
		{
			location:   "https://my.test-bucket.s3.us-west-1.amazonaws.com/test-object.csv",
			s3Location: "s3://my.test-bucket/test-object.csv",
			region:     "us-west-1",
		},
		{
			location:   "https://my.example.s3.bucket.s3.us-west-1.amazonaws.com/test-object.csv",
			s3Location: "s3://my.example.s3.bucket/test-object.csv",
			region:     "us-west-1",
		},
		{
			location:   "https://s3.amazonaws.com/test-bucket/test-object.csv",
			s3Location: "s3://test-bucket/test-object.csv",
			region:     "",
		},
		{
			location:   "https://s3.any-region.amazonaws.com/test-bucket/test-object.csv",
			s3Location: "s3://test-bucket/test-object.csv",
			region:     "any-region",
		},
		{
			location:   "https://s3.amazonaws.com/my.test-bucket/test-object.csv",
			s3Location: "s3://my.test-bucket/test-object.csv",
			region:     "",
		},
		{
			location:   "https://s3.us-west-1.amazonaws.com/my.test-bucket/test-object.csv",
			s3Location: "s3://my.test-bucket/test-object.csv",
			region:     "us-west-1",
		},
		{
			location:   "https://s3.amazonaws.com/bucket.with.a.dot/test-object.csv",
			s3Location: "s3://bucket.with.a.dot/test-object.csv",
			region:     "",
		},
		{
			location:   "https://s3.amazonaws.com/s3.rudderstack/test-object.csv",
			s3Location: "s3://s3.rudderstack/test-object.csv",
			region:     "",
		},
		{
			location:   "https://google.com",
			s3Location: "",
			region:     "",
		},
	}

	for _, input := range inputs {
		s3Location, region := GetS3Location(input.location)
		require.Equal(t, s3Location, input.s3Location)
		require.Equal(t, region, input.region)
	}
}

func TestCaptureRegexGroup(t *testing.T) {
	t.Run("Matches", func(t *testing.T) {
		inputs := []struct {
			regex   *regexp.Regexp
			pattern string
			groups  map[string]string
		}{
			{
				regex:   S3PathStyleRegex,
				pattern: "https://s3.amazonaws.com/bucket.with.a.dot/keyname",
				groups: map[string]string{
					"bucket":  "bucket.with.a.dot",
					"keyname": "keyname",
					"region":  "",
				},
			},
			{
				regex:   S3PathStyleRegex,
				pattern: "https://s3.us-east.amazonaws.com/bucket.with.a.dot/keyname",
				groups: map[string]string{
					"bucket":  "bucket.with.a.dot",
					"keyname": "keyname",
					"region":  "us-east",
				},
			},
			{
				regex:   S3VirtualHostedRegex,
				pattern: "https://bucket.with.a.dot.s3.amazonaws.com/keyname",
				groups: map[string]string{
					"bucket":  "bucket.with.a.dot",
					"keyname": "keyname",
					"region":  "",
				},
			},
			{
				regex:   S3VirtualHostedRegex,
				pattern: "https://bucket.with.a.dot.s3.amazonaws.com/keyname",
				groups: map[string]string{
					"bucket":  "bucket.with.a.dot",
					"keyname": "keyname",
					"region":  "",
				},
			},
		}
		for _, input := range inputs {
			got, err := CaptureRegexGroup(input.regex, input.pattern)
			require.NoError(t, err)
			require.Equal(t, got, input.groups)
		}
	})
	t.Run("Not Matches", func(t *testing.T) {
		inputs := []struct {
			regex   *regexp.Regexp
			pattern string
		}{
			{
				regex:   S3PathStyleRegex,
				pattern: "https://google.com",
			},
			{
				regex:   S3VirtualHostedRegex,
				pattern: "https://google.com",
			},
		}
		for _, input := range inputs {
			_, err := CaptureRegexGroup(input.regex, input.pattern)
			require.Error(t, err)
		}
	})
}

func TestGetS3LocationFolder(t *testing.T) {
	inputs := []struct {
		s3Location       string
		s3LocationFolder string
	}{
		{
			s3Location:       "https://test-bucket.s3.amazonaws.com/myfolder/test-object.csv",
			s3LocationFolder: "s3://test-bucket/myfolder",
		},
		{
			s3Location:       "https://test-bucket.s3.eu-west-2.amazonaws.com/myfolder/test-object.csv",
			s3LocationFolder: "s3://test-bucket/myfolder",
		},
		{
			s3Location:       "https://my.test-bucket.s3.eu-west-2.amazonaws.com/myfolder/test-object.csv",
			s3LocationFolder: "s3://my.test-bucket/myfolder",
		},
	}
	for _, input := range inputs {
		s3LocationFolder := GetS3LocationFolder(input.s3Location)
		require.Equal(t, s3LocationFolder, input.s3LocationFolder)
	}
}

func TestGetS3Locations(t *testing.T) {
	inputs := []LoadFileT{
		{Location: "https://test-bucket.s3.amazonaws.com/test-object.csv"},
		{Location: "https://test-bucket.s3.eu-west-1.amazonaws.com/test-object.csv"},
		{Location: "https://my.test-bucket.s3.amazonaws.com/test-object.csv"},
		{Location: "https://my.test-bucket.s3.us-west-1.amazonaws.com/test-object.csv"},
	}
	outputs := []LoadFileT{
		{Location: "s3://test-bucket/test-object.csv"},
		{Location: "s3://test-bucket/test-object.csv"},
		{Location: "s3://my.test-bucket/test-object.csv"},
		{Location: "s3://my.test-bucket/test-object.csv"},
	}

	s3Locations := GetS3Locations(inputs)
	require.Equal(t, s3Locations, outputs)
}

func TestGetGCSLocation(t *testing.T) {
	inputs := []struct {
		location    string
		gcsLocation string
		format      string
	}{
		{
			location:    "https://storage.googleapis.com/test-bucket/test-object.csv",
			gcsLocation: "gs://test-bucket/test-object.csv",
		},
		{
			location:    "https://storage.googleapis.com/my.test-bucket/test-object.csv",
			gcsLocation: "gs://my.test-bucket/test-object.csv",
		},
		{
			location:    "https://storage.googleapis.com/test-bucket/test-object.csv",
			gcsLocation: "gcs://test-bucket/test-object.csv",
			format:      "gcs",
		},
		{
			location:    "https://storage.googleapis.com/my.test-bucket/test-object.csv",
			gcsLocation: "gcs://my.test-bucket/test-object.csv",
			format:      "gcs",
		},
	}
	for _, input := range inputs {
		gcsLocation := GetGCSLocation(input.location, GCSLocationOptionsT{
			TLDFormat: input.format,
		})
		require.Equal(t, gcsLocation, input.gcsLocation)
	}
}

func TestGetGCSLocationFolder(t *testing.T) {
	inputs := []struct {
		location          string
		gcsLocationFolder string
	}{
		{
			location:          "https://storage.googleapis.com/test-bucket/test-object.csv",
			gcsLocationFolder: "gs://test-bucket",
		},
		{
			location:          "https://storage.googleapis.com/my.test-bucket/test-object.csv",
			gcsLocationFolder: "gs://my.test-bucket",
		},
	}
	for _, input := range inputs {
		gcsLocationFolder := GetGCSLocationFolder(input.location, GCSLocationOptionsT{})
		require.Equal(t, gcsLocationFolder, input.gcsLocationFolder)
	}
}

func TestGetGCSLocations(t *testing.T) {
	inputs := []LoadFileT{
		{Location: "https://storage.googleapis.com/test-bucket/test-object.csv"},
		{Location: "https://storage.googleapis.com/my.test-bucket/test-object.csv"},
		{Location: "https://storage.googleapis.com/my.test-bucket2/test-object.csv"},
		{Location: "https://storage.googleapis.com/my.test-bucket/test-object2.csv"},
	}
	outputs := []string{
		"gs://test-bucket/test-object.csv",
		"gs://my.test-bucket/test-object.csv",
		"gs://my.test-bucket2/test-object.csv",
		"gs://my.test-bucket/test-object2.csv",
	}

	gcsLocations := GetGCSLocations(inputs, GCSLocationOptionsT{})
	require.Equal(t, gcsLocations, outputs)
}

func TestGetAzureBlobLocation(t *testing.T) {
	inputs := []struct {
		location       string
		azBlobLocation string
	}{
		{
			location:       "https://myproject.blob.core.windows.net/test-bucket/test-object.csv",
			azBlobLocation: "azure://myproject.blob.core.windows.net/test-bucket/test-object.csv",
		},
	}
	for _, input := range inputs {
		azBlobLocation := GetAzureBlobLocation(input.location)
		require.Equal(t, azBlobLocation, input.azBlobLocation)
	}
}

func TestGetAzureBlobLocationFolder(t *testing.T) {
	inputs := []struct {
		location             string
		azBlobLocationFolder string
	}{
		{
			location:             "https://myproject.blob.core.windows.net/test-bucket/myfolder/test-object.csv",
			azBlobLocationFolder: "azure://myproject.blob.core.windows.net/test-bucket/myfolder",
		},
	}
	for _, input := range inputs {
		azBlobLocationFolder := GetAzureBlobLocationFolder(input.location)
		require.Equal(t, azBlobLocationFolder, input.azBlobLocationFolder)
	}
}

func TestToSafeNamespace(t *testing.T) {
	inputs := []struct {
		provider      string
		namespace     string
		safeNamespace string
	}{
		{
			namespace:     "omega",
			safeNamespace: "omega",
		},
		{
			namespace:     "omega v2 ",
			safeNamespace: "omega_v_2",
		},
		{
			namespace:     "9mega",
			safeNamespace: "_9_mega",
		},
		{
			namespace:     "mega&",
			safeNamespace: "mega",
		},
		{
			namespace:     "ome$ga",
			safeNamespace: "ome_ga",
		},
		{
			namespace:     "omega$",
			safeNamespace: "omega",
		},
		{
			namespace:     "ome_ ga",
			safeNamespace: "ome_ga",
		},
		{
			namespace:     "9mega________-________90",
			safeNamespace: "_9_mega_90",
		},
		{
			namespace:     "Cízǔ",
			safeNamespace: "c_z",
		},
		{
			namespace:     "Rudderstack",
			safeNamespace: "rudderstack",
		},
		{
			namespace:     "___",
			safeNamespace: "stringempty",
		},
		{
			provider:      "RS",
			namespace:     "group",
			safeNamespace: "_group",
		},
	}
	for _, input := range inputs {
		safeNamespace := ToSafeNamespace(input.provider, input.namespace)
		require.Equal(t, safeNamespace, input.safeNamespace)
	}
}

func TestGetObjectLocation(t *testing.T) {
	inputs := []struct {
		provider       string
		location       string
		objectLocation string
	}{
		{
			provider:       "S3",
			location:       "https://test-bucket.s3.amazonaws.com/test-object.csv",
			objectLocation: "s3://test-bucket/test-object.csv",
		},
		{
			provider:       "GCS",
			location:       "https://storage.googleapis.com/my.test-bucket/test-object.csv",
			objectLocation: "gcs://my.test-bucket/test-object.csv",
		},
		{
			provider:       "AZURE_BLOB",
			location:       "https://myproject.blob.core.windows.net/test-bucket/test-object.csv",
			objectLocation: "azure://myproject.blob.core.windows.net/test-bucket/test-object.csv",
		},
	}

	for _, input := range inputs {
		t.Run(input.provider, func(t *testing.T) {
			objectLocation := GetObjectLocation(input.provider, input.location)
			require.Equal(t, objectLocation, input.objectLocation)
		})
	}
}

func TestGetObjectFolder(t *testing.T) {
	inputs := []struct {
		provider     string
		location     string
		objectFolder string
	}{
		{
			provider:     "S3",
			location:     "https://test-bucket.s3.amazonaws.com/myfolder/test-object.csv",
			objectFolder: "s3://test-bucket/myfolder",
		},
		{
			provider:     "GCS",
			location:     "https://storage.googleapis.com/test-bucket/test-object.csv",
			objectFolder: "gcs://test-bucket",
		},
		{
			provider:     "AZURE_BLOB",
			location:     "https://myproject.blob.core.windows.net/test-bucket/myfolder/test-object.csv",
			objectFolder: "azure://myproject.blob.core.windows.net/test-bucket/myfolder",
		},
	}

	for _, input := range inputs {
		t.Run(input.provider, func(t *testing.T) {
			objectFolder := GetObjectFolder(input.provider, input.location)
			require.Equal(t, objectFolder, input.objectFolder)
		})
	}
}

func TestGetObjectFolderForDeltalake(t *testing.T) {
	inputs := []struct {
		provider     string
		location     string
		objectFolder string
	}{
		{
			provider:     "S3",
			location:     "https://test-bucket.s3.amazonaws.com/myfolder/test-object.csv",
			objectFolder: "s3://test-bucket/myfolder",
		},
		{
			provider:     "GCS",
			location:     "https://storage.googleapis.com/test-bucket/test-object.csv",
			objectFolder: "gs://test-bucket",
		},
		{
			provider:     "AZURE_BLOB",
			location:     "https://myproject.blob.core.windows.net/test-bucket/myfolder/test-object.csv",
			objectFolder: "wasbs://test-bucket@myproject.blob.core.windows.net/myfolder",
		},
	}

	for _, input := range inputs {
		t.Run(input.provider, func(t *testing.T) {
			objectFolder := GetObjectFolderForDeltalake(input.provider, input.location)
			require.Equal(t, objectFolder, input.objectFolder)
		})
	}
}

func TestDoubleQuoteAndJoinByComma(t *testing.T) {
	names := []string{"Samantha Edwards", "Samantha Smith", "Holly Miller", "Tammie Tyler", "Gina Richards"}
	want := "\"Samantha Edwards\",\"Samantha Smith\",\"Holly Miller\",\"Tammie Tyler\",\"Gina Richards\""
	got := DoubleQuoteAndJoinByComma(names)
	require.Equal(t, got, want)
}

func TestSortColumnKeysFromColumnMap(t *testing.T) {
	columnMap := map[string]string{"k5": "V5", "k4": "V4", "k3": "V3", "k2": "V2", "k1": "V1"}
	want := []string{"k1", "k2", "k3", "k4", "k5"}
	got := SortColumnKeysFromColumnMap(columnMap)
	require.Equal(t, got, want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v want %#v input %#v", got, want, columnMap)
	}
}

func TestGetLoadFileGenTime(t *testing.T) {
	inputs := []struct {
		timingsMap        sql.NullString
		loadFilesEpochStr string
	}{
		{
			timingsMap: sql.NullString{
				String: "[{\"generating_upload_schema\":\"2022-07-04T16:09:03.001Z\"},{\"generated_upload_schema\":\"2022-07-04T16:09:04.141Z\"},{\"creating_table_uploads\":\"2022-07-04T16:09:04.144Z\"},{\"created_table_uploads\":\"2022-07-04T16:09:04.164Z\"},{\"generating_load_files\":\"2022-07-04T16:09:04.169Z\"},{\"generated_load_files\":\"2022-07-04T16:09:40.957Z\"},{\"updating_table_uploads_counts\":\"2022-07-04T16:09:40.959Z\"},{\"updated_table_uploads_counts\":\"2022-07-04T16:09:41.916Z\"},{\"creating_remote_schema\":\"2022-07-04T16:09:41.918Z\"},{\"created_remote_schema\":\"2022-07-04T16:09:41.920Z\"},{\"exporting_data\":\"2022-07-04T16:09:41.922Z\"},{\"exporting_data_failed\":\"2022-07-04T17:14:24.424Z\"}]",
			},
			loadFilesEpochStr: "2022-07-04T16:09:04.169Z",
		},
		{
			timingsMap: sql.NullString{
				String: "[]",
			},
			loadFilesEpochStr: "0001-01-01T00:00:00.000Z",
		},
		{
			timingsMap: sql.NullString{
				String: "[{\"generating_upload_schema\":\"2022-07-04T16:09:03.001Z\"},{\"generated_upload_schema\":\"2022-07-04T16:09:04.141Z\"},{\"creating_table_uploads\":\"2022-07-04T16:09:04.144Z\"},{\"created_table_uploads\":\"2022-07-04T16:09:04.164Z\"}]",
			},
			loadFilesEpochStr: "0001-01-01T00:00:00.000Z",
		},
	}
	for _, input := range inputs {
		loadFilesEpochTime, err := time.Parse(misc.RFC3339Milli, input.loadFilesEpochStr)
		require.NoError(t, err)

		loadFileGenTime := GetLoadFileGenTime(input.timingsMap)
		require.Equal(t, loadFilesEpochTime, loadFileGenTime)
	}
}

func TestGetLastFailedStatus(t *testing.T) {
	inputs := []struct {
		timingsMap sql.NullString
		status     string
	}{
		{
			timingsMap: sql.NullString{
				String: "[{\"generating_upload_schema\":\"2022-07-04T16:09:03.001Z\"},{\"generated_upload_schema\":\"2022-07-04T16:09:04.141Z\"},{\"creating_table_uploads\":\"2022-07-04T16:09:04.144Z\"},{\"created_table_uploads\":\"2022-07-04T16:09:04.164Z\"},{\"generating_load_files\":\"2022-07-04T16:09:04.169Z\"},{\"generated_load_files\":\"2022-07-04T16:09:40.957Z\"},{\"updating_table_uploads_counts\":\"2022-07-04T16:09:40.959Z\"},{\"updated_table_uploads_counts\":\"2022-07-04T16:09:41.916Z\"},{\"creating_remote_schema\":\"2022-07-04T16:09:41.918Z\"},{\"created_remote_schema\":\"2022-07-04T16:09:41.920Z\"},{\"exporting_data\":\"2022-07-04T16:09:41.922Z\"},{\"exporting_data_failed\":\"2022-07-04T17:14:24.424Z\"}]",
			},
			status: "exporting_data_failed",
		},
		{
			timingsMap: sql.NullString{
				String: "[]",
			},
			status: "",
		},
		{
			timingsMap: sql.NullString{
				String: "[{\"generating_upload_schema\":\"2022-07-04T16:09:03.001Z\"},{\"generated_upload_schema\":\"2022-07-04T16:09:04.141Z\"},{\"creating_table_uploads\":\"2022-07-04T16:09:04.144Z\"},{\"created_table_uploads\":\"2022-07-04T16:09:04.164Z\"}]",
			},
			status: "",
		},
	}
	for _, input := range inputs {
		status := GetLastFailedStatus(input.timingsMap)
		require.Equal(t, status, input.status)
	}
}

func TestTimingFromJSONString(t *testing.T) {
	inputs := []struct {
		timingsMap        sql.NullString
		loadFilesEpochStr string
		status            string
	}{
		{
			timingsMap: sql.NullString{
				String: "{\"generating_upload_schema\":\"2022-07-04T16:09:04.000Z\"}",
			},
			status:            "generating_upload_schema",
			loadFilesEpochStr: "2022-07-04T16:09:04.000Z",
		},
		{
			timingsMap: sql.NullString{
				String: "{}",
			},
			status:            "",
			loadFilesEpochStr: "0001-01-01T00:00:00.000Z",
		},
	}
	for _, input := range inputs {
		loadFilesEpochTime, err := time.Parse(misc.RFC3339Milli, input.loadFilesEpochStr)
		require.NoError(t, err)

		status, recordedTime := TimingFromJSONString(input.timingsMap)
		require.Equal(t, status, input.status)
		require.Equal(t, loadFilesEpochTime, recordedTime)
	}
}

func TestGetConfigValue(t *testing.T) {
	inputs := []struct {
		key       string
		value     string
		warehouse WarehouseT
	}{
		{
			key:   "k1",
			value: "v1",
			warehouse: WarehouseT{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"k1": "v1",
					},
				},
			},
		},
		{
			key: "u1",
			warehouse: WarehouseT{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{},
				},
			},
		},
	}
	for _, input := range inputs {
		value := GetConfigValue(input.key, input.warehouse)
		require.Equal(t, value, input.value)
	}
}

func TestGetConfigValueBoolString(t *testing.T) {
	inputs := []struct {
		key       string
		value     string
		warehouse WarehouseT
	}{
		{
			key:   "k1",
			value: "true",
			warehouse: WarehouseT{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"k1": true,
					},
				},
			},
		},
		{
			key:   "k1",
			value: "false",
			warehouse: WarehouseT{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"k1": false,
					},
				},
			},
		},
		{
			key:   "u1",
			value: "false",
			warehouse: WarehouseT{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{},
				},
			},
		},
	}
	for _, input := range inputs {
		value := GetConfigValueBoolString(input.key, input.warehouse)
		require.Equal(t, value, input.value)
	}
}

func TestGetConfigValueAsMap(t *testing.T) {
	inputs := []struct {
		key    string
		value  map[string]interface{}
		config map[string]interface{}
	}{
		{
			key: "map",
			value: map[string]interface{}{
				"k1": "v1",
			},
			config: map[string]interface{}{
				"map": map[string]interface{}{
					"k1": "v1",
				},
			},
		},
		{
			key:    "map",
			value:  map[string]interface{}{},
			config: map[string]interface{}{},
		},
	}
	for idx, input := range inputs {
		value := GetConfigValueAsMap(input.key, input.config)
		if !reflect.DeepEqual(value, input.value) {
			t.Errorf("got %q want %q input %d", value, input.value, idx)
		}
	}
}

func TestJoinWithFormatting(t *testing.T) {
	separator := ","
	format := func(idx int, name string) string {
		return fmt.Sprintf(`%s_v%d`, name, idx+1)
	}
	inputs := []struct {
		keys  []string
		value string
	}{
		{
			keys:  []string{"k1", "k2"},
			value: "k1_v1,k2_v2",
		},
		{
			keys: []string{},
		},
	}
	for _, input := range inputs {
		value := JoinWithFormatting(input.keys, format, separator)
		require.Equal(t, value, input.value)
	}
}

func TestToProviderCase(t *testing.T) {
	inputs := []struct {
		provider string
		value    string
	}{
		{
			provider: SNOWFLAKE,
			value:    "RAND",
		},
		{
			provider: POSTGRES,
			value:    "rand",
		},
		{
			provider: CLICKHOUSE,
			value:    "rand",
		},
	}
	for _, input := range inputs {
		t.Run(input.provider, func(t *testing.T) {
			value := ToProviderCase(input.provider, "rand")
			require.Equal(t, value, input.value)
		})
	}
}

func TestSnowflakeCloudProvider(t *testing.T) {
	inputs := []struct {
		config   interface{}
		provider string
	}{
		{
			config: map[string]interface{}{
				"cloudProvider": "GCP",
			},
			provider: "GCP",
		},
		{
			config:   map[string]interface{}{},
			provider: "AWS",
		},
	}
	for _, input := range inputs {
		provider := SnowflakeCloudProvider(input.config)
		require.Equal(t, provider, input.provider)
	}
}

func TestObjectStorageType(t *testing.T) {
	inputs := []struct {
		destType         string
		config           interface{}
		useRudderStorage bool
		storageType      string
	}{
		{
			config:           map[string]interface{}{},
			useRudderStorage: true,
			storageType:      "S3",
		},
		{
			destType:    "RS",
			config:      map[string]interface{}{},
			storageType: "S3",
		},
		{
			destType:    "S3_DATALAKE",
			config:      map[string]interface{}{},
			storageType: "S3",
		},
		{
			destType:    "BQ",
			config:      map[string]interface{}{},
			storageType: "GCS",
		},
		{
			destType:    "GCS_DATALAKE",
			config:      map[string]interface{}{},
			storageType: "GCS",
		},
		{
			destType:    "AZURE_DATALAKE",
			config:      map[string]interface{}{},
			storageType: "AZURE_BLOB",
		},
		{
			destType:    "SNOWFLAKE",
			config:      map[string]interface{}{},
			storageType: "S3",
		},
		{
			destType: "SNOWFLAKE",
			config: map[string]interface{}{
				"cloudProvider": "AZURE",
			},
			storageType: "AZURE_BLOB",
		},
		{
			destType: "SNOWFLAKE",
			config: map[string]interface{}{
				"cloudProvider": "GCP",
			},
			storageType: "GCS",
		},
		{
			destType: "POSTGRES",
			config: map[string]interface{}{
				"bucketProvider": "GCP",
			},
			storageType: "GCP",
		},
		{
			destType: "POSTGRES",
			config:   map[string]interface{}{},
		},
	}
	for _, input := range inputs {
		provider := ObjectStorageType(input.destType, input.config, input.useRudderStorage)
		require.Equal(t, provider, input.storageType)
	}
}

func TestGetTablePathInObjectStorage(t *testing.T) {
	require.NoError(t, os.Setenv("WAREHOUSE_DATALAKE_FOLDER_NAME", "rudder-test-payload"))
	inputs := []struct {
		namespace string
		tableName string
		expected  string
	}{
		{
			namespace: "rudderstack_setup_test",
			tableName: "setup_test_staging",
			expected:  "rudder-test-payload/rudderstack_setup_test/setup_test_staging",
		},
	}
	for _, input := range inputs {
		got := GetTablePathInObjectStorage(input.namespace, input.tableName)
		require.Equal(t, got, input.expected)
	}
}

func TestGetTempFileExtension(t *testing.T) {
	inputs := []struct {
		destType string
		expected string
	}{
		{
			destType: BQ,
			expected: "json.gz",
		},
		{
			destType: RS,
			expected: "csv.gz",
		},
		{
			destType: SNOWFLAKE,
			expected: "csv.gz",
		},
		{
			destType: POSTGRES,
			expected: "csv.gz",
		},
		{
			destType: CLICKHOUSE,
			expected: "csv.gz",
		},
		{
			destType: MSSQL,
			expected: "csv.gz",
		},
		{
			destType: AZURE_SYNAPSE,
			expected: "csv.gz",
		},
		{
			destType: DELTALAKE,
			expected: "csv.gz",
		},
		{
			destType: S3_DATALAKE,
			expected: "csv.gz",
		},
		{
			destType: GCS_DATALAKE,
			expected: "csv.gz",
		},
		{
			destType: AZURE_DATALAKE,
			expected: "csv.gz",
		},
	}
	for _, input := range inputs {
		got := GetTempFileExtension(input.destType)
		require.Equal(t, got, input.expected)
	}
}

func TestGetLoadFilePrefix(t *testing.T) {
	inputs := []struct {
		warehouse WarehouseT
		expected  string
	}{
		{
			warehouse: WarehouseT{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"tableSuffix": "key=val",
					},
				},
				Type: S3_DATALAKE,
			},
			expected: "2022/08/06/14",
		},
		{
			warehouse: WarehouseT{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"tableSuffix": "key=val",
					},
				},
				Type: AZURE_DATALAKE,
			},
			expected: "2022/08/06/14",
		},
		{
			warehouse: WarehouseT{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"tableSuffix": "key=val",
					},
				},
				Type: GCS_DATALAKE,
			},
			expected: "key=val/2022/08/06/14",
		},
		{
			warehouse: WarehouseT{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"tableSuffix":      "key=val",
						"timeWindowLayout": "year=2006/month=01/day=02/hour=15",
					},
				},
				Type: GCS_DATALAKE,
			},
			expected: "key=val/year=2022/month=08/day=06/hour=14",
		},
	}
	for _, input := range inputs {
		timeWindow := time.Date(2022, time.Month(8), 6, 14, 10, 30, 0, time.UTC)
		got := GetLoadFilePrefix(timeWindow, input.warehouse)
		require.Equal(t, got, input.expected)
	}
}

func TestWarehouseT_GetBoolDestinationConfig(t *testing.T) {
	inputs := []struct {
		warehouse WarehouseT
		expected  bool
	}{
		{
			warehouse: WarehouseT{
				Destination: backendconfig.DestinationT{},
			},
			expected: false,
		},
		{
			warehouse: WarehouseT{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{},
				},
			},
			expected: false,
		},
		{
			warehouse: WarehouseT{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"k1": "true",
					},
				},
			},
			expected: false,
		},
		{
			warehouse: WarehouseT{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"k1": false,
					},
				},
			},
			expected: false,
		},
		{
			warehouse: WarehouseT{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"k1": true,
					},
				},
			},
			expected: true,
		},
	}
	for idx, input := range inputs {
		got := input.warehouse.GetBoolDestinationConfig("k1")
		want := input.expected
		if got != want {
			t.Errorf("got %t expected %t input %d", got, want, idx)
		}
	}
}

func TestGetLoadFileFormat(t *testing.T) {
	inputs := []struct {
		whType   string
		expected string
	}{
		{
			whType:   BQ,
			expected: "json.gz",
		},
		{
			whType:   RS,
			expected: "csv.gz",
		},
		{
			whType:   SNOWFLAKE,
			expected: "csv.gz",
		},
		{
			whType:   POSTGRES,
			expected: "csv.gz",
		},
		{
			whType:   CLICKHOUSE,
			expected: "csv.gz",
		},
		{
			whType:   MSSQL,
			expected: "csv.gz",
		},
		{
			whType:   AZURE_SYNAPSE,
			expected: "csv.gz",
		},
		{
			whType:   DELTALAKE,
			expected: "csv.gz",
		},
		{
			whType:   S3_DATALAKE,
			expected: "parquet",
		},
		{
			whType:   GCS_DATALAKE,
			expected: "parquet",
		},
		{
			whType:   AZURE_DATALAKE,
			expected: "parquet",
		},
	}
	for _, input := range inputs {
		got := GetLoadFileFormat(input.whType)
		require.Equal(t, got, input.expected)
	}
}

func TestGetLoadFileType(t *testing.T) {
	inputs := []struct {
		whType   string
		expected string
	}{
		{
			whType:   BQ,
			expected: "json",
		},
		{
			whType:   RS,
			expected: "csv",
		},
		{
			whType:   SNOWFLAKE,
			expected: "csv",
		},
		{
			whType:   POSTGRES,
			expected: "csv",
		},
		{
			whType:   CLICKHOUSE,
			expected: "csv",
		},
		{
			whType:   MSSQL,
			expected: "csv",
		},
		{
			whType:   AZURE_SYNAPSE,
			expected: "csv",
		},
		{
			whType:   DELTALAKE,
			expected: "csv",
		},
		{
			whType:   S3_DATALAKE,
			expected: "parquet",
		},
		{
			whType:   GCS_DATALAKE,
			expected: "parquet",
		},
		{
			whType:   AZURE_DATALAKE,
			expected: "parquet",
		},
	}
	for _, input := range inputs {
		got := GetLoadFileType(input.whType)
		require.Equal(t, got, input.expected)
	}
}

func TestGetTimeWindow(t *testing.T) {
	inputs := []struct {
		ts       time.Time
		expected time.Time
	}{
		{
			ts:       time.Date(2020, 4, 27, 20, 23, 54, 3424534, time.UTC),
			expected: time.Date(2020, 4, 27, 20, 0, 0, 0, time.UTC),
		},
	}
	for _, input := range inputs {
		got := GetTimeWindow(input.ts)
		require.Equal(t, got, input.expected)
	}
}

func TestGetWarehouseIdentifier(t *testing.T) {
	inputs := []struct {
		destType      string
		sourceID      string
		destinationID string
		expected      string
	}{
		{
			destType:      "RS",
			sourceID:      "sourceID",
			destinationID: "destinationID",
			expected:      "RS:sourceID:destinationID",
		},
	}
	for _, input := range inputs {
		got := GetWarehouseIdentifier(input.destType, input.sourceID, input.destinationID)
		require.Equal(t, got, input.expected)
	}
}

var _ = Describe("Utils", func() {
	DescribeTable("JSON schema to Map", func(rawMsg json.RawMessage, expected map[string]map[string]string) {
		got := JSONSchemaToMap(rawMsg)
		Expect(got).To(Equal(expected))
	},
		Entry(nil, json.RawMessage(`{"k1": { "k2": "v2" }}`), map[string]map[string]string{"k1": {"k2": "v2"}}),
	)

	DescribeTable("Get date range list", func(start, end time.Time, format string, expected []string) {
		got := GetDateRangeList(start, end, format)
		Expect(got).To(Equal(expected))
	},
		Entry("Same day", time.Now(), time.Now(), "2006-01-02", []string{time.Now().Format("2006-01-02")}),
		Entry("Multiple days", time.Now(), time.Now().AddDate(0, 0, 1), "2006-01-02", []string{time.Now().Format("2006-01-02"), time.Now().AddDate(0, 0, 1).Format("2006-01-02")}),
		Entry("No days", nil, nil, "2006-01-02", nil),
	)
})

func TestMain(m *testing.M) {
	config.Load()
	Init()
	os.Exit(m.Run())
}
