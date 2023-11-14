package warehouseutils_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-go-kit/awsutil"
	"github.com/rudderlabs/rudder-go-kit/logger"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/utils/misc"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	. "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestSanitizeJSON(t *testing.T) {
	testCases := []struct {
		name     string
		input    json.RawMessage
		expected json.RawMessage
	}{
		{
			name:     "empty json",
			input:    json.RawMessage(`{}`),
			expected: json.RawMessage(`{}`),
		},
		{
			name:     "with unicode characters",
			input:    json.RawMessage(`{"exporting_data_failed":{"attempt":1,"errors":["Start: \u0000\u0000\u0000\u0000\u0000\u0000\u0000 : End"]}}`),
			expected: json.RawMessage(`{"exporting_data_failed":{"attempt":1,"errors":["Start:  : End"]}}`),
		},
		{
			name:     "without unicode characters",
			input:    json.RawMessage(`{"exporting_data_failed":{"attempt":1,"errors":["Start:  : End"]}}`),
			expected: json.RawMessage(`{"exporting_data_failed":{"attempt":1,"errors":["Start:  : End"]}}`),
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.expected, SanitizeJSON(tc.input))
		})
	}
}

func TestSanitizeString(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:  "empty string",
			input: "",
		},
		{
			name:     "with unicode characters",
			input:    "Start: \u0000\u0000\u0000\u0000\u0000\u0000\u0000 : End",
			expected: "Start:  : End",
		},
		{
			name:     "without unicode characters",
			input:    "Start:  : End",
			expected: "Start:  : End",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.expected, SanitizeString(tc.input))
		})
	}
}

func TestDestinationConfigKeys(t *testing.T) {
	for _, whType := range WarehouseDestinations {
		t.Run(whType, func(t *testing.T) {
			require.Contains(t, WHDestNameMap, whType)

			whName := WHDestNameMap[whType]
			configKey := fmt.Sprintf("Warehouse.%s.feature", whName)
			got := config.ConfigKeyToEnv(config.DefaultEnvPrefix, configKey)
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
	inputs := []LoadFile{
		{Location: "https://test-bucket.s3.amazonaws.com/test-object.csv"},
		{Location: "https://test-bucket.s3.eu-west-1.amazonaws.com/test-object.csv"},
		{Location: "https://my.test-bucket.s3.amazonaws.com/test-object.csv"},
		{Location: "https://my.test-bucket.s3.us-west-1.amazonaws.com/test-object.csv"},
	}
	outputs := []LoadFile{
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
		gcsLocation := GetGCSLocation(input.location, GCSLocationOptions{
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
		gcsLocationFolder := GetGCSLocationFolder(input.location, GCSLocationOptions{})
		require.Equal(t, gcsLocationFolder, input.gcsLocationFolder)
	}
}

func TestGetGCSLocations(t *testing.T) {
	inputs := []LoadFile{
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

	gcsLocations := GetGCSLocations(inputs, GCSLocationOptions{})
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
	config.Set("Warehouse.bigquery.skipNamespaceSnakeCasing", true)
	defer config.Reset()

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
		{
			provider:      "RS",
			namespace:     "k3_namespace",
			safeNamespace: "k_3_namespace",
		},
		{
			provider:      "BQ",
			namespace:     "k3_namespace",
			safeNamespace: "k3_namespace",
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
	columnMap := model.TableSchema{"k5": "V5", "k4": "V4", "k3": "V3", "k2": "V2", "k1": "V1"}
	want := []string{"k1", "k2", "k3", "k4", "k5"}
	got := SortColumnKeysFromColumnMap(columnMap)
	require.Equal(t, got, want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v want %#v input %#v", got, want, columnMap)
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
		warehouse model.Warehouse
	}{
		{
			key:   "k1",
			value: "v1",
			warehouse: model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"k1": "v1",
					},
				},
			},
		},
		{
			key: "u1",
			warehouse: model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{},
				},
			},
		},
		{
			key:   "u1",
			value: "v1",
			warehouse: model.Warehouse{
				Source: backendconfig.SourceT{
					ID: "source_id",
				},
				Destination: backendconfig.DestinationT{
					ID:     "destination_id",
					Config: map[string]interface{}{},
				},
			},
		},
	}
	config.Set("Warehouse.pipeline.source_id.destination_id.u1", "v1")
	for _, input := range inputs {
		value := GetConfigValue(input.key, input.warehouse)
		require.Equal(t, value, input.value)
	}
}

func TestGetConfigValueBoolString(t *testing.T) {
	inputs := []struct {
		key       string
		value     string
		warehouse model.Warehouse
	}{
		{
			key:   "k1",
			value: "true",
			warehouse: model.Warehouse{
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
			warehouse: model.Warehouse{
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
			warehouse: model.Warehouse{
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
			destType: AzureSynapse,
			expected: "csv.gz",
		},
		{
			destType: DELTALAKE,
			expected: "csv.gz",
		},
		{
			destType: S3Datalake,
			expected: "csv.gz",
		},
		{
			destType: GCSDatalake,
			expected: "csv.gz",
		},
		{
			destType: AzureDatalake,
			expected: "csv.gz",
		},
	}
	for _, input := range inputs {
		got := GetTempFileExtension(input.destType)
		require.Equal(t, got, input.expected)
	}
}

func TestWarehouseT_GetBoolDestinationConfig(t *testing.T) {
	inputs := []struct {
		warehouse model.Warehouse
		expected  bool
	}{
		{
			warehouse: model.Warehouse{
				Destination: backendconfig.DestinationT{},
			},
			expected: false,
		},
		{
			warehouse: model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{},
				},
			},
			expected: false,
		},
		{
			warehouse: model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"useRudderStorage": "true",
					},
				},
			},
			expected: false,
		},
		{
			warehouse: model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"useRudderStorage": false,
					},
				},
			},
			expected: false,
		},
		{
			warehouse: model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"useRudderStorage": true,
					},
				},
			},
			expected: true,
		},
	}
	for idx, input := range inputs {
		got := input.warehouse.GetBoolDestinationConfig("useRudderStorage")
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
			whType:   AzureSynapse,
			expected: "csv.gz",
		},
		{
			whType:   DELTALAKE,
			expected: "csv.gz",
		},
		{
			whType:   S3Datalake,
			expected: "parquet",
		},
		{
			whType:   GCSDatalake,
			expected: "parquet",
		},
		{
			whType:   AzureDatalake,
			expected: "parquet",
		},
	}
	for _, input := range inputs {
		got := GetLoadFileFormat(GetLoadFileType(input.whType))
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
			whType:   AzureSynapse,
			expected: "csv",
		},
		{
			whType:   DELTALAKE,
			expected: "csv",
		},
		{
			whType:   S3Datalake,
			expected: "parquet",
		},
		{
			whType:   GCSDatalake,
			expected: "parquet",
		},
		{
			whType:   AzureDatalake,
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

func TestCreateAWSSessionConfig(t *testing.T) {
	rudderAccessKeyID := "rudderAccessKeyID"
	rudderAccessKey := "rudderAccessKey"
	t.Setenv("RUDDER_AWS_S3_COPY_USER_ACCESS_KEY_ID", rudderAccessKeyID)
	t.Setenv("RUDDER_AWS_S3_COPY_USER_ACCESS_KEY", rudderAccessKey)

	someAccessKeyID := "someAccessKeyID"
	someAccessKey := "someAccessKey"
	someIAMRoleARN := "someIAMRoleARN"
	someWorkspaceID := "someWorkspaceID"

	inputs := []struct {
		destination    *backendconfig.DestinationT
		service        string
		expectedConfig *awsutil.SessionConfig
	}{
		{
			destination: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"useRudderStorage": true,
				},
			},
			service: "s3",
			expectedConfig: &awsutil.SessionConfig{
				AccessKeyID: rudderAccessKeyID,
				AccessKey:   rudderAccessKey,
				Service:     "s3",
			},
		},
		{
			destination: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"accessKeyID": someAccessKeyID,
					"accessKey":   someAccessKey,
				},
			},
			service: "glue",
			expectedConfig: &awsutil.SessionConfig{
				AccessKeyID: someAccessKeyID,
				AccessKey:   someAccessKey,
				Service:     "glue",
			},
		},
		{
			destination: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"iamRoleARN": someIAMRoleARN,
				},
				WorkspaceID: someWorkspaceID,
			},
			service: "redshift",
			expectedConfig: &awsutil.SessionConfig{
				RoleBasedAuth: true,
				IAMRoleARN:    someIAMRoleARN,
				ExternalID:    someWorkspaceID,
				Service:       "redshift",
			},
		},
		{
			destination: &backendconfig.DestinationT{
				Config:      map[string]interface{}{},
				WorkspaceID: someWorkspaceID,
			},
			service: "redshift",
			expectedConfig: &awsutil.SessionConfig{
				AccessKeyID: rudderAccessKeyID,
				AccessKey:   rudderAccessKey,
				Service:     "redshift",
			},
		},
	}
	for _, input := range inputs {
		config, err := CreateAWSSessionConfig(input.destination, input.service)
		require.Nil(t, err)
		require.Equal(t, config, input.expectedConfig)
	}
}

var _ = Describe("Utils", func() {
	DescribeTable("Get columns from table schema", func(schema model.TableSchema, expected []string) {
		columns := GetColumnsFromTableSchema(schema)
		sort.Strings(columns)
		sort.Strings(expected)
		Expect(columns).To(Equal(expected))
	},
		Entry(nil, model.TableSchema{"k1": "v1", "k2": "v2"}, []string{"k1", "k2"}),
		Entry(nil, model.TableSchema{"k2": "v1", "k1": "v2"}, []string{"k2", "k1"}),
	)

	DescribeTable("JSON schema to Map", func(rawMsg json.RawMessage, expected model.Schema) {
		Expect(JSONSchemaToMap(rawMsg)).To(Equal(expected))
	},
		Entry(nil, json.RawMessage(`{"k1": { "k2": "v2" }}`), model.Schema{"k1": {"k2": "v2"}}),
	)

	DescribeTable("Get date range list", func(start, end time.Time, format string, expected []string) {
		Expect(GetDateRangeList(start, end, format)).To(Equal(expected))
	},
		Entry("Same day", time.Now(), time.Now(), "2006-01-02", []string{time.Now().Format("2006-01-02")}),
		Entry("Multiple days", time.Now(), time.Now().AddDate(0, 0, 1), "2006-01-02", []string{time.Now().Format("2006-01-02"), time.Now().AddDate(0, 0, 1).Format("2006-01-02")}),
		Entry("No days", nil, nil, "2006-01-02", nil),
	)

	DescribeTable("Staging table prefix", func(provider string) {
		Expect(StagingTablePrefix(provider)).To(Equal(ToProviderCase(provider, "rudder_staging_")))
	},
		Entry(nil, BQ),
		Entry(nil, RS),
		Entry(nil, SNOWFLAKE),
		Entry(nil, POSTGRES),
		Entry(nil, CLICKHOUSE),
		Entry(nil, MSSQL),
		Entry(nil, AzureSynapse),
		Entry(nil, DELTALAKE),
		Entry(nil, S3Datalake),
		Entry(nil, GCSDatalake),
		Entry(nil, AzureDatalake),
	)

	DescribeTable("Staging table name", func(provider string, limit int) {
		By("Within limits")
		tableName := ToProviderCase(provider, "demo")
		expectedTableName := ToProviderCase(provider, "rudder_staging_demo_")
		Expect(StagingTableName(provider, tableName, limit)).To(HavePrefix(expectedTableName))

		By("Beyond limits")
		tableName = ToProviderCase(provider, "abcdefghijlkmnopqrstuvwxyzabcdefghijlkmnopqrstuvwxyzabcdefghijlkmnopqrstuvwxyzabcdefghijlkmnopqrstuvwxyzabcdefghijlkmnopqrstuvwxyz")
		expectedTableName = ToProviderCase(provider, "rudder_staging_abcdefghijlkmnopqrstuvwxyzabcdefghijlkmnopqrstuvwxyzabcdefghijlkmnopqrstuvwxyzabcdefghijlkmnopqrstuvwxyzabcdefghijlkmnopqrstuvwxyz"[:limit])
		Expect(StagingTableName(provider, tableName, limit)).To(HavePrefix(expectedTableName))
	},
		Entry(nil, BQ, 127),
		Entry(nil, RS, 127),
		Entry(nil, SNOWFLAKE, 127),
		Entry(nil, POSTGRES, 63),
		Entry(nil, CLICKHOUSE, 127),
		Entry(nil, MSSQL, 127),
		Entry(nil, AzureSynapse, 127),
		Entry(nil, DELTALAKE, 127),
		Entry(nil, S3Datalake, 127),
		Entry(nil, GCSDatalake, 127),
		Entry(nil, AzureDatalake, 127),
	)

	DescribeTable("Identity mapping unique mapping constraints name", func(warehouse model.Warehouse, expected string) {
		Expect(IdentityMappingsUniqueMappingConstraintName(warehouse)).To(Equal(expected))
	},
		Entry(nil, model.Warehouse{Namespace: "namespace", Destination: backendconfig.DestinationT{ID: "id"}}, "unique_merge_property_namespace_id"),
	)

	DescribeTable("Identity mapping table name", func(warehouse model.Warehouse, expected string) {
		Expect(IdentityMappingsTableName(warehouse)).To(Equal(expected))
	},
		Entry(nil, model.Warehouse{Namespace: "namespace", Destination: backendconfig.DestinationT{ID: "id"}}, "rudder_identity_mappings_namespace_id"),
	)

	DescribeTable("Identity merge rules table name", func(warehouse model.Warehouse, expected string) {
		Expect(IdentityMergeRulesTableName(warehouse)).To(Equal(expected))
	},
		Entry(nil, model.Warehouse{Namespace: "namespace", Destination: backendconfig.DestinationT{ID: "id"}}, "rudder_identity_merge_rules_namespace_id"),
	)

	DescribeTable("Identity merge rules warehouse table name", func(provider string) {
		Expect(IdentityMergeRulesWarehouseTableName(provider)).To(Equal(ToProviderCase(provider, IdentityMergeRulesTable)))
	},
		Entry(nil, BQ),
		Entry(nil, RS),
		Entry(nil, SNOWFLAKE),
		Entry(nil, POSTGRES),
		Entry(nil, CLICKHOUSE),
		Entry(nil, MSSQL),
		Entry(nil, AzureSynapse),
		Entry(nil, DELTALAKE),
		Entry(nil, S3Datalake),
		Entry(nil, GCSDatalake),
		Entry(nil, AzureDatalake),
	)

	DescribeTable("Identity mappings warehouse table name", func(provider string) {
		Expect(IdentityMappingsWarehouseTableName(provider)).To(Equal(ToProviderCase(provider, IdentityMappingsTable)))
	},
		Entry(nil, BQ),
		Entry(nil, RS),
		Entry(nil, SNOWFLAKE),
		Entry(nil, POSTGRES),
		Entry(nil, CLICKHOUSE),
		Entry(nil, MSSQL),
		Entry(nil, AzureSynapse),
		Entry(nil, DELTALAKE),
		Entry(nil, S3Datalake),
		Entry(nil, GCSDatalake),
		Entry(nil, AzureDatalake),
	)

	DescribeTable("Get object name", func(location string, config interface{}, objectProvider, objectName string) {
		Expect(GetObjectName(location, config, objectProvider)).To(Equal(objectName))
	},
		Entry(GCS, "https://storage.googleapis.com/bucket-name/key", map[string]interface{}{"bucketName": "bucket-name"}, GCS, "key"),
		Entry(S3, "https://bucket-name.s3.amazonaws.com/key", map[string]interface{}{"bucketName": "bucket-name"}, S3, "key"),
		Entry(AzureBlob, "https://account-name.blob.core.windows.net/container-name/key", map[string]interface{}{"containerName": "container-name"}, AzureBlob, "key"),
		Entry(MINIO, "https://minio-endpoint/bucket-name/key", map[string]interface{}{"bucketName": "bucket-name", "useSSL": true, "endPoint": "minio-endpoint"}, MINIO, "key"),
	)

	It("SSL keys", func() {
		destinationID := "destID"
		clientKey, clientCert, serverCA := misc.FastUUID().String(), misc.FastUUID().String(), misc.FastUUID().String()

		err := WriteSSLKeys(backendconfig.DestinationT{ID: destinationID, Config: map[string]interface{}{"clientKey": clientKey, "clientCert": clientCert, "serverCA": serverCA}})
		Expect(err).To(Equal(WriteSSLKeyError{}))

		path := GetSSLKeyDirPath(destinationID)
		Expect(path).NotTo(BeEmpty())

		Expect(os.RemoveAll(path)).NotTo(HaveOccurred())
	})
})

func TestMain(m *testing.M) {
	config.Reset()
	logger.Reset()
	misc.Init()
	Init()
	os.Exit(m.Run())
}
