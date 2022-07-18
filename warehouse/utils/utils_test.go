package warehouseutils_test

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/misc"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	. "github.com/rudderlabs/rudder-server/warehouse/utils"
)

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
	}

	for idx, input := range inputs {
		s3Location, region := GetS3Location(input.location)
		if s3Location != input.s3Location {
			t.Errorf("got %q want %q input %d", s3Location, input.s3Location, idx)
		}
		if region != input.region {
			t.Errorf("got %q want %q input %d", region, input.region, idx)
		}
	}
}

func TestCaptureRegexGroup(t *testing.T) {
	inputs := []struct {
		regex   string
		pattern string
		groups  map[string]string
	}{
		{
			regex:   "https?://s3([.-](?P<region>[^.]+))?.amazonaws.com/(?P<bucket>[^/]+)/(?P<keyname>.*)",
			pattern: "https://s3.amazonaws.com/bucket.with.a.dot/keyname",
			groups: map[string]string{
				"bucket":  "bucket.with.a.dot",
				"keyname": "keyname",
				"region":  "",
			},
		},
		{
			regex:   "https?://s3([.-](?P<region>[^.]+))?.amazonaws.com/(?P<bucket>[^/]+)/(?P<keyname>.*)",
			pattern: "https://s3.us-east.amazonaws.com/bucket.with.a.dot/keyname",
			groups: map[string]string{
				"bucket":  "bucket.with.a.dot",
				"keyname": "keyname",
				"region":  "us-east",
			},
		},
		{
			regex:   "https?://(?P<bucket>[^/]+).s3([.-](?P<region>[^.]+))?.amazonaws.com/(?P<keyname>.*)",
			pattern: "https://bucket.with.a.dot.s3.amazonaws.com/keyname",
			groups: map[string]string{
				"bucket":  "bucket.with.a.dot",
				"keyname": "keyname",
				"region":  "",
			},
		},
		{
			regex:   "https?://(?P<bucket>[^/]+).s3([.-](?P<region>[^.]+))?.amazonaws.com/(?P<keyname>.*)",
			pattern: "https://bucket.with.a.dot.s3.amazonaws.com/keyname",
			groups: map[string]string{
				"bucket":  "bucket.with.a.dot",
				"keyname": "keyname",
				"region":  "",
			},
		},
	}
	for idx, input := range inputs {
		got, err := CaptureRegexGroup(input.regex, input.pattern)
		assertNoError(t, err)
		if !reflect.DeepEqual(got, input.groups) {
			t.Errorf("got %#v want %#v input %d", got, input.groups, idx)
		}
	}
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
	for idx, input := range inputs {
		s3LocationFolder := GetS3LocationFolder(input.s3Location)
		if s3LocationFolder != input.s3LocationFolder {
			t.Errorf("got %q want %q input %d", s3LocationFolder, input.s3LocationFolder, idx)
		}
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
	if !reflect.DeepEqual(inputs, outputs) {
		t.Errorf("got %#v want %#v", s3Locations, outputs)
	}
}

func TestGetGCSLocation(t *testing.T) {
	inputs := []struct {
		location    string
		gcsLocation string
	}{
		{
			location:    "https://storage.googleapis.com/test-bucket/test-object.csv",
			gcsLocation: "gs://test-bucket/test-object.csv",
		},
		{
			location:    "https://storage.googleapis.com/my.test-bucket/test-object.csv",
			gcsLocation: "gs://my.test-bucket/test-object.csv",
		},
	}
	for idx, input := range inputs {
		gcsLocation := GetGCSLocation(input.location, GCSLocationOptionsT{})
		if gcsLocation != input.gcsLocation {
			t.Errorf("got %q want %q input %d", gcsLocation, input.gcsLocation, idx)
		}
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
	for idx, input := range inputs {
		gcsLocationFolder := GetGCSLocationFolder(input.location, GCSLocationOptionsT{})
		if gcsLocationFolder != input.gcsLocationFolder {
			t.Errorf("got %q want %q input %d", gcsLocationFolder, input.gcsLocationFolder, idx)
		}
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
	if !reflect.DeepEqual(gcsLocations, outputs) {
		t.Errorf("got %#v want %#v", gcsLocations, outputs)
	}
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
	for idx, input := range inputs {
		azBlobLocation := GetAzureBlobLocation(input.location)
		if azBlobLocation != input.azBlobLocation {
			t.Errorf("got %q want %q input %d", azBlobLocation, input.azBlobLocation, idx)
		}
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
	for idx, input := range inputs {
		azBlobLocationFolder := GetAzureBlobLocationFolder(input.location)
		if azBlobLocationFolder != input.azBlobLocationFolder {
			t.Errorf("got %q want %q input %d", azBlobLocationFolder, input.azBlobLocationFolder, idx)
		}
	}
}

func TestToSafeNamespace(t *testing.T) {
	inputs := []struct {
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
	}
	for idx, input := range inputs {
		safeNamespace := ToSafeNamespace("", input.namespace)
		if safeNamespace != input.safeNamespace {
			t.Errorf("got %q want %q input %d", safeNamespace, input.safeNamespace, idx)
		}
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

	for idx, input := range inputs {
		t.Run(input.provider, func(t *testing.T) {
			objectLocation := GetObjectLocation(input.provider, input.location)
			if objectLocation != input.objectLocation {
				t.Errorf("got %q want %q input %d", objectLocation, input.objectLocation, idx)
			}
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

	for idx, input := range inputs {
		t.Run(input.provider, func(t *testing.T) {
			objectLocation := GetObjectFolder(input.provider, input.location)
			if objectLocation != input.objectFolder {
				t.Errorf("got %q want %q input %d", objectLocation, input.objectFolder, idx)
			}
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

	for idx, input := range inputs {
		t.Run(input.provider, func(t *testing.T) {
			objectLocation := GetObjectFolderForDeltalake(input.provider, input.location)
			if objectLocation != input.objectFolder {
				t.Errorf("got %q want %q input %d", objectLocation, input.objectFolder, idx)
			}
		})
	}
}

func TestDoubleQuoteAndJoinByComma(t *testing.T) {
	names := []string{"Samantha Edwards", "Samantha Smith", "Holly Miller", "Tammie Tyler", "Gina Richards"}
	want := "\"Samantha Edwards\",\"Samantha Smith\",\"Holly Miller\",\"Tammie Tyler\",\"Gina Richards\""
	got := DoubleQuoteAndJoinByComma(names)
	if got != want {
		t.Errorf("got %q want %q input %#v", got, want, names)
	}
}

func TestSortColumnKeysFromColumnMap(t *testing.T) {
	columnMap := map[string]string{"k5": "V5", "k4": "V4", "k3": "V3", "k2": "V2", "k1": "V1"}
	want := []string{"k1", "k2", "k3", "k4", "k5"}
	got := SortColumnKeysFromColumnMap(columnMap)
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
		if err != nil {
			t.Errorf("error occurred while extracting generating load files time: %v", err)
		}

		loadFileGenTime := GetLoadFileGenTime(input.timingsMap)
		assertTime(t, loadFilesEpochTime, loadFileGenTime)
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
	for idx, input := range inputs {
		status := GetLastFailedStatus(input.timingsMap)
		if status != input.status {
			t.Errorf("got %q want %q input %d", status, input.status, idx)
		}
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
				String: "{\"generating_upload_schema\":\"2022-07-04T16:09:03.001Z\"}",
			},
			status:            "generating_upload_schema",
			loadFilesEpochStr: "2022-07-04T16:09:04.169Z",
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
		if err != nil {
			t.Errorf("error occurred while extracting generating load files time: %v", err)
		}

		status, recordedTime := TimingFromJSONString(input.timingsMap)
		assertString(t, status, input.status)
		assertTime(t, loadFilesEpochTime, recordedTime)
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
	for idx, input := range inputs {
		value := GetConfigValue(input.key, input.warehouse)
		if value != input.value {
			t.Errorf("got %q want %q input %d", value, input.value, idx)
		}
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
	for idx, input := range inputs {
		value := GetConfigValueBoolString(input.key, input.warehouse)
		if value != input.value {
			t.Errorf("got %q want %q input %d", value, input.value, idx)
		}
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
		assertString(t, value, input.value)
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
			assertString(t, value, input.value)
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
		assertString(t, provider, input.provider)
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
		assertString(t, provider, input.storageType)
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
		assertString(t, got, input.expected)
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
		assertString(t, got, input.expected)
	}
}

func assertTime(t *testing.T, got, want time.Time) {
	t.Helper()
	if got.Before(want) {
		t.Errorf("got %v want %v", got, want)
	}
}

func assertString(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("got %v want %v", got, want)
	}
}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("got error %s when not expected", err.Error())
	}
}

var _ = Describe("Utils", func() {
	Describe("Time window warehouse destinations", func() {
		It("should give time window format based on warehouse destination type", func() {
			warehouse := WarehouseT{
				Destination: backendconfig.DestinationT{
					Config: make(map[string]interface{}),
				},
			}
			warehouse.Destination.Config["tableSuffix"] = "key=val"
			timeWindow := time.Date(2022, time.Month(8), 6, 14, 10, 30, 0, time.UTC)

			warehouse.Type = S3_DATALAKE
			Expect(GetLoadFilePrefix(timeWindow, warehouse)).To(Equal("2022/08/06/14"))

			warehouse.Type = AZURE_DATALAKE
			Expect(GetLoadFilePrefix(timeWindow, warehouse)).To(Equal("2022/08/06/14"))

			warehouse.Type = GCS_DATALAKE
			Expect(GetLoadFilePrefix(timeWindow, warehouse)).To(Equal("key=val/2022/08/06/14"))
			warehouse.Destination.Config["timeWindowLayout"] = "year=2006/month=01/day=02/hour=15"
			Expect(GetLoadFilePrefix(timeWindow, warehouse)).To(Equal("key=val/year=2022/month=08/day=06/hour=14"))
		})
	})
})

func TestMain(m *testing.M) {
	config.Load()
	Init()
	os.Exit(m.Run())
}
