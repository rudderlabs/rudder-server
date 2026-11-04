package maxcompute

import (
	"context"
	"errors"
	"net/url"
	"regexp"
	"strings"
	"unicode"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/types"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const maxOSSLoadBytes int64 = 3 * 1024 * 1024 * 1024

var (
	ossBucketPattern   = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)
	ossRegionPattern   = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)
	ossReadRolePattern = regexp.MustCompile(`^acs:ram::[0-9]+:role/[A-Za-z0-9.-]{1,64}$`)
)

type loadFolder struct {
	location string
	size     int64
}

func safeLoadLiteral(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if char == '\'' || char == '\\' || unicode.IsControl(char) {
			return false
		}
	}
	return true
}

// preparedLoadFiles validates the complete upload before the first LOAD runs.
// Persisted load-file metadata supplies the object size.
func (m *MaxCompute) preparedLoadFiles(ctx context.Context, files []warehouseutils.LoadFile) ([]loadFolder, error) {
	if err := warehouseutils.ValidateMaxComputeSettings(m.warehouse.Destination.Config, m.warehouse.Destination.WorkspaceID); err != nil {
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
	region, _ := converted["region"].(string)
	if !ossBucketPattern.MatchString(bucket) || !ossRegionPattern.MatchString(region) || !safeLoadLiteral(bucket) {
		return nil, errors.New("invalid MaxCompute OSS source")
	}
	role, _ := m.warehouse.Destination.Config["ossRoleArn"].(string)
	if !ossReadRolePattern.MatchString(role) {
		return nil, errors.New("invalid MaxCompute OSS read role")
	}
	folders := make([]loadFolder, 0, len(files))
	indexes := make(map[string]int, len(files))
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := validateLocation(file.Location, m.warehouse.Destination.Config); err != nil {
			return nil, err
		}
		key, err := fm.GetObjectNameFromLocation(file.Location)
		if err != nil || !safeLoadLiteral(key) {
			return nil, errors.New("invalid MaxCompute OSS object name")
		}
		lastSlash := strings.LastIndexByte(key, '/')
		if lastSlash <= 0 || lastSlash == len(key)-1 {
			return nil, errors.New("MaxCompute OSS load file has no folder")
		}
		folderKey := key[:lastSlash+1]
		if !safeLoadLiteral(folderKey) {
			return nil, errors.New("invalid MaxCompute OSS folder")
		}
		var metadata struct {
			ContentLength int64 `json:"content_length"`
		}
		if err := jsonrs.Unmarshal(file.Metadata, &metadata); err != nil || metadata.ContentLength <= 0 || metadata.ContentLength > maxOSSLoadBytes {
			return nil, errors.New("MaxCompute OSS object length missing, invalid or exceeds 3GB")
		}
		location := "oss://oss-" + region + "-internal.aliyuncs.com/" + bucket + "/" + folderKey
		if index, ok := indexes[location]; ok {
			if metadata.ContentLength > (1<<63-1)-folders[index].size {
				return nil, errors.New("MaxCompute OSS folder size overflow")
			}
			folders[index].size += metadata.ContentLength
		} else {
			indexes[location] = len(folders)
			folders = append(folders, loadFolder{location: location, size: metadata.ContentLength})
		}
	}
	return folders, nil
}

func (m *MaxCompute) loadStatement(table, folder string) (string, error) {
	role, _ := m.warehouse.Destination.Config["ossRoleArn"].(string)
	if !ossReadRolePattern.MatchString(role) || !safeLoadLiteral(folder) || !strings.HasSuffix(folder, "/") {
		return "", errors.New("invalid MaxCompute LOAD location or read role")
	}
	return "LOAD INTO TABLE " + table + " FROM LOCATION '" + folder + "'" +
		" ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' WITH SERDEPROPERTIES (" +
		"'odps.properties.rolearn'='" + role + "') STORED AS TEXTFILE", nil
}

func (m *MaxCompute) loadFolder(ctx context.Context, table string, folder loadFolder) (int64, error) {
	statement, err := m.loadStatement(table, folder.location)
	if err != nil {
		return 0, err
	}
	rows, err := m.runSQL(ctx, statement, true)
	if err == nil && rows == 0 && folder.size > 0 && m.logger != nil {
		m.logger.Warnn("MaxCompute LOAD wrote zero rows for non-empty files")
	}
	return rows, err
}

func (m *MaxCompute) loadTable(ctx context.Context, table string, files []warehouseutils.LoadFile) (*types.LoadTableStats, error) {
	name, err := m.qualified(table)
	if err != nil {
		return nil, err
	}
	result := &types.LoadTableStats{}
	if len(files) == 0 {
		return result, nil
	}
	folders, err := m.preparedLoadFiles(ctx, files)
	if err != nil {
		return nil, err
	}
	if !m.warehouse.GetBoolDestinationConfig(model.SkipViewsSetting) {
		schema, err := m.FetchSchema(ctx)
		if err != nil {
			return nil, err
		}
		columns := schema[table]
		if len(columns) == 0 {
			return nil, errors.New("MaxCompute view schema unavailable in catalog")
		}
		if err := m.createTableView(ctx, table, columns); err != nil {
			return nil, err
		}
	}
	for _, folder := range folders {
		rows, err := m.loadFolder(ctx, name, folder)
		if err != nil {
			return nil, err
		}
		if rows < 0 {
			result.RowsInserted = -1 // unknown: never present a partial count as complete
		} else if result.RowsInserted >= 0 {
			result.RowsInserted += rows
		}
	}
	return result, nil
}

func validateLocation(location string, settings map[string]any) error {
	u, err := url.Parse(location)
	if err != nil || u.Scheme != "https" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.ForceQuery || u.Opaque != "" || u.Port() != "" {
		return errors.New("invalid MaxCompute OSS load file location")
	}
	bucket, _ := settings["bucketName"].(string)
	region, _ := settings["region"].(string)
	if !ossBucketPattern.MatchString(bucket) || !ossRegionPattern.MatchString(region) {
		return errors.New("invalid MaxCompute OSS source")
	}
	host := u.Hostname()
	public := "oss-" + region + ".aliyuncs.com"
	internal := "oss-" + region + "-internal.aliyuncs.com"
	virtual := host == bucket+"."+public || host == bucket+"."+internal
	pathStyle := host == public || host == internal
	if (!virtual && !pathStyle) || u.Path == "" || u.Path == "/" || (pathStyle && !strings.HasPrefix(u.Path, "/"+bucket+"/")) {
		return errors.New("MaxCompute load file is not in the configured OSS bucket")
	}
	return nil
}
