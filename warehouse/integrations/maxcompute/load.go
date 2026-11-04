package maxcompute

import (
	"context"
	"errors"
	"net/url"
	"regexp"
	"strings"

	"github.com/rudderlabs/rudder-go-kit/filemanager"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/types"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const maxOSSLoadBytes int64 = 3 * 1024 * 1024 * 1024

var ossBucketPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)

// metadataSize is replaceable for tests. An authenticated OSS HEAD supplies
// the size: load-file metadata from the router is not authoritative.
var metadataSize = ossObjectSize

func (m *MaxCompute) loadTable(ctx context.Context, table, validationLocation string) (*types.LoadTableStats, error) {
	name, err := m.qualified(table)
	if err != nil {
		return nil, err
	}
	var locations []string
	if validationLocation != "" {
		locations = []string{validationLocation}
	} else {
		if m.uploader == nil {
			return nil, errors.New("MaxCompute uploader is not initialized")
		}
		if m.uploader.GetLoadFileType() != warehouseutils.LoadFileTypeJson {
			return nil, errors.New("MaxCompute requires JSON load files")
		}
		files, err := m.uploader.GetLoadFilesMetadata(ctx, warehouseutils.GetLoadFilesOptions{Table: table})
		if err != nil {
			return nil, errors.New("getting MaxCompute load files failed")
		}
		for _, f := range files {
			locations = append(locations, f.Location)
		}
	}
	result := &types.LoadTableStats{}
	if len(locations) == 0 {
		return result, nil
	}
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
	if !ossBucketPattern.MatchString(converted["bucketName"].(string)) {
		return nil, errors.New("invalid MaxCompute OSS source")
	}
	// Validate every object before issuing any LOAD. A bad later file must not
	// leave an earlier file partially committed.
	keys := make([]string, 0, len(locations))
	for _, location := range locations {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := validateLocation(location, m.warehouse.Destination.Config); err != nil {
			return nil, err
		}
		key, err := fm.GetObjectNameFromLocation(location)
		if err != nil || key == "" || strings.ContainsAny(key, "'\n\r\\") {
			return nil, errors.New("invalid MaxCompute OSS object name")
		}
		if strings.Contains(key, "..") {
			return nil, errors.New("invalid MaxCompute OSS source")
		}
		length, err := metadataSize(ctx, m.conf, converted, key)
		if err != nil {
			return nil, err
		}
		if length <= 0 || length > maxOSSLoadBytes {
			return nil, errors.New("MaxCompute OSS object length missing or exceeds 3GB")
		}
		keys = append(keys, key)
	}
	// Use the current catalog, not a possibly stale uploader snapshot. Create
	// the view before the first load so a failed view cannot follow committed data.
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
	bucket := converted["bucketName"].(string)
	for _, key := range keys {
		// No externally supplied URL, SQL identifier or credentials are interpolated.
		rows, err := m.runSQL(ctx, "LOAD DATA INPATH 'oss://"+bucket+"/"+key+"' INTO TABLE "+name, true)
		if err != nil {
			return nil, err
		}
		result.RowsInserted += rows
	}
	return result, nil
}

func validateLocation(location string, settings map[string]any) error {
	u, err := url.Parse(location)
	if err != nil || u.Scheme != "https" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.ForceQuery {
		return errors.New("invalid MaxCompute OSS load file location")
	}
	bucket, _ := settings["bucketName"].(string)
	region, _ := settings["region"].(string)
	if bucket == "" || region == "" || u.Hostname() != bucket+".oss-"+region+".aliyuncs.com" || u.Port() != "" || u.Path == "" || u.Path == "/" || u.EscapedPath() != u.Path || u.Opaque != "" || strings.ContainsAny(u.Path, "'\n\r\\") {
		return errors.New("MaxCompute load file is not in the configured OSS bucket")
	}
	return nil
}
