package main

import (
	"fmt"
	"regexp"
	"strconv"
)

var backupFileNamePattern = regexp.MustCompile(`^(/)?((?P<prefix>[^/]+)/)?(?P<instance>[^/]+)/gw_jobs_(?P<tableIDx>[0-9]+).(?P<minJobID>[0-9]+).(?P<maxJobID>[0-9]+).(?P<minJobCreatedAt>[0-9]+).(?P<maxJobCreatedAt>[0-9]+).(?P<workspaceID>[^.]+).gz$`)

type backupFileInfo struct {
	filePath        string
	prefix          string
	instance        string
	tableIDx        int64
	minJobID        int64
	maxJobID        int64
	minJobCreatedAt int64
	maxJobCreatedAt int64
	workspaceID     string
}

func newBackupFileInfo(backupFilePath string) (*backupFileInfo, error) {
	matches := backupFileNamePattern.FindStringSubmatch(backupFilePath)
	if matches == nil {
		return nil, fmt.Errorf("unable to parse object name %s", backupFilePath)
	}

	info := &backupFileInfo{
		filePath:    backupFilePath,
		prefix:      matches[backupFileNamePattern.SubexpIndex("prefix")],
		instance:    matches[backupFileNamePattern.SubexpIndex("instance")],
		workspaceID: matches[backupFileNamePattern.SubexpIndex("workspaceID")],
	}
	var err error
	info.tableIDx, err = strconv.ParseInt(matches[backupFileNamePattern.SubexpIndex("tableIDx")], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid table name in filename %w", err)
	}
	info.minJobID, _ = strconv.ParseInt(matches[backupFileNamePattern.SubexpIndex("minJobID")], 10, 64)
	info.maxJobID, _ = strconv.ParseInt(matches[backupFileNamePattern.SubexpIndex("maxJobID")], 10, 64)
	info.minJobCreatedAt, _ = strconv.ParseInt(matches[backupFileNamePattern.SubexpIndex("minJobCreatedAt")], 10, 64)
	info.maxJobCreatedAt, _ = strconv.ParseInt(matches[backupFileNamePattern.SubexpIndex("maxJobCreatedAt")], 10, 64)

	return info, nil
}
