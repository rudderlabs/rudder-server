package testhelper

import (
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type EventsCountMap map[string]int

func defaultStagingFilesEventsMap() EventsCountMap {
	return EventsCountMap{
		"wh_staging_files": 32,
	}
}

func defaultStagingFilesWithIDResolutionEventsMap() EventsCountMap {
	return EventsCountMap{
		"wh_staging_files": 56, // 32 + 24 (merge events because of ID resolution)
	}
}

func defaultTableUploadsEventsMap(destType string) EventsCountMap {
	if destType == whutils.BQ {
		return EventsCountMap{
			"identifies": 4, "users": 4, "tracks": 4, "product_track": 4, "pages": 4, "screens": 4, "aliases": 4, "_groups": 4,
		}
	} else {
		return EventsCountMap{
			"identifies": 4, "users": 4, "tracks": 4, "product_track": 4, "pages": 4, "screens": 4, "aliases": 4, "groups": 4,
		}
	}
}

func defaultWarehouseEventsMap(destType string) EventsCountMap {
	if destType == whutils.BQ {
		return EventsCountMap{
			"identifies": 4, "users": 1, "tracks": 4, "product_track": 4, "pages": 4, "screens": 4, "aliases": 4, "_groups": 4,
		}
	} else {
		return EventsCountMap{
			"identifies": 4, "users": 1, "tracks": 4, "product_track": 4, "pages": 4, "screens": 4, "aliases": 4, "groups": 4,
		}
	}
}

func defaultSourcesStagingFilesEventsMap() EventsCountMap {
	return EventsCountMap{
		"wh_staging_files": 8,
	}
}

func defaultSourcesStagingFilesWithIDResolutionEventsMap() EventsCountMap {
	return EventsCountMap{
		"wh_staging_files": 12, // 8 + 4 (merge events because of ID resolution)
	}
}

func defaultSourcesTableUploadsEventsMap() EventsCountMap {
	return EventsCountMap{
		"tracks": 4, "google_sheet": 4,
	}
}

func defaultSourcesWarehouseEventsMap() EventsCountMap {
	return EventsCountMap{
		"google_sheet": 4, "tracks": 4,
	}
}
