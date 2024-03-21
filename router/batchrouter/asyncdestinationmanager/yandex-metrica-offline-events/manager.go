package yandex_metrica_offline_events

import backendconfig "github.com/rudderlabs/rudder-server/backend-config"

func NewManager(destination *backendconfig.DestinationT) (*YandexMetricaBulkUploader, error) {
	return &YandexMetricaBulkUploader{}, nil
}
