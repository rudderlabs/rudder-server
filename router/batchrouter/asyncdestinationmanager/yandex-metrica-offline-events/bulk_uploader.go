package yandex_metrica_offline_events

import "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"

func (ym *YandexMetricaBulkUploader) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	return common.AsyncUploadOutput{}
}
func (b *YandexMetricaBulkUploader) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
	return common.PollStatusResponse{}
}
func (b *YandexMetricaBulkUploader) GetUploadStats(UploadStatsInput common.GetUploadStatsInput) common.GetUploadStatsResponse {
	return common.GetUploadStatsResponse{}
}