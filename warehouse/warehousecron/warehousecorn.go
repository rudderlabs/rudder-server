package warehousecron

import (
	"github.com/robfig/cron/v3"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"strings"
)

type warehouseUploadCronJob struct {
	warehouseUploadCron *cron.Cron
	jobEntryID          cron.EntryID
}

var (
	warehouseChan             chan bool
	cronParser                cron.Parser
	warehouseUploadCronJobMap map[string]warehouseUploadCronJob
	WarehouseUploadTrigerChan chan warehouseutils.WarehouseT
)

func init() {
	cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	warehouseUploadCronJobMap = make(map[string]warehouseUploadCronJob)
	WarehouseUploadTrigerChan = make(chan warehouseutils.WarehouseT)
}

func IsCronExpressionPresent(cronExpression string, warehouseIdentifier string) bool {
	stopExistingCronFromRunningInFuture(warehouseIdentifier)
	if len(strings.TrimSpace(cronExpression)) > 0 {
		return true
	}
	return false
}

func stopExistingCronFromRunningInFuture(warehouseIdentifier string) {
	if uploadCronJob, ok := warehouseUploadCronJobMap[warehouseIdentifier]; ok {
		uploadCronJob.warehouseUploadCron.Remove(uploadCronJob.jobEntryID)
	}
}

func CreateCron(cronExpression string, warehouseIdentifier string, warehouse warehouseutils.WarehouseT) error {
	c := cron.New(cron.WithParser(cronParser))
	entryId, err := c.AddFunc(cronExpression, func() {
		WarehouseUploadTrigerChan <- warehouse
	})
	c.Start()
	if err != nil {
		return err
	}

	warehouseUploadCronJobMap[warehouseIdentifier] = warehouseUploadCronJob{
		warehouseUploadCron: c,
		jobEntryID:          entryId,
	}
	return nil
}
