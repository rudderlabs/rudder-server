package warehousecron

import (
	"github.com/robfig/cron/v3"
	"strings"
)

type warehouseUploadCronJob struct {
	warehouseUploadCron *cron.Cron
	jobEntryID          cron.EntryID
	cronExpression      string
}

var (
	cronParser                cron.Parser
	warehouseUploadCronJobMap map[string]warehouseUploadCronJob
)

func init() {
	cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	warehouseUploadCronJobMap = make(map[string]warehouseUploadCronJob)
}

func IsCronExpressionPresent(cronExpression string) bool {
	if len(strings.TrimSpace(cronExpression)) > 0 {
		return true
	}
	return false
}

func StopExistingCronFromRunningInFuture(warehouseIdentifier string) {
	if uploadCronJob, ok := warehouseUploadCronJobMap[warehouseIdentifier]; ok {
		// stop job from running in future. it wont stop running job
		uploadCronJob.warehouseUploadCron.Remove(uploadCronJob.jobEntryID)
	}
}

func CreateCron(cronExpression string, warehouseIdentifier string, cronFunc func()) error {
	c := cron.New(cron.WithParser(cronParser))
	entryId, err := c.AddFunc(cronExpression, cronFunc)
	if err != nil {
		return err
	}
	c.Start()
	warehouseUploadCronJobMap[warehouseIdentifier] = warehouseUploadCronJob{
		warehouseUploadCron: c,
		jobEntryID:          entryId,
	}
	return nil
}
