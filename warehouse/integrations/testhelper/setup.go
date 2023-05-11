package testhelper

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	"github.com/rudderlabs/rudder-server/utils/timeutil"

	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	_ "github.com/lib/pq"
	warehouseclient "github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/stretchr/testify/require"
)

const (
	WaitFor2Minute         = 2 * time.Minute
	WaitFor10Minute        = 10 * time.Minute
	DefaultQueryFrequency  = 100 * time.Millisecond
	AsyncJOBQueryFrequency = 1000 * time.Millisecond
)

type EventsCountMap map[string]int

type TestConfig struct {
	WriteKey                     string
	Schema                       string
	UserID                       string
	MessageID                    string
	WorkspaceID                  string
	JobRunID                     string
	TaskRunID                    string
	RecordID                     string
	SourceID                     string
	DestinationID                string
	DestinationType              string
	Tables                       []string
	Client                       *warehouseclient.Client
	TimestampBeforeSendingEvents time.Time
	Config                       map[string]interface{}
	EventTemplateCountMap        EventsCountMap
	StagingFilesEventsMap        EventsCountMap
	LoadFilesEventsMap           EventsCountMap
	TableUploadsEventsMap        EventsCountMap
	WarehouseEventsMap           EventsCountMap
	JobsDB                       *sql.DB
	AsyncJob                     bool
	SkipWarehouse                bool
	HTTPPort                     int
}

func (w *TestConfig) VerifyEvents(t testing.TB) {
	t.Helper()

	w.reset()

	createStagingFile(t, w)

	verifyEventsInStagingFiles(t, w)
	verifyEventsInLoadFiles(t, w)
	verifyEventsInTableUploads(t, w)

	if w.AsyncJob {
		verifyAsyncJob(t, w)
	}
	if !w.SkipWarehouse {
		verifyEventsInWareHouse(t, w)
	}
}

func (w *TestConfig) reset() {
	w.TimestampBeforeSendingEvents = timeutil.Now()

	if len(w.StagingFilesEventsMap) == 0 {
		w.StagingFilesEventsMap = defaultStagingFilesEventsMap()
	}
	if len(w.LoadFilesEventsMap) == 0 {
		w.LoadFilesEventsMap = defaultLoadFilesEventsMap()
	}
	if len(w.TableUploadsEventsMap) == 0 {
		w.TableUploadsEventsMap = defaultTableUploadsEventsMap()
	}
	if len(w.WarehouseEventsMap) == 0 {
		w.WarehouseEventsMap = defaultWarehouseEventsMap()
	}
}

func (w *TestConfig) msgID() string {
	if w.MessageID == "" {
		return misc.FastUUID().String()
	}
	return w.MessageID
}

func (w *TestConfig) recordID() string {
	if w.RecordID == "" {
		return misc.FastUUID().String()
	}
	return w.RecordID
}

func defaultStagingFilesEventsMap() EventsCountMap {
	return EventsCountMap{
		"wh_staging_files": 32,
	}
}

func defaultLoadFilesEventsMap() EventsCountMap {
	return EventsCountMap{
		"identifies":    4,
		"users":         4,
		"tracks":        4,
		"product_track": 4,
		"pages":         4,
		"screens":       4,
		"aliases":       4,
		"groups":        4,
	}
}

func defaultTableUploadsEventsMap() EventsCountMap {
	return EventsCountMap{
		"identifies":    4,
		"users":         4,
		"tracks":        4,
		"product_track": 4,
		"pages":         4,
		"screens":       4,
		"aliases":       4,
		"groups":        4,
	}
}

func defaultWarehouseEventsMap() EventsCountMap {
	return EventsCountMap{
		"identifies":    4,
		"users":         1,
		"tracks":        4,
		"product_track": 4,
		"pages":         4,
		"screens":       4,
		"aliases":       4,
		"groups":        4,
	}
}

func SourcesStagingFilesEventsMap() EventsCountMap {
	return EventsCountMap{
		"wh_staging_files": 8,
	}
}

func SourcesLoadFilesEventsMap() EventsCountMap {
	return EventsCountMap{
		"tracks":       4,
		"google_sheet": 4,
	}
}

func SourcesTableUploadsEventsMap() EventsCountMap {
	return EventsCountMap{
		"tracks":       4,
		"google_sheet": 4,
	}
}

func SourcesWarehouseEventsMap() EventsCountMap {
	return EventsCountMap{
		"google_sheet": 4,
		"tracks":       4,
	}
}

func GetUserId(provider string) string {
	return fmt.Sprintf("userId_%s_%s", strings.ToLower(provider), warehouseutils.RandHex())
}

func RandSchema(provider string) string {
	hex := strings.ToLower(rand.String(12))
	namespace := fmt.Sprintf("test_%s_%d", hex, time.Now().Unix())
	return warehouseutils.ToProviderCase(provider, warehouseutils.ToSafeNamespace(provider,
		namespace,
	))
}

func JobsDB(t testing.TB, port int) *sql.DB {
	t.Helper()

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		"rudder",
		"password",
		"localhost",
		strconv.Itoa(port),
		"jobsdb",
	)
	jobsDB, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	require.NoError(t, jobsDB.Ping())

	return jobsDB
}

func WithConstantRetries(operation func() error) error {
	var err error
	for i := 0; i < 6; i++ {
		if err = operation(); err == nil {
			return nil
		}
		time.Sleep(time.Duration(1+i) * time.Second)
	}
	return err
}
