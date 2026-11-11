package eloqua_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mockeloquaservice "github.com/rudderlabs/rudder-server/mocks/router/eloqua"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/eloqua"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	once        sync.Once
	destination = backendconfig.DestinationT{
		Name: "eloqua",
		Config: map[string]any{
			"companyName":     "company_name",
			"password":        "password",
			"userName":        "user_name",
			"rudderAccountId": "rudder_account_id",
		},
		WorkspaceID: "workspace_id",
	}
	currentDir, _ = os.Getwd()
	jobs          = []*jobsdb.JobT{
		{JobID: 1014},
		{JobID: 1015},
		{JobID: 1016},
		{JobID: 1017},
		{JobID: 1018},
		{JobID: 1019},
		{JobID: 1020},
		{JobID: 1021},
		{JobID: 1022},
		{JobID: 1023},
	}
)

func initEloqua() {
	once.Do(func() {
		logger.Reset()
		misc.Init()
	})
}

type eloquaFixture struct {
	ctrl          *gomock.Controller
	eloquaService *mockeloquaservice.MockEloquaService
	bulkUploader  *eloqua.EloquaBulkUploader
}

func newEloquaFixture(t *testing.T) *eloquaFixture {
	t.Helper()
	config.Reset()
	config.Set("BatchRouter.ELOQUA.MaxUploadLimit", 200*bytesize.B)
	t.Cleanup(config.Reset)
	initEloqua()
	ctrl := gomock.NewController(t)
	eloquaService := mockeloquaservice.NewMockEloquaService(ctrl)
	return &eloquaFixture{
		ctrl:          ctrl,
		eloquaService: eloquaService,
		bulkUploader:  eloqua.NewEloquaBulkUploader(logger.NOP, stats.NOP, "Eloqua", "", "", eloquaService),
	}
}

// Simulates the batch router persisting JobImportingParameters into each
// importing status; no event payload or uploader-local state is available.
func importingJobsWithRows(source []*jobsdb.JobT) []*jobsdb.JobT {
	result := make([]*jobsdb.JobT, 0, len(source))
	for i, job := range source {
		result = append(result, &jobsdb.JobT{
			JobID: job.JobID,
			LastJobStatus: jobsdb.JobStatusT{
				Parameters: json.RawMessage(fmt.Sprintf(`{"importId":"/syncs/384:/contacts/imports/384","metadata":{"csvRowIndex":%d}}`, i+1)),
			},
		})
	}
	return result
}

func TestEloqua(t *testing.T) {
	t.Run("When uploading the file", func(t *testing.T) {
		t.Run("TestEloquaUploadWrongFilepath", func(t *testing.T) {
			f := newEloquaFixture(t)
			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1014, 1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022, 1023},
				FailedJobIDs:    []int64{},
				FileName:        "",
				Destination:     &destination,
				Manager:         f.bulkUploader,
			}
			expected := common.AsyncUploadOutput{
				FailedReason:        "got error while opening the file. open : no such file or directory",
				ImportingJobIDs:     nil,
				FailedJobIDs:        []int64{1014, 1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022, 1023},
				ImportingParameters: nil,
				ImportingCount:      0,
				FailedCount:         10,
			}
			received := f.bulkUploader.Upload(t.Context(), &asyncDestination)
			require.Equal(t, expected, received)
		})
		t.Run("TestEloquaErrorWhileFetchingFields", func(t *testing.T) {
			f := newEloquaFixture(t)
			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1014, 1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022, 1023},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "testdata/uploadDataIdentify.txt"),
				Destination:     &destination,
				Manager:         f.bulkUploader,
			}
			f.eloquaService.EXPECT().FetchFields(gomock.Any()).Return(nil, fmt.Errorf("either authorization is wrong or the object is not found"))

			expected := common.AsyncUploadOutput{
				FailedReason:        "got error while fetching fields. either authorization is wrong or the object is not found",
				ImportingJobIDs:     nil,
				FailedJobIDs:        []int64{1014, 1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022, 1023},
				ImportingParameters: nil,
				ImportingCount:      0,
				FailedCount:         10,
			}
			received := f.bulkUploader.Upload(t.Context(), &asyncDestination)
			require.Equal(t, expected, received)
		})

		t.Run("TestEloquaErrorWhileCreatingImportDefinition", func(t *testing.T) {
			f := newEloquaFixture(t)
			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1014, 1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022, 1023},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "testdata/uploadDataIdentify.txt"),
				Destination:     &destination,
				Manager:         f.bulkUploader,
			}

			f.eloquaService.EXPECT().FetchFields(gomock.Any()).Return(&eloqua.Fields{
				Items: []eloqua.Item{
					{
						Statement:    "{{CustomObject[172].Field[976]}}",
						InternalName: "C_EmailAddress",
					},
					{
						Statement:    "{{CustomObject[172].Field[976]}}",
						InternalName: "C_FirstName",
					},
				},
				TotalResults: 4,
				Limit:        1000,
				Offset:       0,
				Count:        4,
				HasMore:      false,
			}, nil)
			f.eloquaService.EXPECT().CreateImportDefinition(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("some error while creating the importDefinition"))
			expected := common.AsyncUploadOutput{
				FailedReason:        "unable to create importdefinition. some error while creating the importDefinition",
				ImportingJobIDs:     nil,
				FailedJobIDs:        []int64{1014, 1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022, 1023},
				ImportingParameters: nil,
				ImportingCount:      0,
				FailedCount:         10,
			}
			received := f.bulkUploader.Upload(t.Context(), &asyncDestination)
			require.Equal(t, expected, received)
		})

		t.Run("TestEloquaErrorWhileUploadingData", func(t *testing.T) {
			f := newEloquaFixture(t)
			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1014, 1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022, 1023},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "testdata/uploadDataIdentify.txt"),
				Destination:     &destination,
				Manager:         f.bulkUploader,
			}

			f.eloquaService.EXPECT().FetchFields(gomock.Any()).Return(&eloqua.Fields{
				Items: []eloqua.Item{
					{
						Statement:    "{{CustomObject[172].Field[976]}}",
						InternalName: "C_EmailAddress",
					},
					{
						Statement:    "{{CustomObject[172].Field[976]}}",
						InternalName: "C_FirstName",
					},
				},
				TotalResults: 4,
				Limit:        1000,
				Offset:       0,
				Count:        4,
				HasMore:      false,
			}, nil)
			f.eloquaService.EXPECT().CreateImportDefinition(gomock.Any(), gomock.Any()).Return(
				&eloqua.ImportDefinition{
					URI: "/contacts/imports/384",
				}, nil)
			f.eloquaService.EXPECT().UploadData(gomock.Any(), gomock.Any()).Return(fmt.Errorf("some error while uploading the data"))
			expected := common.AsyncUploadOutput{
				FailedReason:        "unable to upload the data. some error while uploading the data",
				ImportingJobIDs:     nil,
				FailedJobIDs:        []int64{1014, 1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022, 1023},
				ImportingParameters: nil,
				ImportingCount:      0,
				FailedCount:         10,
			}
			received := f.bulkUploader.Upload(t.Context(), &asyncDestination)
			require.Equal(t, expected, received)
		})

		t.Run("TestEloquaErrorWhileRunningSync", func(t *testing.T) {
			f := newEloquaFixture(t)
			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1014, 1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022, 1023},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "testdata/uploadDataIdentify.txt"),
				Destination:     &destination,
				Manager:         f.bulkUploader,
			}

			f.eloquaService.EXPECT().FetchFields(gomock.Any()).Return(&eloqua.Fields{
				Items: []eloqua.Item{
					{
						Statement:    "{{CustomObject[172].Field[976]}}",
						InternalName: "C_EmailAddress",
					},
					{
						Statement:    "{{CustomObject[172].Field[976]}}",
						InternalName: "C_FirstName",
					},
				},
				TotalResults: 4,
				Limit:        1000,
				Offset:       0,
				Count:        4,
				HasMore:      false,
			}, nil)
			f.eloquaService.EXPECT().CreateImportDefinition(gomock.Any(), gomock.Any()).Return(
				&eloqua.ImportDefinition{
					URI: "/contacts/imports/384",
				}, nil)
			f.eloquaService.EXPECT().UploadData(gomock.Any(), gomock.Any()).Return(nil)
			f.eloquaService.EXPECT().RunSync(gomock.Any()).Return("", fmt.Errorf("some error occurred while running the sync"))
			expected := common.AsyncUploadOutput{
				FailedReason:        "unable to run the sync after uploading the file. some error occurred while running the sync",
				ImportingJobIDs:     nil,
				FailedJobIDs:        []int64{1014, 1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022, 1023},
				ImportingParameters: nil,
				ImportingCount:      0,
				FailedCount:         10,
			}
			received := f.bulkUploader.Upload(t.Context(), &asyncDestination)
			require.Equal(t, expected, received)
		})
		t.Run("TestEloquaSuccessfulIdentify", func(t *testing.T) {
			f := newEloquaFixture(t)
			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1014, 1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022, 1023},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "testdata/uploadDataIdentify.txt"),
				Destination:     &destination,
				Manager:         f.bulkUploader,
			}

			f.eloquaService.EXPECT().FetchFields(gomock.Any()).Return(&eloqua.Fields{
				Items: []eloqua.Item{
					{
						Statement:    "{{CustomObject[172].Field[976]}}",
						InternalName: "C_EmailAddress",
					},
					{
						Statement:    "{{CustomObject[172].Field[976]}}",
						InternalName: "C_FirstName",
					},
				},
				TotalResults: 4,
				Limit:        1000,
				Offset:       0,
				Count:        4,
				HasMore:      false,
			}, nil)
			f.eloquaService.EXPECT().CreateImportDefinition(gomock.Any(), gomock.Any()).Return(
				&eloqua.ImportDefinition{
					URI: "/contacts/imports/384",
				}, nil)
			f.eloquaService.EXPECT().UploadData(gomock.Any(), gomock.Any()).Return(nil)
			f.eloquaService.EXPECT().RunSync(gomock.Any()).Return("/syncs/384", nil)
			importParameters := []byte(`{"importId":"/syncs/384:/contacts/imports/384","importCount":9,"uniqueKeys":[]}`)
			jobParameters := make(map[int64]json.RawMessage)
			for i, jobID := range asyncDestination.ImportingJobIDs[:9] {
				jobParameters[jobID] = json.RawMessage(fmt.Sprintf(`{"csvRowIndex":%d}`, i+1))
			}
			expected := common.AsyncUploadOutput{
				FailedReason:           "failed as the fileSizeLimit has over",
				ImportingJobIDs:        []int64{1014, 1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022},
				FailedJobIDs:           []int64{1023},
				ImportingParameters:    json.RawMessage(importParameters),
				JobImportingParameters: jobParameters,
				ImportingCount:         9,
				FailedCount:            1,
			}
			received := f.bulkUploader.Upload(t.Context(), &asyncDestination)
			require.Equal(t, expected, received)
		})
	})
	t.Run("When poling the file", func(t *testing.T) {
		t.Run("TestEloquaFailedToGetSyncStatus", func(t *testing.T) {
			f := newEloquaFixture(t)
			pollInput := common.AsyncPoll{
				ImportId: "/syncs/384:/contacts/imports/384",
			}
			f.eloquaService.EXPECT().CheckSyncStatus(gomock.Any()).Return("", fmt.Errorf("some error occurred while fetching the sync status"))
			expected := common.PollStatusResponse{
				Complete:   false,
				InProgress: false,
				StatusCode: 500,
				HasFailed:  false,
				HasWarning: false,
			}
			received := f.bulkUploader.Poll(t.Context(), pollInput)
			require.Equal(t, expected, received)
		})
		t.Run("TestEloquaReceivedSuccess", func(t *testing.T) {
			f := newEloquaFixture(t)
			pollInput := common.AsyncPoll{
				ImportId: "/syncs/384:/contacts/imports/384",
			}
			f.eloquaService.EXPECT().CheckSyncStatus(gomock.Any()).Return("success", nil)
			f.eloquaService.EXPECT().DeleteImportDefinition(gomock.Any()).Return(nil)
			expected := common.PollStatusResponse{
				Complete:   true,
				InProgress: false,
				StatusCode: 200,
				HasFailed:  false,
				HasWarning: false,
			}
			received := f.bulkUploader.Poll(t.Context(), pollInput)
			require.Equal(t, expected, received)
		})
		t.Run("TestEloquaReceivedPending", func(t *testing.T) {
			f := newEloquaFixture(t)
			pollInput := common.AsyncPoll{
				ImportId: "/syncs/384:/contacts/imports/384",
			}
			f.eloquaService.EXPECT().CheckSyncStatus(gomock.Any()).Return("pending", nil)
			expected := common.PollStatusResponse{
				InProgress: true,
			}
			received := f.bulkUploader.Poll(t.Context(), pollInput)
			require.Equal(t, expected, received)
		})
	})

	t.Run("While poling the failed events", func(t *testing.T) {
		t.Run("TestEloquaErrorOccurredWhileUploading", func(t *testing.T) {
			f := newEloquaFixture(t)

			pollInput := common.GetUploadStatsInput{
				FailedJobParameters: "/syncs/384",
				Parameters:          []byte(`{"importId":"/syncs/384:/contacts/imports/384","importCount":10}`),
				ImportingList:       importingJobsWithRows(jobs),
			}
			metadata := common.EventStatMeta{
				AbortedKeys:   []int64{1014, 1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022, 1023},
				SucceededKeys: []int64{},
				AbortedReasons: map[int64]string{
					1016: "some error occurred please check the logs, using this syncId: /syncs/384",
					1021: "some error occurred please check the logs, using this syncId: /syncs/384",
					1022: "some error occurred please check the logs, using this syncId: /syncs/384",
					1014: "some error occurred please check the logs, using this syncId: /syncs/384",
					1015: "some error occurred please check the logs, using this syncId: /syncs/384",
					1017: "some error occurred please check the logs, using this syncId: /syncs/384",
					1018: "some error occurred please check the logs, using this syncId: /syncs/384",
					1019: "some error occurred please check the logs, using this syncId: /syncs/384",
					1020: "some error occurred please check the logs, using this syncId: /syncs/384",
					1023: "some error occurred please check the logs, using this syncId: /syncs/384",
				},
			}
			expected := common.GetUploadStatsResponse{
				StatusCode: 200,
				Metadata:   metadata,
			}
			received := f.bulkUploader.GetUploadStats(pollInput)
			require.Equal(t, expected, received)
		})
		t.Run("TestEloquaFailedToFetchRejectedData", func(t *testing.T) {
			f := newEloquaFixture(t)

			pollInput := common.GetUploadStatsInput{
				WarningJobParameters: "/syncs/384",
				Parameters:           []byte(`{"importId":"/syncs/384:/contacts/imports/384","importCount":10,"uniqueKeys":[]}`),
				ImportingList:        importingJobsWithRows(jobs),
			}

			f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).Return(nil, fmt.Errorf("some error occurred while fetching the data"))
			received := f.bulkUploader.GetUploadStats(pollInput)
			require.Equal(t, 500, received.StatusCode)
			require.Empty(t, received.Error)
		})
		t.Run("TestEloquaSucceedToFetchRejectedData", func(t *testing.T) {
			f := newEloquaFixture(t)
			pollInput := common.GetUploadStatsInput{
				WarningJobParameters: "/syncs/384",
				Parameters:           []byte(`{"importId":"/syncs/384:/contacts/imports/384","importCount":10,"uniqueKeys":[]}`),
				ImportingList:        importingJobsWithRows(jobs),
			}
			// The returned rows are the importing set; no upload or payload is
			// needed to rebuild the mapping.
			f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).Return(&eloqua.RejectResponse{
				Count:        2,
				TotalResults: 2,
				Limit:        1000,
				Offset:       0,
				HasMore:      false,
				Items: []eloqua.RejectedItem{
					{
						FieldValues: map[string]string{
							"C_FirstName":    "test7@mail.com",
							"C_EmailAddress": "Test7",
						},
						Message:       "Invalid email address.",
						StatusCode:    "ELQ-00002",
						RecordIndex:   5,
						InvalidFields: []string{"C_EmailAddress"},
					},
					{
						FieldValues: map[string]string{
							"C_FirstName":    "test13@mail.com",
							"C_EmailAddress": "Test12",
						},
						Message:       "Invalid email address.",
						StatusCode:    "ELQ-00002",
						RecordIndex:   9,
						InvalidFields: []string{"C_EmailAddress"},
					},
				},
			}, nil)

			metadata := common.EventStatMeta{
				AbortedKeys:   []int64{1018, 1022},
				SucceededKeys: []int64{1014, 1015, 1016, 1017, 1019, 1020, 1021, 1023},
				AbortedReasons: map[int64]string{
					1018: "ELQ-00002 : Invalid email address. C_EmailAddress : Test7",
					1022: "ELQ-00002 : Invalid email address. C_EmailAddress : Test12",
				},
			}
			expected := common.GetUploadStatsResponse{
				StatusCode: 200,
				Metadata:   metadata,
			}
			received := f.bulkUploader.GetUploadStats(pollInput)
			require.Equal(t, expected, received)
		})
		t.Run("fresh manager after config change", func(t *testing.T) {
			f := newEloquaFixture(t)
			f.bulkUploader = eloqua.NewEloquaBulkUploader(logger.NOP, stats.NOP, "Eloqua", "changed authorization", "changed endpoint", f.eloquaService)
			input := statsInput(statsJobs())
			for _, job := range input.ImportingList {
				job.EventPayload = nil
			}
			f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).Return(rejectPage(2,
				eloqua.RejectedItem{RecordIndex: 1, StatusCode: "ELQ-00040", InvalidFields: []string{"C_FirstName"}},
				eloqua.RejectedItem{RecordIndex: 2, StatusCode: "ELQ-00040", InvalidFields: []string{"C_EmailAddress"}}), nil)
			result := f.bulkUploader.GetUploadStats(input)
			require.Equal(t, 200, result.StatusCode)
			require.Equal(t, []int64{11}, result.Metadata.SucceededKeys)
			require.Equal(t, []int64{12}, result.Metadata.AbortedKeys)
			require.Contains(t, result.Metadata.AbortedReasons[12], "ELQ-00040")
		})

		t.Run("retry rejected records after failed fetch", func(t *testing.T) {
			f := newEloquaFixture(t)
			input := statsInput(statsJobs())
			f.eloquaService.EXPECT().CheckSyncStatus(gomock.Any()).Return("warning", nil)
			f.eloquaService.EXPECT().DeleteImportDefinition(gomock.Any()).Return(nil)
			poll := f.bulkUploader.Poll(t.Context(), common.AsyncPoll{ImportId: "/syncs/384:/contacts/imports/384"})
			require.True(t, poll.HasWarning)
			f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).Return(nil, errors.New("temporary API failure"))
			first := f.bulkUploader.GetUploadStats(input)
			require.Equal(t, 500, first.StatusCode)
			require.Empty(t, first.Metadata.SucceededKeys)
			f.bulkUploader = eloqua.NewEloquaBulkUploader(logger.NOP, stats.NOP, "Eloqua", "", "", f.eloquaService)
			f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).Return(rejectPage(1, eloqua.RejectedItem{RecordIndex: 2, StatusCode: "INVALID"}), nil)
			second := f.bulkUploader.GetUploadStats(input)
			require.Equal(t, 200, second.StatusCode)
			require.Equal(t, []int64{11}, second.Metadata.SucceededKeys)
			require.Equal(t, []int64{12}, second.Metadata.AbortedKeys)
		})

		t.Run("partial metadata and shared row key", func(t *testing.T) {
			for _, duplicate := range []bool{false, true} {
				t.Run(fmt.Sprint(duplicate), func(t *testing.T) {
					f := newEloquaFixture(t)
					jobs := importingJobsWithRows([]*jobsdb.JobT{{JobID: 11}, {JobID: 12}, {JobID: 13}})
					if duplicate {
						jobs[1].LastJobStatus.Parameters = jobs[0].LastJobStatus.Parameters
					} else {
						jobs[1].LastJobStatus.Parameters = []byte(sharedEloquaParams)
					}
					f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).Return(rejectPage(1, eloqua.RejectedItem{RecordIndex: 1, StatusCode: "INVALID"}), nil)
					result := f.bulkUploader.GetUploadStats(statsInput(jobs))
					require.Equal(t, []int64{11, 12}, result.Metadata.AbortedKeys)
					require.Equal(t, []int64{13}, result.Metadata.SucceededKeys)
					if duplicate {
						require.Equal(t, result.Metadata.AbortedReasons[11], result.Metadata.AbortedReasons[12])
					} else {
						require.Contains(t, result.Metadata.AbortedReasons[12], "cannot be reconciled")
					}
				})
			}
		})

		t.Run("unmatched reject aborts unproved remainder", func(t *testing.T) {
			f := newEloquaFixture(t)
			f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).Return(rejectPage(1, eloqua.RejectedItem{RecordIndex: 3, StatusCode: "INVALID"}), nil)
			result := f.bulkUploader.GetUploadStats(statsInput(statsJobs()))
			require.Equal(t, 200, result.StatusCode)
			require.ElementsMatch(t, []int64{11, 12}, result.Metadata.AbortedKeys)
			require.Empty(t, result.Metadata.SucceededKeys)
		})

		t.Run("no correlation metadata aborts batch", func(t *testing.T) {
			f := newEloquaFixture(t)
			result := f.bulkUploader.GetUploadStats(statsInput([]*jobsdb.JobT{{JobID: 11}, {JobID: 12}}))
			require.Equal(t, 200, result.StatusCode)
			require.ElementsMatch(t, []int64{11, 12}, result.Metadata.AbortedKeys)
			require.Empty(t, result.Metadata.SucceededKeys)
		})
	})
}

const sharedEloquaParams = `{"importId":"/syncs/384:/contacts/imports/384","importCount":2,"uniqueKeys":["C_EmailAddress"]}`

func statsJobs() []*jobsdb.JobT {
	return importingJobsWithRows([]*jobsdb.JobT{{JobID: 11}, {JobID: 12}})
}

func statsInput(jobs []*jobsdb.JobT) common.GetUploadStatsInput {
	return common.GetUploadStatsInput{WarningJobParameters: "/syncs/384", Parameters: []byte(sharedEloquaParams), ImportingList: jobs}
}

func rejectPage(total int, items ...eloqua.RejectedItem) *eloqua.RejectResponse {
	return &eloqua.RejectResponse{Offset: 0, TotalResults: total, Limit: 1000, Count: len(items), Items: items}
}
