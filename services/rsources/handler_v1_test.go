package rsources

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"

	_ "github.com/lib/pq"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"

	testlog "github.com/rudderlabs/rudder-server/testhelper/log"
)

// TODO: delete this file once we remove support for the v1 api
var _ = Describe("Using sources handler v1", func() {
	var noPaging PagingInfo
	Context("single-tenant setup with a single local datasource v1", Ordered, func() {
		var (
			pool     *dockertest.Pool
			resource postgresResource
			sh       JobService
		)
		stats := Stats{
			In:     10,
			Out:    4,
			Failed: 6,
		}
		BeforeEach(func() {
			var err error
			pool, err = dockertest.NewPool("")
			Expect(err).NotTo(HaveOccurred())
			resource = newDBResource(pool, "", "postgres")
			config := JobServiceConfig{
				LocalHostname:       "postgres",
				MaxPoolSize:         1,
				LocalConn:           resource.externalDSN,
				Log:                 testlog.GinkgoLogger,
				ShouldSetupSharedDB: true,
			}
			sh = createService(config)
		})

		AfterEach(func() {
			purgeResources(pool, resource.resource)
		})

		It("should be able to add and get failed records v1", func() {
			jobRunId := newJobRunId()
			addFailedRecords(resource.db, jobRunId, defaultJobTargetKey, sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`)},
				{Record: []byte(`{"record-2": "id-2"}`)},
			})
			jobFilters := JobFilter{
				SourceID:  []string{"source_id"},
				TaskRunID: []string{"task_run_id"},
			}

			failedRecords, err := sh.GetFailedRecordsV1(context.Background(), jobRunId, jobFilters, noPaging)
			Expect(err).NotTo(HaveOccurred(), "it should be able to get failed records")
			expcetedRecords := JobFailedRecordsV1{
				ID: jobRunId,
				Tasks: []TaskFailedRecords[json.RawMessage]{{
					ID: "task_run_id",
					Sources: []SourceFailedRecords[json.RawMessage]{{
						ID: "source_id",
						Destinations: []DestinationFailedRecords[json.RawMessage]{{
							ID: "destination_id",
							Records: []json.RawMessage{
								[]byte(`{"record-1": "id-1"}`),
								[]byte(`{"record-2": "id-2"}`),
							},
						}},
					}},
				}},
			}
			Expect(failedRecords).To(Equal(expcetedRecords), "it should be able to get failed records")
		})

		It("should be able to add and get failed records v1 using pagination", func() {
			jobRunId := newJobRunId()
			addFailedRecords(resource.db, jobRunId, defaultJobTargetKey, sh, []FailedRecord{
				{Record: []byte(`"id-1"`)},
				{Record: []byte(`"id-2"`)},
				{Record: []byte(`"id-3"`)},
				{Record: []byte(`"id-4"`)},
			})
			jobFilters := JobFilter{
				SourceID:  []string{defaultJobTargetKey.SourceID},
				TaskRunID: []string{defaultJobTargetKey.TaskRunID},
			}
			paging := PagingInfo{
				Size: 2,
			}
			for i := 0; i < 2; i++ {
				failedRecords, err := sh.GetFailedRecordsV1(context.Background(), jobRunId, jobFilters, paging)
				Expect(err).NotTo(HaveOccurred(), "it should be able to get failed records")
				Expect(failedRecords.Paging).NotTo(BeNil(), "paging should not be nil")
				paging = *failedRecords.Paging
				expectedRecords := JobFailedRecordsV1{
					ID: jobRunId,
					Tasks: []TaskFailedRecords[json.RawMessage]{{
						ID: "task_run_id",
						Sources: []SourceFailedRecords[json.RawMessage]{{
							ID: defaultJobTargetKey.SourceID,
							Destinations: []DestinationFailedRecords[json.RawMessage]{{
								ID: defaultJobTargetKey.DestinationID,
								Records: []json.RawMessage{
									[]byte(fmt.Sprintf(`"id-%d"`, (2*i)+1)),
									[]byte(fmt.Sprintf(`"id-%d"`, (2*i)+2)),
								},
							}},
						}},
					}},
					Paging: failedRecords.Paging,
				}
				Expect(failedRecords).To(Equal(expectedRecords), "it should be able to get failed records")

			}
			failedRecords, err := sh.GetFailedRecordsV1(context.Background(), jobRunId, jobFilters, paging)
			Expect(err).NotTo(HaveOccurred(), "it should be able to get failed records")
			Expect(failedRecords.Tasks).To(BeEmpty(), "last page should be empty")
			Expect(failedRecords.Paging).To(BeNil(), "last page should have no paging")
		})

		It("shouldn't be able to get failed records v1 when failed records collection is disabled", func() {
			handler := sh.(*sourcesHandler)
			previous := handler.config.SkipFailedRecordsCollection
			handler.config.SkipFailedRecordsCollection = true
			defer func() { handler.config.SkipFailedRecordsCollection = previous }()

			jobRunId := newJobRunId()
			addFailedRecords(resource.db, jobRunId, defaultJobTargetKey, sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`)},
				{Record: []byte(`{"record-2": "id-2"}`)},
			})

			failedRecords, err := sh.GetFailedRecordsV1(context.Background(), jobRunId, JobFilter{}, noPaging)
			Expect(err).To(HaveOccurred(), "it shouldn't be able to get failed records")
			Expect(failedRecords).To(Equal(JobFailedRecordsV1{ID: jobRunId}), "it should return an empty failed records")
			Expect(err).To(Equal(ErrOperationNotSupported), "it should return an ErrOperationNotSupported error")
		})

		It("should be able to delete stats and failed keys v1", func() {
			jobRunId := newJobRunId()
			increment(resource.db, jobRunId, defaultJobTargetKey, stats, sh, nil)
			addFailedRecords(resource.db, jobRunId, defaultJobTargetKey, sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`)},
				{Record: []byte(`{"record-2": "id-2"}`)},
			})
			err := sh.Delete(context.Background(), jobRunId, JobFilter{})
			Expect(err).NotTo(HaveOccurred(), "it should be able to delete stats, failed keys for the jobrunid")
			jobFilters := JobFilter{
				SourceID:  []string{"source_id"},
				TaskRunID: []string{"task_run_id"},
			}
			status, err := sh.GetStatus(context.Background(), jobRunId, jobFilters)
			Expect(err).To(HaveOccurred())
			Expect(status).To(Equal(JobStatus{}))
			Expect(errors.Is(err, ErrStatusNotFound)).To(BeTrue(), "it should return a StatusNotFoundError")
			failedRecords, err := sh.GetFailedRecordsV1(context.Background(), jobRunId, jobFilters, noPaging)
			Expect(err).NotTo(HaveOccurred())
			Expect(failedRecords).To(Equal(JobFailedRecordsV1{ID: jobRunId}))
		})

		It("should be able to delete stats and failed keys partially v1", func() {
			otherJobTargetKey := defaultJobTargetKey
			otherJobTargetKey.SourceID = "other_source_id"
			jobRunId := newJobRunId()
			increment(resource.db, jobRunId, defaultJobTargetKey, stats, sh, nil)
			increment(resource.db, jobRunId, otherJobTargetKey, stats, sh, nil)

			addFailedRecords(resource.db, jobRunId, defaultJobTargetKey, sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`)},
				{Record: []byte(`{"record-2": "id-2"}`)},
			})
			addFailedRecords(resource.db, jobRunId, otherJobTargetKey, sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`)},
				{Record: []byte(`{"record-2": "id-2"}`)},
			})
			err := sh.Delete(context.Background(), jobRunId, JobFilter{SourceID: []string{"other_source_id"}})
			Expect(err).NotTo(HaveOccurred(), "it should be able to delete stats, failed keys for the jobrunid")

			jobFilters := JobFilter{
				SourceID:  []string{"other_source_id"},
				TaskRunID: []string{"task_run_id"},
			}
			status, err := sh.GetStatus(context.Background(), jobRunId, jobFilters)
			Expect(err).To(HaveOccurred())
			Expect(status).To(Equal(JobStatus{}))
			Expect(errors.Is(err, ErrStatusNotFound)).To(BeTrue(), "it should return a StatusNotFoundError")
			failedRecords, err := sh.GetFailedRecordsV1(context.Background(), jobRunId, jobFilters, noPaging)
			Expect(err).NotTo(HaveOccurred())
			Expect(failedRecords).To(Equal(JobFailedRecordsV1{ID: jobRunId}))

			jobFilters.SourceID = []string{defaultJobTargetKey.SourceID}
			_, err = sh.GetStatus(context.Background(), jobRunId, jobFilters)
			Expect(err).ToNot(HaveOccurred())
			failedRecords, err = sh.GetFailedRecordsV1(context.Background(), jobRunId, jobFilters, noPaging)
			Expect(err).NotTo(HaveOccurred())
			Expect(failedRecords).ToNot(Equal(JobFailedRecordsV1{ID: jobRunId}))
		})

		It("shouldn't be able to delete stats for an incomplete source v1", func() {
			jobRunId := newJobRunId()
			stats := Stats{
				In:     10,
				Out:    4,
				Failed: 5,
			}
			increment(resource.db, jobRunId, defaultJobTargetKey, stats, sh, nil)

			err := sh.Delete(context.Background(), jobRunId, JobFilter{SourceID: []string{defaultJobTargetKey.SourceID}})
			Expect(err).To(Equal(ErrSourceNotCompleted), "it shouldn't be able to delete stats for an incomplete source")
		})

		It("shouldn't be able to delete stats for an invalid job v1", func() {
			jobRunId := newJobRunId()
			stats := Stats{
				In:     10,
				Out:    4,
				Failed: 5,
			}
			increment(resource.db, jobRunId, defaultJobTargetKey, stats, sh, nil)

			err := sh.Delete(context.Background(), "invalidJobRunId", JobFilter{SourceID: []string{defaultJobTargetKey.SourceID}})
			Expect(err).To(Equal(ErrStatusNotFound), "it shouldn't be able to delete stats for an invalid jobrunid")
		})

		It("should be able to get failed records by filtering v1", func() {
			jobRunId := newJobRunId()

			addFailedRecords(resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id1",
				SourceID:      "source_id1",
				DestinationID: "destination_id",
			}, sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`)},
				{Record: []byte(`{"record-2": "id-2"}`)},
			})
			addFailedRecords(resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id1",
				SourceID:      "source_id1",
				DestinationID: "destination_id",
			}, sh, []FailedRecord{
				{Record: []byte(`{"record-11": "id-112"}`)},
				{Record: []byte(`{"record-22": "id-222"}`)},
			})
			addFailedRecords(resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id1",
				SourceID:      "source_id2",
				DestinationID: "destination_id",
			}, sh, []FailedRecord{
				{Record: []byte(`{"record-12": "id-12"}`)},
				{Record: []byte(`{"record-21": "id-21"}`)},
			})
			addFailedRecords(resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id2",
				SourceID:      "source_id2",
				DestinationID: "destination_id",
			}, sh, []FailedRecord{
				{Record: []byte(`{"record-11": "id-11"}`)},
				{Record: []byte(`{"record-32": "id-32"}`)},
			})
			addFailedRecords(resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id2",
				SourceID:      "source_id3",
				DestinationID: "destination_id",
			}, sh, []FailedRecord{
				{Record: []byte(`{"record-13": "id-13"}`)},
				{Record: []byte(`{"record-23": "id-23"}`)},
			})
			expected := JobFailedRecordsV1{
				ID: jobRunId,
				Tasks: []TaskFailedRecords[json.RawMessage]{
					{
						ID: "task_run_id1",
						Sources: []SourceFailedRecords[json.RawMessage]{
							{
								ID: "source_id1",
								Destinations: []DestinationFailedRecords[json.RawMessage]{{
									ID: "destination_id",
									Records: []json.RawMessage{
										[]byte(`{"record-1": "id-1"}`),
										[]byte(`{"record-11": "id-112"}`),
										[]byte(`{"record-2": "id-2"}`),
										[]byte(`{"record-22": "id-222"}`),
									},
								}},
							},
							{
								ID: "source_id2",
								Destinations: []DestinationFailedRecords[json.RawMessage]{{
									ID: "destination_id",
									Records: []json.RawMessage{
										[]byte(`{"record-12": "id-12"}`),
										[]byte(`{"record-21": "id-21"}`),
									},
								}},
							},
						},
					},
					{
						ID: "task_run_id2",
						Sources: []SourceFailedRecords[json.RawMessage]{{
							ID: "source_id2",
							Destinations: []DestinationFailedRecords[json.RawMessage]{{
								ID: "destination_id",
								Records: []json.RawMessage{
									[]byte(`{"record-11": "id-11"}`),
									[]byte(`{"record-32": "id-32"}`),
								},
							}},
						}},
					},
				},
			}
			failedRecords, err := sh.GetFailedRecordsV1(context.Background(), jobRunId, JobFilter{
				SourceID:  []string{"source_id1", "source_id2"},
				TaskRunID: []string{"task_run_id1", "task_run_id2"},
			}, noPaging)
			Expect(err).NotTo(HaveOccurred())
			Expect(expected).To(Equal(failedRecords), "it should return the failed records for the jobrunid based on the filtering")
		})
	})

	Context("multitenant setup with local & shared datasources v1", Ordered, func() {
		var (
			pool               *dockertest.Pool
			network            *docker.Network
			pgA, pgB, pgC      postgresResource
			configA, configB   JobServiceConfig
			serviceA, serviceB JobService
		)

		BeforeEach(func() {
			var err error
			pool, err = dockertest.NewPool("")
			Expect(err).NotTo(HaveOccurred())
			networkId := randomString()
			network, _ = pool.Client.NetworkInfo(networkId)
			if network == nil {
				network, err = pool.Client.CreateNetwork(docker.CreateNetworkOptions{Name: networkId})
				Expect(err).NotTo(HaveOccurred())
			}
			for containerID := range network.Containers { // Remove any containers left from previous runs
				_ = pool.Client.RemoveContainer(docker.RemoveContainerOptions{ID: containerID, Force: true, RemoveVolumes: true})
			}
			postgres1Hostname := randomString() + "-1"
			postgres2Hostname := randomString() + "-2"
			postgres3Hostname := randomString() + "-3"
			pgA = newDBResource(pool, network.ID, postgres1Hostname, "wal_level=logical")
			pgB = newDBResource(pool, network.ID, postgres2Hostname, "wal_level=logical")
			pgC = newDBResource(pool, network.ID, postgres3Hostname)

			configA = JobServiceConfig{
				LocalHostname:          postgres1Hostname,
				MaxPoolSize:            1,
				LocalConn:              pgA.externalDSN,
				SharedConn:             pgC.externalDSN,
				SubscriptionTargetConn: pgA.internalDSN,
				Log:                    testlog.GinkgoLogger,
				ShouldSetupSharedDB:    true,
			}

			configB = JobServiceConfig{
				LocalHostname:          postgres2Hostname,
				MaxPoolSize:            1,
				LocalConn:              pgB.externalDSN,
				SharedConn:             pgC.externalDSN,
				SubscriptionTargetConn: pgB.internalDSN,
				Log:                    testlog.GinkgoLogger,
				ShouldSetupSharedDB:    true,
			}

			// Start 2 JobServices
			// 1. js1 with local=pgA, remote=pgC
			// 2. js2 with local=pgB, remote=pgC
			serviceA = createService(configA)
			serviceB = createService(configB)
		})

		AfterEach(func() {
			purgeResources(pool, pgA.resource, pgB.resource, pgC.resource)
			if network != nil {
				_ = pool.Client.RemoveNetwork(network.ID)
			}
		})

		It("should be able to query both services for the same jobRunId and receive same failed Records v1", func() {
			jobRunId := newJobRunId()
			addFailedRecords(pgA.db, jobRunId, defaultJobTargetKey, serviceA, []FailedRecord{
				{Record: json.RawMessage(`{"id": "1"}`)},
				{Record: json.RawMessage(`{"id": "2"}`)},
			})
			addFailedRecords(pgB.db, jobRunId, defaultJobTargetKey, serviceB, []FailedRecord{
				{Record: json.RawMessage(`{"id": "2"}`)},
				{Record: json.RawMessage(`{"id": "3"}`)},
			})
			expected := JobFailedRecordsV1{
				ID: jobRunId,
				Tasks: []TaskFailedRecords[json.RawMessage]{
					{
						ID: defaultJobTargetKey.TaskRunID,
						Sources: []SourceFailedRecords[json.RawMessage]{
							{
								ID: defaultJobTargetKey.SourceID,
								Destinations: []DestinationFailedRecords[json.RawMessage]{{
									ID: defaultJobTargetKey.DestinationID,
									Records: []json.RawMessage{
										[]byte(`{"id": "1"}`),
										[]byte(`{"id": "2"}`),
										[]byte(`{"id": "2"}`),
										[]byte(`{"id": "3"}`),
									},
								}},
							},
						},
					},
				},
			}

			var err error
			var failedKeysA, failedKeysB JobFailedRecordsV1
			Eventually(func() bool {
				failedKeysA, err = serviceA.GetFailedRecordsV1(context.Background(), jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				}, noPaging)
				if err != nil {
					err = fmt.Errorf("failed to get failed records from serviceA: %w", err)
					return false
				}
				failedKeysB, err = serviceB.GetFailedRecordsV1(context.Background(), jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				}, noPaging)
				if err != nil {
					err = fmt.Errorf("failed to get failed records from serviceB: %w", err)
					return false
				}
				if !reflect.DeepEqual(failedKeysA, failedKeysB) {
					err = fmt.Errorf("failed keys from serviceA are different compared to failed keys from serviceB")
					return false
				}
				if len(failedKeysA.Tasks) != 1 || len(failedKeysA.Tasks[0].Sources) != 1 || len(failedKeysA.Tasks[0].Sources[0].Destinations) != 1 {
					err = fmt.Errorf("failed keys from serviceA don't contain 1 task with 1 source and 1 destination")
					return false
				}
				sort.Slice(failedKeysA.Tasks[0].Sources[0].Destinations[0].Records, func(i, j int) bool {
					return string(failedKeysA.Tasks[0].Sources[0].Destinations[0].Records[i]) < string(failedKeysA.Tasks[0].Sources[0].Destinations[0].Records[j])
				})
				if !reflect.DeepEqual(failedKeysA, expected) {
					err = fmt.Errorf("failed keys from serviceA don't match expectation")
					return false
				}
				return true
			}, "30s", "100ms").Should(BeTrue(), "Failed Records from both services should be the same", string(mustMarshal(failedKeysA)), string(mustMarshal(failedKeysB)), string(mustMarshal(expected)), err)
		})
	})
})
