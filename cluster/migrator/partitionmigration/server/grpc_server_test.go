package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/cluster/migrator/partitionmigration/client"
	"github.com/rudderlabs/rudder-server/jobsdb"
	proto "github.com/rudderlabs/rudder-server/proto/cluster"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/tx"
)

func TestGrpcServer(t *testing.T) {
	const ackTimeout = 15 * time.Second
	const sendTimeout = 5 * time.Second

	const migrationJobID = "job-1"
	const tablePrefix = "test"
	const partitionID = "partition-1"

	// newEnv initializes the test environment with a running gRPC server and a jobsdb instance.
	newEnv := func(t *testing.T, opts ...Opt) *testGrpcServerEnv {
		t.Helper()
		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		pg, err := postgres.Setup(pool, t)
		require.NoError(t, err)
		runNodeMigration(t, pg.DB)
		db := jobsdb.NewForReadWrite(tablePrefix,
			jobsdb.WithDBHandle(pg.DB),
			jobsdb.WithNumPartitions(64),
			jobsdb.WithSkipMaintenanceErr(true),
		)
		err = db.Start()
		require.NoError(t, err)
		t.Cleanup(func() {
			db.Stop()
		})
		mjobsdb := &mockJobsDB{JobsDB: db}
		port, err := testhelper.GetFreePort()
		c := config.New()
		require.NoError(t, err)
		c.Set("PartitionMigration.Grpc.Server.Port", port)
		c.Set("PartitionMigration.Grpc.Server.WriteBufferSize", -1) // disable write buffer
		c.Set("LOG_LEVEL", "DEBUG")
		opts = append(opts, WithLogger(logger.NewFactory(c).NewLogger()))
		pms := NewPartitionMigrationServer(t.Context(), []jobsdb.JobsDB{mjobsdb}, opts...)
		grpcServer := NewGRPCServer(c, pms)
		require.NoError(t, grpcServer.Start(), "it should be able to start the grpc server")
		t.Cleanup(grpcServer.Stop)
		return &testGrpcServerEnv{
			sqldb:      pg.DB,
			jobsdb:     mjobsdb,
			grpcServer: grpcServer,
			target:     "127.0.0.1:" + strconv.Itoa(port),
		}
	}

	t.Run("stream jobs successfully", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Minute)
		defer cancel()
		env := newEnv(t)
		c := config.New()

		pmc, err := client.NewPartitionMigrationClient(env.target, c)
		require.NoError(t, err, "it should be able to create grpc client")
		t.Cleanup(func() {
			_ = pmc.Close()
		})

		stream, err := pmc.StreamJobs(ctx)
		require.NoError(t, err, "it should be able to create grpc stream")

		err = stream.Send(&proto.StreamJobsRequest{
			Payload: &proto.StreamJobsRequest_Metadata{
				Metadata: &proto.JobStreamMetadata{
					MigrationJobId: migrationJobID,
					TablePrefix:    env.jobsdb.Identifier(),
					PartitionIds:   []string{partitionID},
				},
			},
		})
		require.NoError(t, err, "it should be able to send metadata over grpc stream")

		totalJobs := 1000
		chunkSize := 10
		jobs := env.generateJobs(t, totalJobs)
		chunks := lo.Chunk(jobs, chunkSize)

		for i, chunk := range chunks {
			req := &proto.StreamJobsRequest{
				Payload: &proto.StreamJobsRequest_Chunk{
					Chunk: &proto.JobsBatchChunk{
						BatchIndex: 1,
						LastChunk:  i == len(chunks)-1,
						Jobs:       lo.Map(chunk, proto.JobFromJobsdbJob),
					},
				},
			}
			err := stream.Send(req)
			require.NoError(t, err, "it should be able to send jobs over grpc stream")
		}
		err = stream.CloseSend()
		require.NoError(t, err, "it should be able to close grpc stream")

		ack, err := receiveAckWithTimeout(t, stream, ackTimeout)
		require.NoError(t, err, "it should be able to receive grpc stream response")
		require.EqualValues(t, 1, ack.BatchIndex, "it should receive correct ack from grpc stream")

		_, err = receiveAckWithTimeout(t, stream, ackTimeout)
		require.Error(t, err, "it should receive error on further recv after stream closed")
		require.ErrorIs(t, err, io.EOF, "error should be an EOF")

		jobsResult, err := env.jobsdb.GetUnprocessed(ctx, jobsdb.GetQueryParams{JobsLimit: totalJobs * 2})
		require.NoError(t, err, "it should be able to fetch jobs from jobsdb")
		require.Len(t, jobsResult.Jobs, totalJobs, "it should have stored all jobs to the target jobsdb")

		require.EqualValues(t, -1, env.getLastJobId(t, migrationJobID, env.jobsdb.Identifier()), "it should not have kept any dedup state for the migration")
	})

	t.Run("stream jobs with dedup enabled gets interrupted and then resumes", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Minute)
		defer cancel()
		env := newEnv(t, WithDedupEnabled(true))
		c := config.New()

		sendBatch := func(batch []*jobsdb.JobT, interrupt bool) {
			pmc, err := client.NewPartitionMigrationClient(env.target, c)
			require.NoError(t, err, "it should be able to create grpc client")
			t.Cleanup(func() {
				_ = pmc.Close()
			})
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			stream, err := pmc.StreamJobs(ctx)
			require.NoError(t, err, "it should be able to create grpc stream")

			err = stream.Send(&proto.StreamJobsRequest{
				Payload: &proto.StreamJobsRequest_Metadata{
					Metadata: &proto.JobStreamMetadata{
						MigrationJobId: migrationJobID,
						TablePrefix:    env.jobsdb.Identifier(),
						PartitionIds:   []string{partitionID},
					},
				},
			})
			require.NoError(t, err, "it should be able to send metadata over grpc stream")
			chunks := lo.Chunk(batch, 10)

			for i, chunk := range chunks {
				req := &proto.StreamJobsRequest{
					Payload: &proto.StreamJobsRequest_Chunk{
						Chunk: &proto.JobsBatchChunk{
							BatchIndex: 1,
							LastChunk:  i == len(chunks)-1,
							Jobs:       lo.Map(chunk, proto.JobFromJobsdbJob),
						},
					},
				}
				err := stream.Send(req)
				require.NoError(t, err, "it should be able to send jobs over grpc stream")
			}

			ack, err := receiveAckWithTimeout(t, stream, ackTimeout)
			require.NoError(t, err, "it should be able to receive grpc stream response")
			require.EqualValues(t, 1, ack.BatchIndex, "it should receive correct ack from grpc stream")

			if interrupt {
				// if interrupting, cancel the context early to simulate an interruption and wait for error on recv
				cancel()
				_, err = receiveAckWithTimeout(t, stream, ackTimeout)
				require.Error(t, err, "it should receive error after interrupting grpc stream")
				st, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, codes.Canceled, st.Code())
				return
			}

			// if not interrupted, close the stream normally
			err = stream.CloseSend()
			require.NoError(t, err, "it should be able to close grpc stream")
			_, err = receiveAckWithTimeout(t, stream, ackTimeout)
			require.ErrorIs(t, err, io.EOF, "it should receive EOF after closing grpc stream")
		}
		totalJobs := 1000
		overlap := 50
		jobs := env.generateJobs(t, totalJobs)

		sendBatch(jobs[:totalJobs/2], true) // send first half and interrupt before closing
		require.Greater(t, env.getLastJobId(t, migrationJobID, env.jobsdb.Identifier()), int64(0), "it should have kept dedup state for the migration")

		sendBatch(jobs[(totalJobs/2)-overlap:], false) // send second half with some overlapping jobs

		jobsResult, err := env.jobsdb.GetUnprocessed(ctx, jobsdb.GetQueryParams{JobsLimit: totalJobs * 2})
		require.NoError(t, err, "it should be able to fetch jobs from jobsdb")
		require.Len(t, jobsResult.Jobs, totalJobs, "it should have stored all jobs to the target jobsdb with deduplication")
		require.EqualValues(t, -1, env.getLastJobId(t, migrationJobID, env.jobsdb.Identifier()), "it should not have kept any dedup state for the migration")
	})

	t.Run("stream metadata twice", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Minute)
		defer cancel()
		env := newEnv(t)
		c := config.New()

		pmc, err := client.NewPartitionMigrationClient(env.target, c)
		require.NoError(t, err, "it should be able to create grpc client")
		t.Cleanup(func() {
			_ = pmc.Close()
		})

		stream, err := pmc.StreamJobs(ctx)
		require.NoError(t, err, "it should be able to create grpc stream")

		for range 2 {
			err = stream.Send(&proto.StreamJobsRequest{
				Payload: &proto.StreamJobsRequest_Metadata{
					Metadata: &proto.JobStreamMetadata{
						MigrationJobId: migrationJobID,
						TablePrefix:    env.jobsdb.Identifier(),
						PartitionIds:   []string{partitionID},
					},
				},
			})
			require.NoError(t, err, "it should be able to send metadata over grpc stream")
		}
		// the second metadata should cause an error
		_, err = receiveAckWithTimeout(t, stream, ackTimeout)
		require.Error(t, err, "it should receive error after sending metadata twice over grpc stream")
		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.FailedPrecondition, st.Code())
		require.Equal(t, "received message without job batch", st.Message())
	})

	t.Run("stream without metadata", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Minute)
		defer cancel()
		env := newEnv(t)
		c := config.New()

		pmc, err := client.NewPartitionMigrationClient(env.target, c)
		require.NoError(t, err, "it should be able to create grpc client")
		t.Cleanup(func() {
			_ = pmc.Close()
		})

		stream, err := pmc.StreamJobs(ctx)
		require.NoError(t, err, "it should be able to create grpc stream")

		jobs := env.generateJobs(t, 1)
		err = stream.Send(&proto.StreamJobsRequest{
			Payload: &proto.StreamJobsRequest_Chunk{
				Chunk: &proto.JobsBatchChunk{
					BatchIndex: 1,
					LastChunk:  true,
					Jobs:       lo.Map(jobs, proto.JobFromJobsdbJob),
				},
			},
		})
		require.NoError(t, err, "it should be able to send chunk request over grpc stream even if server expects metadata")

		_, err = receiveAckWithTimeout(t, stream, ackTimeout)
		require.Error(t, err, "it should receive error after sending a chunk instead of a metadata")
		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.FailedPrecondition, st.Code())

		err = stream.Send(&proto.StreamJobsRequest{
			Payload: &proto.StreamJobsRequest_Metadata{
				Metadata: &proto.JobStreamMetadata{
					MigrationJobId: migrationJobID,
					TablePrefix:    env.jobsdb.Identifier(),
					PartitionIds:   []string{partitionID},
				},
			},
		})
		require.Error(t, err, "it should no longer be able to send requests after an error has occurred on the grpc stream")
		require.ErrorIs(t, err, io.EOF, "error should be an EOF")
	})

	t.Run("stream for an invalid tablePrefix", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Minute)
		defer cancel()
		env := newEnv(t)
		c := config.New()

		pmc, err := client.NewPartitionMigrationClient(env.target, c)
		require.NoError(t, err, "it should be able to create grpc client")
		t.Cleanup(func() {
			_ = pmc.Close()
		})

		stream, err := pmc.StreamJobs(ctx)
		require.NoError(t, err, "it should be able to create grpc stream")

		err = stream.Send(&proto.StreamJobsRequest{
			Payload: &proto.StreamJobsRequest_Metadata{
				Metadata: &proto.JobStreamMetadata{
					MigrationJobId: migrationJobID,
					TablePrefix:    "invalid-table-prefix",
					PartitionIds:   []string{partitionID},
				},
			},
		})
		require.NoError(t, err, "it should be able to send metadata request over grpc stream even if it is for an invalid tablePrefix")

		_, err = receiveAckWithTimeout(t, stream, ackTimeout)
		require.Error(t, err, "it should receive error after sending an invalid tablePrefix for metadata")
		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.FailedPrecondition, st.Code())

		err = stream.Send(&proto.StreamJobsRequest{
			Payload: &proto.StreamJobsRequest_Metadata{
				Metadata: &proto.JobStreamMetadata{
					MigrationJobId: "job-1",
					TablePrefix:    env.jobsdb.Identifier(),
					PartitionIds:   []string{"partition-1"},
				},
			},
		})
		require.Error(t, err, "it should no longer be able to send requests after an error has occurred on the grpc stream")
		require.ErrorIs(t, err, io.EOF, "error should be an EOF")
	})

	t.Run("try to stream while another stream for the same key is running", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Minute)
		defer cancel()
		env := newEnv(t)
		c := config.New()

		pmc, err := client.NewPartitionMigrationClient(env.target, c)
		require.NoError(t, err, "it should be able to create grpc client")
		t.Cleanup(func() {
			_ = pmc.Close()
		})

		stream, err := pmc.StreamJobs(ctx)
		require.NoError(t, err, "it should be able to create grpc stream")

		err = stream.Send(&proto.StreamJobsRequest{
			Payload: &proto.StreamJobsRequest_Metadata{
				Metadata: &proto.JobStreamMetadata{
					MigrationJobId: migrationJobID,
					TablePrefix:    env.jobsdb.Identifier(),
					PartitionIds:   []string{partitionID},
				},
			},
		})
		require.NoError(t, err, "it should be able to send metadata over grpc stream")

		// send one batch to ensure the stream is active
		totalJobs := 10
		jobs := env.generateJobs(t, totalJobs)
		err = stream.Send(&proto.StreamJobsRequest{
			Payload: &proto.StreamJobsRequest_Chunk{
				Chunk: &proto.JobsBatchChunk{
					BatchIndex: 1,
					LastChunk:  true,
					Jobs:       lo.Map(jobs, proto.JobFromJobsdbJob),
				},
			},
		})
		require.NoError(t, err, "it should be able to send jobs over grpc stream")
		ack, err := receiveAckWithTimeout(t, stream, ackTimeout)
		require.NoError(t, err, "it should be able to receive grpc stream response")
		require.EqualValues(t, 1, ack.BatchIndex, "it should receive correct ack from grpc stream")

		{
			pmc2, err := client.NewPartitionMigrationClient(env.target, c)
			require.NoError(t, err, "it should be able to create grpc client")
			t.Cleanup(func() {
				_ = pmc2.Close()
			})
			stream2, err := pmc2.StreamJobs(ctx)
			require.NoError(t, err, "it should be able to create another grpc stream")

			msg := &proto.StreamJobsRequest{
				Payload: &proto.StreamJobsRequest_Metadata{
					Metadata: &proto.JobStreamMetadata{
						MigrationJobId: migrationJobID,
						TablePrefix:    env.jobsdb.Identifier(),
						PartitionIds:   []string{partitionID},
					},
				},
			}
			err = stream2.Send(msg)
			require.NoError(t, err, "it should be able to send metadata over the second grpc stream")

			_, err = receiveAckWithTimeout(t, stream2, ackTimeout)
			require.Error(t, err, "it should receive error on the second grpc stream since another stream is active for the same key")
			st, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, codes.Aborted, st.Code(), "it should receive Aborted error code on the second grpc stream")

		}

		// original stream should still be functional
		totalJobs += 1000
		jobs = env.generateJobs(t, 1000)
		chunks := lo.Chunk(jobs, 10)

		for i, chunk := range chunks {
			req := &proto.StreamJobsRequest{
				Payload: &proto.StreamJobsRequest_Chunk{
					Chunk: &proto.JobsBatchChunk{
						BatchIndex: 2,
						LastChunk:  i == len(chunks)-1,
						Jobs:       lo.Map(chunk, proto.JobFromJobsdbJob),
					},
				},
			}
			err := stream.Send(req)
			require.NoError(t, err, "it should be able to send jobs over grpc stream")
		}
		err = stream.CloseSend()
		require.NoError(t, err, "it should be able to close grpc stream")

		ack, err = receiveAckWithTimeout(t, stream, ackTimeout)
		require.NoError(t, err, "it should be able to receive grpc stream response")
		require.EqualValues(t, 2, ack.BatchIndex, "it should receive correct ack from grpc stream")

		_, err = receiveAckWithTimeout(t, stream, ackTimeout)
		require.Error(t, err, "it should receive error on further recv after stream closed")
		require.ErrorIs(t, err, io.EOF, "error should be an EOF")

		jobsResult, err := env.jobsdb.GetUnprocessed(ctx, jobsdb.GetQueryParams{JobsLimit: totalJobs * 2})
		require.NoError(t, err, "it should be able to fetch jobs from jobsdb")
		require.Len(t, jobsResult.Jobs, totalJobs, "it should have stored all jobs to the target jobsdb")
	})

	t.Run("stream with a target jobsdb that is failing", func(t *testing.T) {
		t.Run("with dedup", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 1*time.Minute)
			defer cancel()
			env := newEnv(t, WithDedupEnabled(true))
			c := config.New()

			pmc, err := client.NewPartitionMigrationClient(env.target, c)
			require.NoError(t, err, "it should be able to create grpc client")
			t.Cleanup(func() {
				_ = pmc.Close()
			})

			stream, err := pmc.StreamJobs(ctx)
			require.NoError(t, err, "it should be able to create grpc stream")

			env.jobsdb.txErr = errors.New("error during tx begin")

			err = stream.Send(&proto.StreamJobsRequest{
				Payload: &proto.StreamJobsRequest_Metadata{
					Metadata: &proto.JobStreamMetadata{
						MigrationJobId: migrationJobID,
						TablePrefix:    env.jobsdb.Identifier(),
						PartitionIds:   []string{partitionID},
					},
				},
			})
			require.NoError(t, err, "it should be able to send metadata over grpc stream")

			_, err = receiveAckWithTimeout(t, stream, ackTimeout)
			require.Error(t, err, "it should receive error when target jobsdb is failing")
			st, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, codes.Unknown, st.Code(), "it should receive Internal error code when target jobsdb is failing")
			require.Equal(t, "loading lastJobID for migration job-1_test: error during tx begin", st.Message(), "error should indicate dedup lastJobID failure")
		})
		t.Run("without dedup", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 1*time.Minute)
			defer cancel()
			env := newEnv(t)
			c := config.New()

			pmc, err := client.NewPartitionMigrationClient(env.target, c)
			require.NoError(t, err, "it should be able to create grpc client")
			t.Cleanup(func() {
				_ = pmc.Close()
			})

			stream, err := pmc.StreamJobs(ctx)
			require.NoError(t, err, "it should be able to create grpc stream")

			err = stream.Send(&proto.StreamJobsRequest{
				Payload: &proto.StreamJobsRequest_Metadata{
					Metadata: &proto.JobStreamMetadata{
						MigrationJobId: migrationJobID,
						TablePrefix:    env.jobsdb.Identifier(),
						PartitionIds:   []string{partitionID},
					},
				},
			})
			require.NoError(t, err, "it should be able to send metadata over grpc stream")
			env.jobsdb.storeErr = errors.New("error during store")

			jobs := env.generateJobs(t, 10)
			err = stream.Send(&proto.StreamJobsRequest{
				Payload: &proto.StreamJobsRequest_Chunk{
					Chunk: &proto.JobsBatchChunk{
						BatchIndex: 1,
						LastChunk:  true,
						Jobs:       lo.Map(jobs, proto.JobFromJobsdbJob),
					},
				},
			})
			require.NoError(t, err, "it should be able to send jobs over grpc stream")

			_, err = receiveAckWithTimeout(t, stream, ackTimeout)
			require.Error(t, err, "it should receive error when target jobsdb is failing")
			st, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, codes.Unknown, st.Code(), "it should receive Internal error code when target jobsdb is failing")
			require.Equal(t, "storing jobs batch 1: storing jobs in test jobsdb: error during store", st.Message(), "error should indicate store failure")
		})
	})

	t.Run("stream without sending anything should cause a receive timeout on server", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Minute)
		defer cancel()
		env := newEnv(t, WithStreamTimeout(config.SingleValueLoader(10*time.Millisecond)))
		c := config.New()

		pmc, err := client.NewPartitionMigrationClient(env.target, c)
		require.NoError(t, err, "it should be able to create grpc client")
		t.Cleanup(func() {
			_ = pmc.Close()
		})

		stream, err := pmc.StreamJobs(ctx)
		require.NoError(t, err, "it should be able to create grpc stream")

		_, err = receiveAckWithTimeout(t, stream, ackTimeout)
		require.Error(t, err, "it should receive error on grpc stream due to server receive timeout")
		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.DeadlineExceeded, st.Code(), "it should receive DeadlineExceeded error code due to server receive timeout")
		require.Equal(t, "receiving stream metadata: rpc error: code = DeadlineExceeded desc = stream receive operation timed out after 10ms", st.Message(), "error should indicate receive timeout")
	})

	t.Run("stream without sending chunks should cause a receive timeout on server", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Minute)
		defer cancel()
		env := newEnv(t, WithStreamTimeout(config.SingleValueLoader(500*time.Millisecond)))
		c := config.New()

		pmc, err := client.NewPartitionMigrationClient(env.target, c)
		require.NoError(t, err, "it should be able to create grpc client")
		t.Cleanup(func() {
			_ = pmc.Close()
		})

		stream, err := pmc.StreamJobs(ctx)
		require.NoError(t, err, "it should be able to create grpc stream")

		err = stream.Send(&proto.StreamJobsRequest{
			Payload: &proto.StreamJobsRequest_Metadata{
				Metadata: &proto.JobStreamMetadata{
					MigrationJobId: migrationJobID,
					TablePrefix:    env.jobsdb.Identifier(),
					PartitionIds:   []string{partitionID},
				},
			},
		})
		require.NoError(t, err, "it should be able to send metadata over grpc stream")

		_, err = receiveAckWithTimeout(t, stream, ackTimeout)
		require.Error(t, err, "it should receive error on grpc stream due to server receive timeout")
		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.DeadlineExceeded, st.Code(), "it should receive DeadlineExceeded error code due to server receive timeout")
		require.Equal(t, "receiving from client stream: rpc error: code = DeadlineExceeded desc = stream receive operation timed out after 500ms", st.Message(), "error should indicate receive timeout")
	})

	t.Run("stream without receiving in time should cause a send timeout on server", func(t *testing.T) {
		// This test validates server-side send timeout, which is tricky to trigger.
		//
		// The challenge: gRPC buffers messages, so we need to fill connection buffers
		// to cause backpressure that prevents the server from sending.
		//
		// Strategy:
		// 1. Use large batch indices (9223372036854000000) to increase message size
		// 2. Client continuously sends without ever calling Recv()
		// 3. Eventually, TCP window fills up and server Send() blocks
		// 4. After 500ms timeout, server returns DeadlineExceeded
		//
		// Note: This test can take more time due to buffer filling time.
		// The "ackedBatches" log helps debug how many batches were processed.
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		defer cancel()
		env := newEnv(t, WithStreamTimeout(config.SingleValueLoader(500*time.Millisecond)))
		c := config.New()

		pmc, err := client.NewPartitionMigrationClient(env.target, c)
		require.NoError(t, err, "it should be able to create grpc client")
		t.Cleanup(func() {
			_ = pmc.Close()
		})

		stream, err := pmc.StreamJobs(ctx)
		require.NoError(t, err, "it should be able to create grpc stream")

		err = stream.Send(&proto.StreamJobsRequest{
			Payload: &proto.StreamJobsRequest_Metadata{
				Metadata: &proto.JobStreamMetadata{
					MigrationJobId: migrationJobID,
					TablePrefix:    env.jobsdb.Identifier(),
					PartitionIds:   []string{partitionID},
				},
			},
		})
		require.NoError(t, err, "it should be able to send metadata over grpc stream")

		batchIndex := int64(9223372036854000000) // use a large batch index to fill buffers faster
		// send one batch to trigger a response from server
		err = stream.Send(&proto.StreamJobsRequest{
			Payload: &proto.StreamJobsRequest_Chunk{
				Chunk: &proto.JobsBatchChunk{
					BatchIndex: batchIndex,
					LastChunk:  true,
					Jobs:       lo.Map(env.generateJobs(t, 1), proto.JobFromJobsdbJob),
				},
			},
		})
		require.NoError(t, err, "it should be able to send jobs over grpc stream")

		require.Eventually(t, func() bool {
			batchIndex++
			err = sendWithTimeout(t, stream, &proto.StreamJobsRequest{
				Payload: &proto.StreamJobsRequest_Chunk{
					Chunk: &proto.JobsBatchChunk{
						BatchIndex: batchIndex,
						LastChunk:  true,
						Jobs:       lo.Map(env.generateJobs(t, 1), proto.JobFromJobsdbJob),
					},
				},
			}, sendTimeout)
			return err != nil
		}, 180*time.Second, 1*time.Millisecond) // we need to wait a lot in order to fill connection windows and buffers
		require.ErrorIs(t, err, errSendTimeout, "client should eventually not be able to send anymore, since connection buffers should be full due to client not receiving")

		// now try to receive acks until we get the timeout error
		ackedBatches := int64(-1)
		for err = nil; err == nil; {
			_, err = receiveAckWithTimeout(t, stream, ackTimeout)
			if err == nil {
				ackedBatches++
			}
		}
		t.Logf("ackedBatches: %d", ackedBatches)
		require.Error(t, err, "it should receive error on grpc stream due to server send timeout")
		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.DeadlineExceeded, st.Code(), "it should receive DeadlineExceeded error code due to server send timeout")
		require.Contains(t, st.Message(), "stream send operation timed out after 500ms", "error should indicate send timeout")

		require.Greater(t, env.getLastJobId(t, migrationJobID, env.jobsdb.Identifier()), int64(0), "it should have kept dedup state for the migration")
	})
}

type testGrpcServerEnv struct {
	target     string      // address of the target grpc server
	sqldb      *sql.DB     // underlying sql.DB
	jobsdb     *mockJobsDB // jobsdb instance used by the grpc server, with mocking capabilities for error reproduction
	grpcServer *GRPCServer // running grpc server
}

// generateJobs creates a list of jobsdb.JobT for testing purposes with increasing JobIDs.
func (testGrpcServerEnv) generateJobs(t *testing.T, count int) []*jobsdb.JobT {
	t.Helper()
	jobs := make([]*jobsdb.JobT, 0, count)
	for i := range count {
		jobs = append(jobs, &jobsdb.JobT{
			UUID:         uuid.New(),
			JobID:        100 + int64(i),
			UserID:       "user-1",
			CreatedAt:    time.Now(),
			ExpireAt:     time.Now(),
			CustomVal:    "test",
			EventCount:   1,
			EventPayload: []byte("{}"),
			Parameters:   []byte(`{"source_id":"source-1", "destination_id":"dest-1"}`),
			WorkspaceId:  "workspace-1",
			PartitionID:  "partition-1",
		})
	}
	return jobs
}

func (e testGrpcServerEnv) getLastJobId(t *testing.T, migrationJobID, tablePrefix string) int64 { // nolint: unparam
	t.Helper()
	var lastJobID int64
	migrationKey := migrationJobID + "_" + tablePrefix
	err := e.sqldb.QueryRowContext(
		t.Context(),
		"SELECT COALESCE((SELECT last_job_id FROM partition_migration_dedup WHERE key = $1), -1)",
		migrationKey,
	).Scan(&lastJobID)
	require.NoError(t, err)
	return lastJobID
}

// runNodeMigration runs the node migrations on the provided database handle.
func runNodeMigration(t *testing.T, db *sql.DB) {
	t.Helper()
	m := &migrator.Migrator{
		Handle:          db,
		MigrationsTable: "node_migrations",
	}
	require.NoError(t, m.Migrate("node"))
}

// mockJobsDB is a wrapper around jobsdb.JobsDB that allows simulating errors during operations.
type mockJobsDB struct {
	jobsdb.JobsDB
	storeErr error
	txErr    error
}

// WithTx overrides the JobsDB's WithTx method to simulate a transaction begin error if txErr is set.
func (m *mockJobsDB) WithTx(fn func(tx *tx.Tx) error) error {
	if m.txErr != nil {
		return m.txErr
	}
	return m.JobsDB.WithTx(fn)
}

// StoreInTx overrides the JobsDB's StoreInTx method to simulate a store error if storeErr is set.
func (m *mockJobsDB) StoreInTx(ctx context.Context, tx jobsdb.StoreSafeTx, jobList []*jobsdb.JobT) error {
	if m.storeErr != nil {
		return m.storeErr
	}
	return m.JobsDB.StoreInTx(ctx, tx, jobList)
}

var errReceiveTimeout = errors.New("receive operation timed out")

// receiveAckWithTimeout attempts to receive a JobsBatchAck from the provided stream within the specified timeout duration.
// If the ack is not received within the timeout, the test fails.
func receiveAckWithTimeout(t *testing.T, stream proto.PartitionMigration_StreamJobsClient, timeout time.Duration) (*proto.JobsBatchAck, error) { // nolint: unparam
	t.Helper()
	ackCh := make(chan lo.Tuple2[*proto.JobsBatchAck, error], 1)
	go func() {
		ackCh <- lo.T2(stream.Recv())
	}()
	select {
	case ack := <-ackCh:
		return ack.A, ack.B
	case <-time.After(timeout):
		return nil, fmt.Errorf("receive operation timed out after %s: %w", timeout.String(), errReceiveTimeout)
	}
}

var errSendTimeout = errors.New("send operation timed out")

func sendWithTimeout(t *testing.T, stream proto.PartitionMigration_StreamJobsClient, req *proto.StreamJobsRequest, timeout time.Duration) error {
	t.Helper()
	errCh := make(chan error, 1)
	go func() {
		errCh <- stream.Send(req)
	}()
	select {
	case err := <-errCh:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("send operation timed out after %s: %w", timeout.String(), errSendTimeout)
	}
}
