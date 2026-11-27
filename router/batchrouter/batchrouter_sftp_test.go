package batchrouter

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/sshserver"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/rsources"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	sftpTestUser     = "rudder"
	sftpTestPassword = "password"
)

// TestSFTP delivers records to a real SFTP server, with and without async workers.
func TestSFTP(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pg, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	require.NoError(t, (&migrator.Migrator{Handle: pg.DB, MigrationsTable: "node_migrations"}).Migrate("node"))
	server, err := sshserver.Setup(pool, t, sshserver.WithCredentials(sftpTestUser, sftpTestPassword))
	require.NoError(t, err)

	for _, asyncWorker := range []bool{true, false} {
		t.Run(fmt.Sprintf("async worker: %t", asyncWorker), func(t *testing.T) {
			// only async workers keep the order of the records of a sync across files
			requireIDs := func(t *testing.T, expected, actual []string) {
				t.Helper()
				if asyncWorker {
					require.Equal(t, expected, actual)
				} else {
					require.ElementsMatch(t, expected, actual)
				}
			}

			t.Run("a sync that doesn't fit in a file is split in numbered files with the same path, and the next sync starts over", func(t *testing.T) {
				env := newSFTPTestEnv(t, pool, pg, server, asyncWorker, "json", "/tmp/{destinationID}/{jobRunID}-{timestampInMS}.json")
				for _, run := range []string{"run-1", "run-2"} {
					env.storeRecords(run, 40)
					env.requireRecordsInState(run, jobsdb.Succeeded.State, 40)

					files := env.remoteFiles(run)
					require.Greater(t, len(files), 1, "the records of a sync don't fit in one file")
					require.Less(t, len(files), 20, "files aren't limited by maxEventsInABatch")
					prefix := strings.TrimSuffix(files[0], "_1.json")
					var ids []string
					for i, file := range files {
						require.Equal(t, fmt.Sprintf("%s_%d.json", prefix, i+1), file)
						var records []sftpTestRecord
						require.NoError(t, jsonrs.Unmarshal(env.remoteFile(file), &records))
						for _, record := range records {
							require.Equal(t, "insert", record.Message.Action)
							ids = append(ids, record.Message.Fields["id"])
						}
					}
					requireIDs(t, recordIDs(run, 40), ids)
				}
			})

			t.Run("csv", func(t *testing.T) {
				env := newSFTPTestEnv(t, pool, pg, server, asyncWorker, "csv", "/tmp/{destinationID}/{jobRunID}.csv")
				env.storeRecords("run-1", 40)
				env.requireRecordsInState("run-1", jobsdb.Succeeded.State, 40)

				var ids []string
				for _, file := range env.remoteFiles("run-1") {
					rows, err := csv.NewReader(bytes.NewReader(env.remoteFile(file))).ReadAll()
					require.NoError(t, err)
					require.Equal(t, []string{"action", "id", "name"}, rows[0])
					for _, row := range rows[1:] {
						require.Equal(t, []string{"insert", row[1], "name of " + row[1]}, row)
						ids = append(ids, row[1])
					}
				}
				requireIDs(t, recordIDs("run-1", 40), ids)
			})

			t.Run("records that the server rejects are aborted", func(t *testing.T) {
				env := newSFTPTestEnv(t, pool, pg, server, asyncWorker, "json", "/root/{destinationID}/{jobRunID}.json")
				env.storeRecords("run-1", 5)
				env.requireRecordsInState("run-1", jobsdb.Aborted.State, 5)
			})
		})
	}
}

type sftpTestRecord struct {
	Message struct {
		Action string            `json:"action"`
		Fields map[string]string `json:"fields"`
	} `json:"message"`
}

type sftpTestEnv struct {
	t             *testing.T
	pool          *dockertest.Pool
	pg            *postgres.Resource
	server        *sshserver.Resource
	db            jobsdb.JobsDB
	tablePrefix   string
	destinationID string
}

// newSFTPTestEnv starts a batch router with one SFTP destination
func newSFTPTestEnv(t *testing.T, pool *dockertest.Pool, pg *postgres.Resource, server *sshserver.Resource, asyncWorker bool, fileFormat, filePath string) *sftpTestEnv {
	misc.Init()
	config.Reset()
	t.Cleanup(config.Reset)
	config.Set("BatchRouter.SFTP.asyncWorker.enabled", asyncWorker)
	config.Set("BatchRouter.asyncUploadTimeout", "200ms")
	config.Set("BatchRouter.asyncUploadWorkerTimeout", "50ms")
	config.Set("BatchRouter.maxEventsInABatch", 2) // doesn't apply to SFTP
	config.Set("BatchRouter.maxPayloadSizeInBytes", 1000)
	config.Set("BatchRouter.minIdleSleep", "1ms")
	config.Set("BatchRouter.uploadFreq", "10ms")
	config.Set("BatchRouter.pingFrequency", "10ms")
	config.Set("Router.jobRetention", "175200h")

	env := &sftpTestEnv{
		t:             t,
		pool:          pool,
		pg:            pg,
		server:        server,
		tablePrefix:   fmt.Sprintf("brt_%d", time.Now().UnixNano()),
		destinationID: "destination-" + uuid.NewString(),
	}
	db := jobsdb.NewForReadWrite(env.tablePrefix, jobsdb.WithDBHandle(pg.DB), jobsdb.WithStats(stats.NOP))
	require.NoError(t, db.Start())
	t.Cleanup(db.TearDown)
	env.db = db

	destination := backendconfigtest.NewDestinationBuilder("SFTP").WithID(env.destinationID).
		WithConfigOption("authMethod", "passwordAuth").
		WithConfigOption("username", sftpTestUser).
		WithConfigOption("password", sftpTestPassword).
		WithConfigOption("host", "localhost").
		WithConfigOption("port", strconv.Itoa(server.Port)).
		WithConfigOption("fileFormat", fileFormat).
		WithConfigOption("filePath", filePath).
		WithConfigOption("sortColumnNames", true).
		Build()
	source := backendconfigtest.NewSourceBuilder().WithID("source-1").WithConnection(destination).Build()
	brt := &Handle{}
	brt.Setup("SFTP", backendconfigtest.NewStaticLibrary(map[string]backendconfig.ConfigT{
		"workspace-1": backendconfigtest.NewConfigBuilder().WithWorkspaceID("workspace-1").WithSource(source).Build(),
	}), db, nil, transientsource.NewEmptyService(), rsources.NewNoOpService(), rsources.NewStaticSyncSettingDelegate("", nil),
		destinationdebugger.NewNoOpService(), config.Default)
	brt.Start()
	t.Cleanup(brt.Shutdown)
	return env
}

// recordIDs returns the IDs of the records of a sync, in the order they are stored
func recordIDs(jobRunID string, n int) []string {
	return lo.Times(n, func(i int) string { return fmt.Sprintf("%s-%d", jobRunID, i) })
}

func (env *sftpTestEnv) storeRecords(jobRunID string, n int) {
	jobs := lo.Map(recordIDs(jobRunID, n), func(id string, _ int) *jobsdb.JobT {
		return &jobsdb.JobT{
			UUID:         uuid.New(),
			UserID:       "user",
			WorkspaceId:  "workspace-1",
			EventPayload: fmt.Appendf(nil, `{"type":"record","action":"insert","fields":{"id":%q,"name":"name of %s"}}`, id, id),
			Parameters: fmt.Appendf(nil, `{"source_id":"source-1","destination_id":%q,"source_job_run_id":%q,"received_at":%q}`,
				env.destinationID, jobRunID, time.Now().Format(misc.RFC3339Milli)),
			CustomVal: "SFTP",
			CreatedAt: time.Now(),
			ExpireAt:  time.Now(),
		}
	})
	require.NoError(env.t, env.db.Store(context.Background(), jobs))
}

// requireRecordsInState waits until the given number of records of a sync have the given state
func (env *sftpTestEnv) requireRecordsInState(jobRunID, state string, n int) {
	env.t.Helper()
	require.Eventually(env.t, func() bool {
		var count int
		err := env.pg.DB.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %[1]q j
			JOIN (SELECT DISTINCT ON (job_id) job_id, job_state FROM %[2]q ORDER BY job_id, id DESC) s ON s.job_id = j.job_id
			WHERE j.parameters->>'source_job_run_id' = $1 AND s.job_state = $2`, env.tablePrefix+"_jobs_1", env.tablePrefix+"_job_status_1"),
			jobRunID, state).Scan(&count)
		return err == nil && count == n
	}, 60*time.Second, 50*time.Millisecond)
}

// remoteFiles returns the paths of the files of a sync, in the order they were numbered
func (env *sftpTestEnv) remoteFiles(jobRunID string) []string {
	out := env.run(fmt.Sprintf("ls -v /tmp/%s/%s*", env.destinationID, jobRunID))
	return strings.Fields(string(out))
}

func (env *sftpTestEnv) remoteFile(name string) []byte {
	return env.run("cat " + name)
}

// run runs a command on the server
func (env *sftpTestEnv) run(command string) []byte {
	env.t.Helper()
	containers, err := env.pool.Client.ListContainers(dc.ListContainersOptions{})
	require.NoError(env.t, err)
	container, ok := lo.Find(containers, func(c dc.APIContainers) bool {
		return lo.ContainsBy(c.Ports, func(p dc.APIPort) bool { return p.PublicPort == int64(env.server.Port) })
	})
	require.True(env.t, ok, "the server's container is running")
	exec, err := env.pool.Client.CreateExec(dc.CreateExecOptions{Container: container.ID, Cmd: []string{"sh", "-c", command}, AttachStdout: true, AttachStderr: true})
	require.NoError(env.t, err)
	var stdout, stderr bytes.Buffer
	require.NoError(env.t, env.pool.Client.StartExec(exec.ID, dc.StartExecOptions{OutputStream: &stdout, ErrorStream: &stderr}))
	result, err := env.pool.Client.InspectExec(exec.ID)
	require.NoError(env.t, err)
	require.Zero(env.t, result.ExitCode, stderr.String())
	return stdout.Bytes()
}
