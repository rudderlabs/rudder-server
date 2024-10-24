package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/google/uuid"
	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/phayes/freeport"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"

	_ "embed"
	"encoding/json"

	"github.com/rudderlabs/rudder-server/jobsdb"

	"bufio"
)

var (
	duration        time.Duration
	batchSize       int
	eventCounter    int64 // Atomic counter for total events processed
	writeWorkers    int   // Number of write workers
	binaryPayload   bool
	compression     bool
	postgresVersion string // New variable for PostgreSQL version
)

//go:embed init.sql
var initSQL string

//go:embed payload.json
var payloadJSON []byte

var eventPayload []byte

func init() {
	flag.DurationVar(&duration, "duration", 1*time.Minute, "Duration to run the test")
	flag.IntVar(&batchSize, "batch", 10000, "Batch size for storing and reading events")
	flag.IntVar(&writeWorkers, "writers", 2, "Number of write workers")
	flag.BoolVar(&binaryPayload, "binary", false, "Use binary payload")
	flag.BoolVar(&compression, "compression", false, "Enable payload compression")
	flag.StringVar(&postgresVersion, "postgres-version", "15", "PostgreSQL version to use")

	var err error
	eventPayload, err = json.Marshal(json.RawMessage(payloadJSON))
	if err != nil {
		panic(err)
	}
}

type SimpleCleaner struct{}

func (sc *SimpleCleaner) Cleanup(f func())                        { f() }
func (sc *SimpleCleaner) Log(args ...interface{})                 {}
func (sc *SimpleCleaner) Logf(format string, args ...interface{}) {}
func (sc *SimpleCleaner) Failed() bool                            { return false }

func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func main() {
	flag.Parse()

	// Create a new Docker pool
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	// Generate random password
	randomPassword := generateRandomString(16)

	// Find a free port
	freePort, err := freeport.GetFreePort()
	if err != nil {
		log.Fatalf("Could not get free port: %s", err)
	}

	// Create a temporary file for the init script
	tmpfile, err := os.CreateTemp("", "init.sql")
	if err != nil {
		log.Fatalf("Could not create temp file: %s", err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.WriteString(initSQL); err != nil {
		log.Fatalf("Could not write to temp file: %s", err)
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatalf("Could not close temp file: %s", err)
	}

	// Start a PostgreSQL container
	// docker.io/rudderstack/postgres:bitnamiDoremonPocket15
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        postgresVersion, // Use the configurable version
		Env: []string{
			"POSTGRES_PASSWORD=" + randomPassword,
			"POSTGRES_DB=testdb",
		},
		Mounts: []string{
			fmt.Sprintf("%s:/docker-entrypoint-initdb.d/init.sql", tmpfile.Name()),
		},
		Cmd: []string{
			"postgres",
			"-c", "shared_preload_libraries=pg_stat_statements",
		},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"5432/tcp": {{HostIP: "0.0.0.0", HostPort: fmt.Sprintf("%d", freePort)}},
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{
			Name: "no",
		}
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	// Construct the database URL
	dbURL := fmt.Sprintf("postgres://postgres:%s@localhost:%d/testdb?sslmode=disable", randomPassword, freePort)

	// Print the database URL
	fmt.Printf("Database URL: %s\n", dbURL)

	// Create a connection to the database
	var db *sql.DB
	err = pool.Retry(func() error {
		var err error
		db, err = sql.Open("postgres", dbURL)
		if err != nil {
			return err
		}
		return db.Ping()
	})
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	fmt.Println("Successfully connected to database")

	// Ensure the container is removed when we're done
	defer func() {
		if err := pool.Purge(resource); err != nil {
			log.Printf("Could not purge resource: %s", err)
		}
	}()

	c := config.New()
	c.Set("INSTANCE_ID", "1")

	opts := []jobsdb.OptsFunc{
		jobsdb.WithDBHandle(db), jobsdb.WithConfig(c), jobsdb.WithStats(stats.NOP),
	}
	if binaryPayload {
		opts = append(opts, jobsdb.WithBinaryPayload(true))
	}
	if compression {
		opts = append(opts, jobsdb.WithPayloadCompression(true))
	}

	// Use the db connection for your jobsdb
	jobsDB := jobsdb.NewForReadWrite("bench_db", opts...)
	if err := jobsDB.Start(); err != nil {
		panic(err)
	}

	defer jobsDB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	// Create a separate context for signal handling
	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	g, ctx := errgroup.WithContext(ctx)
	start := time.Now()

	// Start multiple store workers
	for i := 0; i < writeWorkers; i++ {
		g.Go(func() error {
			return storeWorker(ctx, jobsDB)
		})
	}

	// Start the read worker
	g.Go(func() error {
		return readWorker(ctx, jobsDB)
	})

	// Start the status update goroutine
	g.Go(func() error {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		lastCount := int64(0)
		lastTime := start

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case t := <-ticker.C:
				currentCount := atomic.LoadInt64(&eventCounter)
				currentTime := t
				duration := currentTime.Sub(lastTime)
				rate := float64(currentCount-lastCount) / duration.Seconds()

				fmt.Printf("[%s] Processed %d events. Current rate: %.2f events/second\n",
					currentTime.Format("15:04:05"), currentCount, rate)

				lastCount = currentCount
				lastTime = currentTime
			}
		}
	})

	// Wait for either the workers to finish or a signal
	select {
	case <-ctx.Done():
		if err := g.Wait(); err != nil && err != context.DeadlineExceeded {
			log.Printf("Error occurred: %v", err)
		}
	case <-sigCtx.Done():
		log.Println("Received termination signal")
		cancel() // Cancel the worker context
		if err := g.Wait(); err != nil && err != context.Canceled {
			log.Printf("Error occurred during shutdown: %v", err)
		}
	}

	elapsed := time.Since(start)
	totalEvents := atomic.LoadInt64(&eventCounter)
	fmt.Printf("\nFinal results:\n")
	fmt.Printf("Processed %d events in %v\n", totalEvents, elapsed)
	fmt.Printf("Average rate: %.2f events/second\n", float64(totalEvents)/elapsed.Seconds())
	fmt.Printf("Database URL: %s\n", dbURL)

	// Wait for user confirmation before exiting
	fmt.Println("\nPress Enter to quit...")
	bufio.NewReader(os.Stdin).ReadString('\n')
}

func storeWorker(ctx context.Context, db jobsdb.JobsDB) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			batch := make([]*jobsdb.JobT, 0, batchSize)
			for j := 0; j < batchSize; j++ {
				job := &jobsdb.JobT{
					UserID:       fmt.Sprintf("user-%d", atomic.AddInt64(&eventCounter, 1)),
					UUID:         uuid.New(),
					Parameters:   []byte(fmt.Sprintf(`{"event_id": %d}`, atomic.LoadInt64(&eventCounter))),
					CustomVal:    "benchmark",
					EventPayload: eventPayload,
				}
				batch = append(batch, job)
			}

			if err := db.Store(ctx, batch); err != nil {
				return fmt.Errorf("failed to store batch: %w", err)
			}
		}
	}
}

func readWorker(ctx context.Context, db jobsdb.JobsDB) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			jobs, err := db.GetUnprocessed(ctx, jobsdb.GetQueryParams{
				JobsLimit: batchSize,
				// ... existing query params ...
			})
			if err != nil {
				return fmt.Errorf("failed to retrieve jobs: %w", err)
			}

			if len(jobs.Jobs) == 0 {
				time.Sleep(10 * time.Millisecond) // Avoid tight loop if no jobs
				continue
			}

			var statusList []*jobsdb.JobStatusT
			for _, job := range jobs.Jobs {

				statusList = append(statusList, &jobsdb.JobStatusT{
					JobID:         job.JobID,
					JobState:      jobsdb.Succeeded.State,
					AttemptNum:    1,
					ExecTime:      time.Now(),
					RetryTime:     time.Now(),
					ErrorCode:     "200",
					ErrorResponse: []byte(`{"success": true}`),
					Parameters:    []byte(`{"event_id": 1}`),
				})
			}

			if err := db.UpdateJobStatus(ctx, statusList, []string{}, []jobsdb.ParameterFilterT{}); err != nil {
				return fmt.Errorf("failed to update job status: %w", err)
			}
		}
	}
}
