package scylla

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	_ "go.uber.org/automaxprocs"

	"github.com/google/uuid"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/profiler"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/metric"
)

var (
	keySpace                = "scylla"
	loadTable               = "dedup"
	defaultHistogramBuckets = []float64{
		0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 1, 2.5, 5, 10, 60,
	}
)

type DB struct {
	pg       *sql.DB
	scylla   *gocql.Session
	conf     *config.Config
	stat     stats.Stats
	timeout  time.Duration
	ttl      int
	sampling float64
}

func (d *DB) PopulateBatch() (time.Duration, error) {
	startTime := time.Now()
	err := d.SetupPGTable()
	if err != nil {
		return 0, err
	}
	if err := d.scylla.Query(fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s with replication = {'class': 'SimpleStrategy', 'replication_factor': 3}", keySpace)).Exec(); err != nil {
		return 0, err
	}
	fmt.Println("Created Keyspace")
	tableName := fmt.Sprintf("%s.%s", keySpace, loadTable)
	if err := d.scylla.Query(fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
				id uuid PRIMARY KEY) WITH bloom_filter_fp_chance = 0.005 and default_time_to_live = %d;`, tableName, d.ttl,
	)).Exec(); err != nil {
		return 0, err
	}
	fmt.Println("Created Table")
	totalWorkspaces := d.conf.GetInt("ScyllaDB.totalWorkspaces", 10)
	batchSize := d.conf.GetInt("ScyllaDB.batchSize", 20)
	pgBatchSize := d.conf.GetInt("DB.pgBatchSize", 10)
	sample := int(1/d.sampling) + 1
	pgBatch := make([]string, 0, pgBatchSize)
	var totalCount atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < totalWorkspaces; i++ {
		fmt.Println("Starting workspace", i)
		errorStat := d.stat.NewTaggedStat("scylla_batch_write_error", stats.CountType, stats.Tags{})
		pgErrorStat := d.stat.NewTaggedStat("pg_batch_write_error", stats.CountType, stats.Tags{})
		queryStat := d.stat.NewTaggedStat("scylla_batch_write_stat", stats.TimerType, stats.Tags{})
		countStat := d.stat.NewTaggedStat("scylla_batch_write_count", stats.CountType, stats.Tags{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			totalParallelGoRoutines := d.conf.GetInt("ScyllaDB.totalParallelGoRoutines", 1)
			for j := 0; j < totalParallelGoRoutines; j++ {
				batch := d.scylla.NewBatch(gocql.LoggedBatch).WithContext(context.Background())
				fmt.Println("Starting parallel go routine", j)
				go func() {
					for {
						time.Sleep(4 * time.Millisecond)
						uid := uuid.New().String()
						batch.Entries = append(batch.Entries, gocql.BatchEntry{
							Stmt: fmt.Sprintf("INSERT INTO %s.%s (id) VALUES (?)", keySpace, loadTable),
							Args: []interface{}{uid},
						})
						totalCount.Add(1)
						time := time.Now()
						if len(batch.Entries) == batchSize {
							if err := d.scylla.ExecuteBatch(batch); err != nil {
								fmt.Printf("failed executing batch into scylla with %v and batch size %d", err, batch.Size())
								errorStat.Increment()
								continue
							}
							countStat.Count(len(batch.Entries))
							batch = d.scylla.NewBatch(gocql.LoggedBatch).WithContext(context.Background())
						}
						queryStat.Since(time)
						if rand.IntN(sample)%sample == 0 {
							pgBatch = append(pgBatch, uid)
						}
						if len(pgBatch) == pgBatchSize {
							if err := d.InsertPG(pgBatch); err != nil {
								fmt.Printf("Failed inserting into PG with %v", err)
								pgErrorStat.Increment()
								continue
							}
							pgBatch = nil
						}
					}
				}()
			}
		}()
		if len(pgBatch) > 0 {
			if err := d.InsertPG(pgBatch); err != nil {
				return 0, err
			}
		}
	}
	wg.Wait()
	return time.Since(startTime), nil
}

func (d *DB) SetupPGTable() error {
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
	defer cancel()
	tx, err := d.pg.Begin()
	if err != nil {
		return err
	}
	createTableStmt := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
			id uuid PRIMARY KEY);`, loadTable,
	)
	if _, err := tx.ExecContext(ctx, createTableStmt); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (d *DB) InsertPG(keys []string) error {
	tx, err := d.pg.Begin()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
	defer cancel()
	stmt, err := tx.PrepareContext(ctx, pq.CopyIn(loadTable, "id"))
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	for _, key := range keys {
		if _, err = stmt.ExecContext(ctx, key); err != nil {
			return err
		}
	}
	if _, err = stmt.ExecContext(ctx); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) BenchmarkNewKeys() {
	time.Sleep(1 * time.Minute)
	totalWorkspaces := d.conf.GetInt("ScyllaDB.totalWorkspaces", 10)
	totalParallelGoRoutines := d.conf.GetInt("ScyllaDB.totalParallelGoRoutines", 10)
	newKeyStat := d.stat.NewTaggedStat("new_keys_read_stat_count", stats.CountType, stats.Tags{})
	var totalCount atomic.Int64
	wg := sync.WaitGroup{}
	newReadErrorStat := d.stat.NewTaggedStat("new_keys_read_error", stats.CountType, stats.Tags{})
	for {
		for i := 0; i < totalWorkspaces; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				wg1 := sync.WaitGroup{}
				for j := 0; j < totalParallelGoRoutines; j++ {
					wg1.Add(1)
					go func() {
						defer wg1.Done()
						time.Sleep(400 * time.Millisecond)
						queryStart := time.Now()
						queryStat := d.stat.NewTaggedStat("new_keys_stat_timer", stats.TimerType, stats.Tags{})
						uid := uuid.New().String()
						var key string
						err := d.scylla.Query("SELECT id FROM scylla.dedup WHERE id = ?", uid).Scan(&key)
						if err != nil && err != gocql.ErrNotFound {
							fmt.Println("Error closing iterator with", err)
						}
						if key != "" {
							newReadErrorStat.Increment()
							fmt.Println("key exists")
						}
						queryStat.Since(queryStart)
						totalCount.Add(1)
						newKeyStat.Increment()
					}()
					wg1.Wait()
					time.Sleep(25 * time.Millisecond)
				}
			}()
			time.Sleep(10 * time.Millisecond)
		}
		fmt.Println("Total New Keys Read", totalCount.Load())
	}
}

func (d *DB) BenchmarkExistingKeys() {
	time.Sleep(1 * time.Minute)
	totalKeysToRead := d.conf.GetInt("ScyllaDB.totalKeysToMigrate", 12100)
	sqlQuery := fmt.Sprintf("SELECT id FROM %s limit %d", loadTable, totalKeysToRead)
	readKeyStat := d.stat.NewTaggedStat("read_keys_stat", stats.CountType, stats.Tags{})
	var totalCount atomic.Int64
	existingReadErrorStat := d.stat.NewTaggedStat("existing_keys_read_error", stats.CountType, stats.Tags{})
	for {
		rows, err := d.pg.Query(sqlQuery)
		if err != nil {
			existingReadErrorStat.Increment()
			continue
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				fmt.Println("Error scanning row", err)
				existingReadErrorStat.Increment()
				continue
			}
			var key string
			queryStart := time.Now()
			queryStat := d.stat.NewTaggedStat("existing_keys_stat", stats.TimerType, stats.Tags{})

			err := d.scylla.Query("SELECT id FROM scylla.dedup WHERE id = ?", id).Scan(&key)
			if err == gocql.ErrNotFound {
				fmt.Println("key does not exist", id)
				existingReadErrorStat.Increment()
				continue
			}
			if err != nil {
				fmt.Println("Error getting Existing release ", err)
				existingReadErrorStat.Increment()
				continue
			}
			if key == "" {
				existingReadErrorStat.Increment()
				fmt.Println("key does not exist", id)
			}
			queryStat.Since(queryStart)
			totalCount.Add(1)
			readKeyStat.Increment()
		}
		_ = rows.Close()
		if totalCount.Load() == 0 {
			time.Sleep(2 * time.Minute)
		}
		fmt.Println("Total Existing Keys Read", totalCount.Load())
	}
}

func (d *DB) Destroy() (time.Duration, error) {
	startTime := time.Now()
	err := d.scylla.Query(fmt.Sprintf("DROP TABLE  %s.%s", keySpace, loadTable)).Exec()
	if err != nil {
		return 0, err
	}
	err = d.scylla.Query(fmt.Sprintf("DROP KEYSPACE %s", keySpace)).Exec()
	if err != nil {
		return 0, err
	}
	return time.Since(startTime), nil
}

func Start(runningMode string) error {
	s, err := New()
	if err != nil {
		return err
	}
	switch runningMode {
	case "populate-batch":
		_, err := s.PopulateBatch()
		fmt.Println("Error while populating batch: ", err)
	case "benchmark-existing-keys":
		s.BenchmarkExistingKeys()
	case "benchmark-new-keys":
		s.BenchmarkNewKeys()
	case "populate-and-benchmark":
		wg := sync.WaitGroup{}
		wg.Add(3)
		go func() {
			_, err := s.PopulateBatch()
			fmt.Println("Error while populating batch: ", err)
			wg.Done()
		}()
		go func() {
			s.BenchmarkExistingKeys()
			wg.Done()
		}()
		go func() {
			s.BenchmarkNewKeys()
			wg.Done()
		}()
		wg.Wait()
	default:
		_, err := s.Destroy()
		fmt.Printf("Error while destroying: %v", err)
	}
	return nil
}

func New() (*DB, error) {
	conf := config.New(config.WithEnvPrefix("BENCHMARK"))
	statsOptions := stats.WithDefaultHistogramBuckets(defaultHistogramBuckets)
	stats.Default = stats.NewStats(conf, logger.NewFactory(conf), metric.NewManager(), statsOptions)
	err := stats.Default.Start(context.TODO(), GoRoutineFactory)
	if err != nil {
		return nil, err
	}
	cluster := gocql.NewCluster(conf.GetString("Scylla.Hosts", "localhost:9042"))
	cluster.Consistency = gocql.Quorum

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("postgres", getConnectionString(conf))
	if err != nil {
		return nil, err
	}

	go func() {
		_ = profiler.StartServer(context.TODO(), conf.GetInt("Profiler.Port", 7777))
	}()

	return &DB{
		pg:       db,
		scylla:   session,
		conf:     conf,
		stat:     stats.Default,
		timeout:  conf.GetDuration("Scylla.Timeout", 10, time.Second),
		ttl:      conf.GetInt("Scylla.TTL", 1209600),
		sampling: conf.GetFloat64("DB.sampling", 0.00001),
	}, nil
}

func getConnectionString(config *config.Config) string {
	host := config.GetString("DB.host", "localhost")
	user := config.GetString("DB.user", "rudder")
	dbname := config.GetString("DB.name", "jobsdb")
	port := config.GetInt("DB.port", 6432)
	password := config.GetString("DB.password", "password")
	sslmode := config.GetString("DB.sslMode", "disable")
	idleTxTimeout := config.GetDuration("DB.IdleTxTimeout", 5, time.Minute)

	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=%s "+
		" options='-c idle_in_transaction_session_timeout=%d'",
		host, port, user, password, dbname, sslmode,
		idleTxTimeout.Milliseconds(),
	)
}

func Go(function func()) {
	go func() {
		function()
	}()
}

func GoForWarehouse(function func()) {
	go func() {
		function()
	}()
}

var GoRoutineFactory goRoutineFactory

type goRoutineFactory struct{}

func (goRoutineFactory) Go(function func()) {
	Go(function)
}
