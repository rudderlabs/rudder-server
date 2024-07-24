package scylla

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"

	"github.com/google/uuid"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

var (
	keySpace  = "scylla"
	loadTable = "dedup"
)

type scyllaDB struct {
	scylla  *gocql.Session
	conf    *config.Config
	stat    stats.Stats
	timeout time.Duration
	ttl     int
}

func (d *scyllaDB) PopulateBatch() (time.Duration, error) {
	startTime := time.Now()
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
	var totalCount atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < totalWorkspaces; i++ {
		fmt.Println("Starting workspace", i)
		errorStat := d.stat.NewTaggedStat("scylla_batch_write_error", stats.CountType, stats.Tags{})
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
						if len(batch.Entries) == batchSize {
							if err := d.scylla.ExecuteBatch(batch); err != nil {
								fmt.Printf("failed executing batch into scylla with %v and batch size %d", err, batch.Size())
								errorStat.Increment()
								continue
							}
							countStat.Count(len(batch.Entries))
							batch = d.scylla.NewBatch(gocql.LoggedBatch).WithContext(context.Background())
						}
					}
				}()
			}
		}()
	}
	wg.Wait()
	return time.Since(startTime), nil
}

func (d *scyllaDB) BenchmarkNewKeys() {
	var key string
	err := d.scylla.Query("SELECT id FROM scylla.dedup WHERE id = ?", uuid.New().String()).Scan(&key)
	if err != nil && err != gocql.ErrNotFound {
		fmt.Println("Error closing iterator with", err)
	}
	if key != "" {
		fmt.Println("key exists")
	}
}

func New(conf *config.Config, stats stats.Stats) (*scyllaDB, error) {
	cluster := gocql.NewCluster(conf.GetString("Scylla.Hosts", "localhost:9042"))
	cluster.Consistency = gocql.Quorum

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &scyllaDB{
		scylla:  session,
		conf:    conf,
		stat:    stats,
		timeout: conf.GetDuration("Scylla.Timeout", 10, time.Second),
		ttl:     conf.GetInt("Scylla.TTL", 1209600),
	}, nil
}
