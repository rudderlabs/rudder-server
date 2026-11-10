package batchrouter

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
)

// asyncBatchLimits are the limits of a batch. A batch without a jobs limit is limited only by its bytes.
type asyncBatchLimits struct {
	jobs  int
	bytes int
}

// asyncBatch holds the jobs that a worker has taken but not uploaded yet. Its jobs are executing, and each one is a
// line in the upload file.
type asyncBatch struct {
	destinationID string
	limits        asyncBatchLimits
	jobs          []*jobsdb.JobT
	size          int
	full          bool // it takes no more jobs
	fileName      string
	file          *os.File // the upload file, while it is open
	createdAt     time.Time
}

func (b *asyncBatch) empty() bool {
	return len(b.jobs) == 0
}

// ready returns true if the batch should be uploaded: it is full, or older than the upload interval
func (b *asyncBatch) ready(uploadInterval time.Duration) bool {
	return !b.empty() && (b.full || time.Since(b.createdAt) >= uploadInterval)
}

// add adds a job to the batch, or returns false if the job doesn't fit, which leaves the batch full. A batch always
// takes its first job, even if the job is larger than the limit, so that one large job never stops a destination.
func (b *asyncBatch) add(job *jobsdb.JobT, line string) (bool, error) {
	if b.full || !b.empty() && b.size+len(line) > b.limits.bytes {
		b.full = true
		return false, nil
	}
	if b.empty() {
		if err := b.nameFile(); err != nil {
			return false, fmt.Errorf("naming the upload file: %w", err)
		}
		b.createdAt = timeutil.Now()
	}
	if b.file == nil {
		var err error
		if b.file, err = os.OpenFile(b.fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600); err != nil {
			return false, fmt.Errorf("opening the upload file: %w", err)
		}
	}
	if _, err := b.file.WriteString(line); err != nil {
		return false, fmt.Errorf("writing the upload file: %w", err)
	}
	b.jobs = append(b.jobs, job)
	b.size += len(line)
	b.full = b.size >= b.limits.bytes || (b.limits.jobs > 0 && len(b.jobs) >= b.limits.jobs)
	return true, nil
}

func (b *asyncBatch) nameFile() error {
	tmpDirPath, err := misc.GetTmpDir()
	if err != nil {
		return err
	}
	b.fileName = filepath.Join(tmpDirPath, misc.RudderAsyncDestinationLogs, fmt.Sprintf("%s.%s.txt", b.destinationID, uuid.New().String()))
	return os.MkdirAll(filepath.Dir(b.fileName), os.ModePerm)
}

// closeFile closes the upload file. Adding a job opens it again.
func (b *asyncBatch) closeFile() {
	if b.file != nil {
		_ = b.file.Close()
		b.file = nil
	}
}

func (b *asyncBatch) removeFile() {
	b.closeFile()
	misc.RemoveFilePaths(b.fileName)
}
