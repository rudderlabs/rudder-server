package jobsdb

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/segmentio/ksuid"
)

type diskManager struct {
	fileReaderMu  *sync.Mutex
	fileReaderMap map[string]fileReaderChan
	maxReadSem    chan struct{}
	maxWriteSem   chan struct{}
	log           logger.Logger
}

func NewDiskManager() *diskManager {
	return &diskManager{
		fileReaderMu:  &sync.Mutex{},
		fileReaderMap: make(map[string]fileReaderChan),
		maxReadSem:    make(chan struct{}, 50),
		maxWriteSem:   make(chan struct{}, 50),
	}
}

func (d *diskManager) WriteToFile(jobs []*JobT) (string, error) {
	d.maxWriteSem <- struct{}{}
	defer func() { <-d.maxWriteSem }()
	var fileName string

	if len(jobs) == 0 {
		return "", nil
	}
	tempDir, err := misc.CreateTMPDIR()

	fileName = fmt.Sprintf("%s/jobs_%s", tempDir, ksuid.New().String())
	_, err = os.Stat(fileName)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", fmt.Errorf("stat file - %s: %w", fileName, err)
		}
	}
	file, err := os.Create(fileName)
	if err != nil {
		return "", fmt.Errorf("create file - %s: %w", fileName, err)
	}

	deferredFuncs := []func() bool{}
	defer func() {
		for _, function := range deferredFuncs {
			if !function() {
				return
			}
		}
	}()
	buffer := bufio.NewWriter(file)
	deferredFuncs = append(deferredFuncs, func() bool {
		if err := buffer.Flush(); err != nil {
			d.log.Errorn("error flushing buffer", logger.NewStringField("file", fileName), logger.NewErrorField(err))
			return false
		}
		if err := file.Close(); err != nil {
			d.log.Errorn("error closing file", logger.NewStringField("file", fileName), logger.NewErrorField(err))
			return false
		}
		return true
	})

	offset := 0
	for i := range jobs {
		length, err := buffer.Write(jobs[i].EventPayload)
		if err != nil {
			return "", fmt.Errorf("write job payload to buffer - %s: %w", fileName, err)
		}
		params := bytes.TrimSuffix(jobs[i].Parameters, []byte("}"))
		params = append(params, []byte(fmt.Sprintf(`,"payload_file":"%s","offset": %d,"length":%d}`, fileName, offset, length))...)
		jobs[i].Parameters = params
		deferredFuncs = append(deferredFuncs, func() bool {
			jobs[i].EventPayload = []byte(`{}`)
			return true
		})
		offset += length
	}
	return fileName, nil
}

// WriteToFile writes the payloads of the jobs to a new file
// and updates the job parametets with fileName, offset and length
//
// # Updates jobs in place
//
// returns the file name
func WriteToFile(jobs []*JobT) (string, error) {
	var fileName string

	if len(jobs) == 0 {
		return "", nil
	}
	tempDir, err := misc.CreateTMPDIR()

	fileName = fmt.Sprintf("%s/jobs_%s", tempDir, ksuid.New().String())
	_, err = os.Stat(fileName)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", fmt.Errorf("stat file - %s: %w", fileName, err)
		}
	}
	file, err := os.Create(fileName)
	if err != nil {
		return "", fmt.Errorf("create file - %s: %w", fileName, err)
	}

	deferredFuncs := []func() bool{}
	defer func() {
		for _, function := range deferredFuncs {
			if !function() {
				return
			}
		}
	}()
	deferredFuncs = append(deferredFuncs, func() bool {
		if err := file.Close(); err != nil {
			return false
		}
		return true
	})
	offset := 0
	for i := range jobs {
		length, err := file.Write(jobs[i].EventPayload)
		if err != nil {
			return "", fmt.Errorf("write job payload to file - %s: %w", fileName, err)
		}
		params := bytes.TrimSuffix(jobs[i].Parameters, []byte("}"))
		params = append(params, []byte(fmt.Sprintf(`,"payload_file":"%s","offset": %d,"length":%d}`, fileName, offset, length))...)
		jobs[i].Parameters = params
		deferredFuncs = append(deferredFuncs, func() bool {
			jobs[i].EventPayload = []byte(`{}`)
			return true
		})
		offset += length
	}
	return fileName, nil
}

var fileReaderMu = &sync.Mutex{}
var fileReaderMap = make(map[string]fileReaderChan)
var fileWriterMap = make(map[string]chan []byte)

type fileReaderChan struct {
	readerChan chan readRequest
	closed     *atomic.Bool
}

type readRequest struct {
	offset, length int
	response       chan payloadOrError
}
type payloadOrError struct {
	payload []byte
	err     error
}

// ConcurrentReadFromFile allows user to channel reads to file.
// It keeps the file open and reads the file in a separate go routine
//
// It closes the file after 5 seconds of inactivity or when a read fails due to any reason
func ConcurrentReadFromFile(fileName string, offset, length int) ([]byte, error) {
	fileReaderMu.Lock()
	if _, ok := fileReaderMap[fileName]; !ok {
		fileReadChan := fileReaderChan{readerChan: make(chan readRequest), closed: &atomic.Bool{}}
		fileReaderMap[fileName] = fileReadChan
		go readFromFile(fileName, fileReadChan)
	} else {
		if fileReaderMap[fileName].closed.Load() {
			fileReadChan := fileReaderChan{readerChan: make(chan readRequest), closed: &atomic.Bool{}}
			fileReaderMap[fileName] = fileReadChan
			go readFromFile(fileName, fileReadChan)
		}
	}
	readRequestChan := fileReaderMap[fileName].readerChan
	fileReaderMu.Unlock()
	responseChan := make(chan payloadOrError)
	defer close(responseChan)
	readRequestChan <- readRequest{offset: offset, length: length, response: responseChan}
	response := <-responseChan
	if response.err != nil {
		return nil, response.err
	}
	return response.payload, nil
}

func readFromFile(fileName string, fileReadChan fileReaderChan) {
	closeTimer := time.NewTicker(5 * time.Second)
	defer closeTimer.Stop()
	file, err := os.Open(fileName)
	if err != nil {
		panic(fmt.Errorf("open file - %s: %w", fileName, err))
	}
	defer file.Close()
	for {
		select {
		case request := <-fileReadChan.readerChan:
			payload := make([]byte, request.length)
			_, err = file.ReadAt(payload, int64(request.offset))
			if err != nil {
				request.response <- payloadOrError{err: fmt.Errorf("read file - %s - %d: %w", fileName, request.offset, err)}
				return
			}
			request.response <- payloadOrError{payload: payload, err: err}
			closeTimer.Reset(5 * time.Second)
		case <-closeTimer.C:
			return
		}
	}
}
