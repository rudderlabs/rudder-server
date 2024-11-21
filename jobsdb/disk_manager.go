package jobsdb

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/segmentio/ksuid"
)

type diskManager struct {
	fileReaderMu  *sync.Mutex
	fileReaderMap map[string]fileReaderChan
	maxReadSem    chan struct{}
	maxWriteSem   chan struct{}
	closeTime     time.Duration
	log           logger.Logger
}

func NewDiskManager(conf *config.Config) *diskManager {
	d := &diskManager{
		fileReaderMu:  &sync.Mutex{},
		fileReaderMap: make(map[string]fileReaderChan),
		maxReadSem:    make(chan struct{}, config.GetInt("maxReadSem", 50)),
		maxWriteSem:   make(chan struct{}, config.GetInt("maxWriteSem", 50)),
		closeTime:     config.GetDuration("closeTime", 5, time.Second),
	}
	go d.readRoutine()
	return d
}

func (d *diskManager) readRoutine() {
	ticker := time.NewTicker(10 * d.closeTime)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			d.fileReaderMu.Lock()
			newMap := make(map[string]fileReaderChan)
			for fileName, fileReadChan := range d.fileReaderMap {
				if fileReadChan.closed.Load() {
					close(fileReadChan.readerChan)
					delete(d.fileReaderMap, fileName)
					continue
				}
				newMap[fileName] = fileReadChan
			}
			d.fileReaderMap = newMap
			d.fileReaderMu.Unlock()
		}
	}
}

func (d *diskManager) Read(fileName string, length, offset int) ([]byte, error) {
	d.fileReaderMu.Lock()
	if _, ok := fileReaderMap[fileName]; !ok {
		fileReadChan := fileReaderChan{readerChan: make(chan readRequest), closed: &atomic.Bool{}}
		fileReaderMap[fileName] = fileReadChan
		go readFromFile(fileName, fileReadChan, d.closeTime)
	} else {
		if fileReaderMap[fileName].closed.Load() {
			fileReaderMap[fileName].closed.Store(false)
			go readFromFile(fileName, fileReaderMap[fileName], d.closeTime)
		}
	}
	readRequestChan := fileReaderMap[fileName].readerChan
	d.fileReaderMu.Unlock()
	responseChan := make(chan payloadOrError)
	defer close(responseChan)
	readRequestChan <- readRequest{offset: offset, length: length, response: responseChan}
	response := <-responseChan
	if response.err != nil {
		return nil, response.err
	}
	return response.payload, nil
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
		go readFromFile(fileName, fileReadChan, 5*time.Second)
	} else {
		if fileReaderMap[fileName].closed.Load() {
			fileReadChan := fileReaderChan{readerChan: make(chan readRequest), closed: &atomic.Bool{}}
			fileReaderMap[fileName] = fileReadChan
			go readFromFile(fileName, fileReadChan, 5*time.Second)
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

func readFromFile(fileName string, fileReadChan fileReaderChan, closeTime time.Duration) {
	defer fileReadChan.closed.Store(true)
	closeTimer := time.NewTicker(closeTime)
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
			closeTimer.Reset(closeTime)
		case <-closeTimer.C:
			return
		}
	}
}
