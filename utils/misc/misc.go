package misc

import (
	"archive/zip"
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"

	//"runtime/debug"
	"time"

	"github.com/bugsnag/bugsnag-go"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

//AssertError panics if error
func AssertError(err error) {
	if err != nil {
		//debug.SetTraceback("all")
		//debug.PrintStack()
		defer bugsnag.AutoNotify()
		panic(err)
	}
}

//Assert panics if false
func Assert(cond bool) {
	if !cond {
		//debug.SetTraceback("all")
		//debug.PrintStack()
		defer bugsnag.AutoNotify()
		panic("Assertion failed")
	}
}

//GetRudderEventMap returns the event structure from the client payload
func GetRudderEventMap(rudderEvent interface{}) (map[string]interface{}, bool) {

	rudderEventMap, ok := rudderEvent.(map[string]interface{})
	if !ok {
		return nil, false
	}
	rudderMsg, ok := rudderEventMap["rl_message"]
	if !ok {
		return nil, false
	}
	rudderMsgMap, ok := rudderMsg.(map[string]interface{})
	if !ok {
		return nil, false
	}
	return rudderMsgMap, true
}

//GetRudderEventVal returns the value corresponding to the key in the message structure
func GetRudderEventVal(key string, rudderEvent interface{}) (interface{}, bool) {

	rudderMsgMap, ok := GetRudderEventMap(rudderEvent)
	if !ok {
		return nil, false
	}
	rudderVal, ok := rudderMsgMap[key]
	if !ok {
		return nil, false
	}
	return rudderVal, true
}

//ParseRudderEventBatch looks for the batch structure inside event
func ParseRudderEventBatch(eventPayload json.RawMessage) ([]interface{}, bool) {
	var eventListJSON map[string]interface{}
	err := json.Unmarshal(eventPayload, &eventListJSON)
	if err != nil {
		return nil, false
	}
	_, ok := eventListJSON["batch"]
	if !ok {
		return nil, false
	}
	eventListJSONBatchType, ok := eventListJSON["batch"].([]interface{})
	if !ok {
		return nil, false
	}
	return eventListJSONBatchType, true
}

//GetRudderEventUserID return the UserID from the object
func GetRudderEventUserID(eventList []interface{}) (string, bool) {
	userID, ok := GetRudderEventVal("rl_anonymous_id", eventList[0])
	if !ok {
		return "", false
	}
	userIDStr, ok := userID.(string)
	return userIDStr, true
}

// ZipFiles compresses files[] into zip at filename
func ZipFiles(filename string, files []string) error {

	newZipFile, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer newZipFile.Close()

	zipWriter := zip.NewWriter(newZipFile)
	defer zipWriter.Close()

	// Add files to zip
	for _, file := range files {
		if err = AddFileToZip(zipWriter, file); err != nil {
			return err
		}
	}
	return nil
}

// AddFileToZip adds file to zip including size header stats
func AddFileToZip(zipWriter *zip.Writer, filename string) error {

	fileToZip, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer fileToZip.Close()

	// Get the file information
	info, err := fileToZip.Stat()
	AssertError(err)

	header, err := zip.FileInfoHeader(info)
	AssertError(err)

	// Using FileInfoHeader() above only uses the basename of the file. If we want
	// to preserve the folder structure we can overwrite this with the full path.
	// uncomment this line to preserve folder structure
	// header.Name = filename

	// Change to deflate to gain better compression
	// see http://golang.org/pkg/archive/zip/#pkg-constants
	header.Method = zip.Deflate

	writer, err := zipWriter.CreateHeader(header)
	AssertError(err)
	_, err = io.Copy(writer, fileToZip)
	return err
}

// ReadLines reads a whole file into memory
// and returns a slice of its lines.
func ReadLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

//PerfStats is the class for managing performance stats. Not multi-threaded safe now
type PerfStats struct {
	eventCount           int64
	elapsedTime          time.Duration
	lastPrintEventCount  int64
	lastPrintElapsedTime time.Duration
	lastPrintTime        time.Time
	compStr              string
	tmpStart             time.Time
	instantRateCall      float64
	printThres           int
}

//Setup initializes the stat collector
func (stats *PerfStats) Setup(comp string) {
	stats.compStr = comp
	stats.lastPrintTime = time.Now()
	stats.printThres = 5 //seconds
}

//Start marks the start of event collection
func (stats *PerfStats) Start() {
	stats.tmpStart = time.Now()
}

//End marks the end of one round of stat collection. events is number of events processed since start
func (stats *PerfStats) End(events int) {
	elapsed := time.Since(stats.tmpStart)
	stats.elapsedTime += elapsed
	stats.eventCount += int64(events)
	stats.instantRateCall = float64(events) * float64(time.Second) / float64(elapsed)
}

//Print displays the stats
func (stats *PerfStats) Print() {
	if time.Since(stats.lastPrintTime) > time.Duration(stats.printThres)*time.Second {
		overallRate := float64(stats.eventCount) * float64(time.Second) / float64(stats.elapsedTime)
		instantRate := float64(stats.eventCount-stats.lastPrintEventCount) * float64(time.Second) / float64(stats.elapsedTime-stats.lastPrintElapsedTime)
		logger.Infof("%s: Total: %d Overall:%f, Instant(print):%f, Instant(call):%f\n",
			stats.compStr, stats.eventCount, overallRate, instantRate, stats.instantRateCall)
		stats.lastPrintEventCount = stats.eventCount
		stats.lastPrintElapsedTime = stats.elapsedTime
		stats.lastPrintTime = time.Now()
	}
}

//Copy copies the exported fields from src to dest
//Used for copying the default transport
func Copy(dst, src interface{}) {
	srcV := reflect.ValueOf(src)
	dstV := reflect.ValueOf(dst)

	// First src and dst must be pointers, so that dst can be assignable.
	if srcV.Kind() != reflect.Ptr {
		panic("Copy: src must be a pointer")
	}
	if dstV.Kind() != reflect.Ptr {
		panic("Copy: dst must be a pointer")
	}
	srcV = srcV.Elem()
	dstV = dstV.Elem()

	// Then src must be assignable to dst and both must be structs (but this is
	// already guaranteed).
	srcT := srcV.Type()
	dstT := dstV.Type()
	if !srcT.AssignableTo(dstT) {
		panic("Copy not assignable to")
	}
	if srcT.Kind() != reflect.Struct || dstT.Kind() != reflect.Struct {
		panic("Copy are not structs")
	}

	// Finally, copy all exported fields.  Since the types are the same, we
	// have no problems and we only have to ignore unexported fields.
	for i := 0; i < srcV.NumField(); i++ {
		sf := dstT.Field(i)
		if sf.PkgPath != "" {
			// Unexported field.
			continue
		}
		dstV.Field(i).Set(srcV.Field(i))
	}
}

// GetIPFromReq gets ip address from request
func GetIPFromReq(req *http.Request) string {
	addresses := strings.Split(req.Header.Get("X-Forwarded-For"), ",")
	if addresses[0] == "" {
		return req.RemoteAddr // When there is no load-balancer
	}
	strings.Replace(addresses[0], " ", "", -1)
	return addresses[0]
}

func ContainsString(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

func equal(expected, actual interface{}) bool {
	if expected == nil || actual == nil {
		return expected == actual
	}

	return reflect.DeepEqual(expected, actual)

}

// Contains returns true if an element is present in a iteratee.
// https://github.com/thoas/go-funk
func Contains(in interface{}, elem interface{}) bool {
	inValue := reflect.ValueOf(in)
	elemValue := reflect.ValueOf(elem)
	inType := inValue.Type()

	switch inType.Kind() {
	case reflect.String:
		return strings.Contains(inValue.String(), elemValue.String())
	case reflect.Map:
		for _, key := range inValue.MapKeys() {
			if equal(key.Interface(), elem) {
				return true
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < inValue.Len(); i++ {
			if equal(inValue.Index(i).Interface(), elem) {
				return true
			}
		}
	default:
		AssertError(fmt.Errorf("Type %s is not supported by Contains, supported types are String, Map, Slice, Array", inType.String()))
	}

	return false
}

// IncrementMapByKey starts with 1 and increments the counter of a key
func IncrementMapByKey(m map[string]int, key string) {
	_, found := m[key]
	if found {
		m[key] = m[key] + 1
	} else {
		m[key] = 1
	}
}
