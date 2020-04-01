package misc

import (
	"archive/zip"
	"bufio"
	"compress/gzip"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strings"

	//"runtime/debug"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/thoas/go-funk"
)

var AppStartTime int64

const (
	// RFC3339 with milli sec precision
	RFC3339Milli = "2006-01-02T15:04:05.999Z07:00"
	//This is integer representation of Postgres version.
	//For ex, integer representation of version 9.6.3 is 90603
	//Minimum postgres version needed for rudder server is 10
	minPostgresVersion = 100000
)

// ErrorStoreT : DS to store the app errors
type ErrorStoreT struct {
	Errors []RudderError
}

//RudderError : to store rudder error
type RudderError struct {
	StartTime         int64
	ReadableStartTime string
	Message           string
	StackTrace        string
	Code              int
}

func getErrorStore() (ErrorStoreT, error) {
	var errorStore ErrorStoreT
	errorStorePath := config.GetString("recovery.errorStorePath", "/tmp/error_store.json")
	data, err := ioutil.ReadFile(errorStorePath)
	if os.IsNotExist(err) {
		defaultErrorStoreJSON := "{\"Errors\":[]}"
		data = []byte(defaultErrorStoreJSON)
	} else if err != nil {
		logger.Fatal("Failed to get ErrorStore", err)
		return errorStore, err
	}

	err = json.Unmarshal(data, &errorStore)

	if err != nil {
		logger.Fatal("Failed to unmarshall ErrorStore to json", err)
		return errorStore, err
	}

	return errorStore, nil
}

func saveErrorStore(errorStore ErrorStoreT) {
	errorStoreJSON, err := json.MarshalIndent(&errorStore, "", " ")
	if err != nil {
		logger.Fatal("failed to marshal errorStore", errorStore)
		return
	}
	errorStorePath := config.GetString("recovery.errorStorePath", "/tmp/error_store.json")
	err = ioutil.WriteFile(errorStorePath, errorStoreJSON, 0644)
	if err != nil {
		logger.Fatal("failed to write to errorStore")
	}
}

//RecordAppError appends the error occured to error_store.json
func RecordAppError(err error) {
	if err == nil {
		return
	}

	if AppStartTime == 0 {
		return
	}

	byteArr := make([]byte, 2048) // adjust buffer size to be larger than expected stack
	n := runtime.Stack(byteArr, false)
	stackTrace := string(byteArr[:n])

	errorStore, localErr := getErrorStore()
	if localErr != nil || errorStore.Errors == nil {
		return
	}

	//TODO Code is hardcoded now. When we introduce rudder error codes, we can use them.
	errorStore.Errors = append(errorStore.Errors,
		RudderError{
			StartTime:         AppStartTime,
			ReadableStartTime: fmt.Sprint(time.Unix(AppStartTime, 0)),
			Message:           err.Error(),
			StackTrace:        stackTrace,
			Code:              101,
		})
	saveErrorStore(errorStore)
}

func AssertErrorIfDev(err error) {

	goEnv := os.Getenv("GO_ENV")
	if goEnv == "production" {
		logger.Error(err.Error())
		return
	}

	if err != nil {
		panic(err)
	}
}

//GetMD5Hash returns EncodeToString(md5 hash of the input string)
func GetMD5Hash(input string) string {
	hash := md5.Sum([]byte(input))
	return hex.EncodeToString(hash[:])
}

//GetRudderEventMap returns the event structure from the client payload
func GetRudderEventMap(rudderEvent interface{}) (map[string]interface{}, bool) {

	rudderEventMap, ok := rudderEvent.(map[string]interface{})
	if !ok {
		return nil, false
	}
	return rudderEventMap, true
}

//GetRudderEventVal returns the value corresponding to the key in the message structure
func GetRudderEventVal(key string, rudderEvent interface{}) (interface{}, bool) {

	rudderEventMap, ok := GetRudderEventMap(rudderEvent)
	if !ok {
		return nil, false
	}
	rudderVal, ok := rudderEventMap[key]
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
		logger.Debug("json parsing of event payload failed ", string(eventPayload))
		return nil, false
	}
	_, ok := eventListJSON["batch"]
	if !ok {
		logger.Debug("error retrieving value for batch key ", string(eventPayload))
		return nil, false
	}
	eventListJSONBatchType, ok := eventListJSON["batch"].([]interface{})
	if !ok {
		logger.Error("error casting batch value to list of maps ", string(eventPayload))
		return nil, false
	}
	return eventListJSONBatchType, true
}

//GetAnonymousID return the UserID from the object
func GetAnonymousID(event interface{}) (string, bool) {
	userID, ok := GetRudderEventVal("anonymousId", event)
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
	if err != nil {
		panic(err)
	}

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		panic(err)
	}

	// Using FileInfoHeader() above only uses the basename of the file. If we want
	// to preserve the folder structure we can overwrite this with the full path.
	// uncomment this line to preserve folder structure
	// header.Name = filename

	// Change to deflate to gain better compression
	// see http://golang.org/pkg/archive/zip/#pkg-constants
	header.Method = zip.Deflate

	writer, err := zipWriter.CreateHeader(header)
	if err != nil {
		panic(err)
	}
	_, err = io.Copy(writer, fileToZip)
	return err
}

// UnZipSingleFile unzips zip containing single file into ouputfile path passed
func UnZipSingleFile(outputfile string, filename string) {
	r, err := zip.OpenReader(filename)
	if err != nil {
		panic(err)
	}
	defer r.Close()
	inputfile := r.File[0]
	// Make File
	err = os.MkdirAll(filepath.Dir(outputfile), os.ModePerm)
	outFile, err := os.OpenFile(outputfile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, inputfile.Mode())
	if err != nil {
		panic(err)
	}
	rc, err := inputfile.Open()
	_, err = io.Copy(outFile, rc)
	outFile.Close()
	rc.Close()
}

func RemoveFilePaths(filepaths ...string) {
	for _, filepath := range filepaths {
		err := os.Remove(filepath)
		if err != nil {
			logger.Error(err)
		}
	}
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

// CreateTMPDIR creates tmp dir at path configured via RUDDER_TMPDIR env var
func CreateTMPDIR() (string, error) {
	tmpdirPath := strings.TrimSuffix(config.GetEnv("RUDDER_TMPDIR", ""), "/")
	// second chance: fallback to /tmp if this folder exists
	if tmpdirPath == "" {
		fallbackPath := "/tmp"
		_, err := os.Stat(fallbackPath)
		if err == nil {
			tmpdirPath = fallbackPath
			logger.Infof("RUDDER_TMPDIR not found, falling back to %v\n", fallbackPath)
		}
	}
	if tmpdirPath == "" {
		return os.UserHomeDir()
	}
	return tmpdirPath, nil
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
		logger.Infof("%s: Total: %d Overall:%f, Instant(print):%f, Instant(call):%f",
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
		panic(fmt.Errorf("Type %s is not supported by Contains, supported types are String, Map, Slice, Array", inType.String()))
	}

	return false
}

// IncrementMapByKey starts with 1 and increments the counter of a key
func IncrementMapByKey(m map[string]int, key string, increment int) {
	_, found := m[key]
	if found {
		m[key] = m[key] + increment
	} else {
		m[key] = increment
	}
}

// Returns chronological timestamp of the event using the formula
// timestamp = receivedAt - (sentAt - originalTimestamp)
func GetChronologicalTimeStamp(receivedAt, sentAt, originalTimestamp time.Time) time.Time {
	return receivedAt.Add(-sentAt.Sub(originalTimestamp))
}

func StringKeys(input interface{}) []string {
	keys := funk.Keys(input)
	stringKeys := keys.([]string)
	return stringKeys
}

func MapStringKeys(input map[string]interface{}) []string {
	keys := make([]string, 0, len(input))
	for k := range input {
		keys = append(keys, k)
	}
	return keys
}

func TruncateStr(str string, limit int) string {
	if len(str) > limit {
		str = str[:limit]
	}
	return str
}

func SortedMapKeys(input interface{}) []string {
	inValue := reflect.ValueOf(input)
	mapKeys := inValue.MapKeys()
	keys := make([]string, 0, len(mapKeys))
	for _, key := range mapKeys {
		keys = append(keys, key.String())
	}
	sort.Strings(keys)
	return keys
}

func SortedStructSliceValues(input interface{}, filedName string) []string {
	items := reflect.ValueOf(input)
	var keys []string
	if items.Kind() == reflect.Slice {
		for i := 0; i < items.Len(); i++ {
			item := items.Index(i)
			if item.Kind() == reflect.Struct {
				v := reflect.Indirect(item)
				for j := 0; j < v.NumField(); j++ {
					if v.Type().Field(j).Name == "Name" {
						keys = append(keys, v.Field(j).String())
					}
				}
			}
		}
	}
	sort.Strings(keys)
	return keys
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func ReplaceMultiRegex(str string, expList map[string]string) (string, error) {
	replacedStr := str
	for regex, substitute := range expList {
		exp, err := regexp.Compile(regex)
		if err != nil {
			return "", err
		}
		replacedStr = exp.ReplaceAllString(replacedStr, substitute)
	}
	return replacedStr, nil
}

// PrintMemUsage outputs the current, total and OS memory being used. As well as the number
// of garage collection cycles completed.
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	logger.Debug("#########")
	logger.Debugf("Alloc = %v MiB\n", bToMb(m.Alloc))
	logger.Debugf("\tTotalAlloc = %v MiB\n", bToMb(m.TotalAlloc))
	logger.Debugf("\tSys = %v MiB\n", bToMb(m.Sys))
	logger.Debugf("\tNumGC = %v\n", m.NumGC)
	logger.Debug("#########")
}

type GZipWriter struct {
	File      *os.File
	GzWriter  *gzip.Writer
	BufWriter *bufio.Writer
}

func CreateGZ(s string) (w GZipWriter, err error) {

	file, err := os.OpenFile(s, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		return
	}
	gzWriter := gzip.NewWriter(file)
	bufWriter := bufio.NewWriter(gzWriter)
	w = GZipWriter{
		File:      file,
		GzWriter:  gzWriter,
		BufWriter: bufWriter,
	}
	return
}

func (w GZipWriter) WriteGZ(s string) {
	w.BufWriter.WriteString(s)
}

func (w GZipWriter) Write(b []byte) {
	w.BufWriter.Write(b)
}

func (w GZipWriter) CloseGZ() {
	w.BufWriter.Flush()
	w.GzWriter.Close()
	w.File.Close()
}

func KeepProcessAlive() {
	var ch chan int
	_ = <-ch
}

// GetOutboundIP returns preferred outbound ip of this machine
// https://stackoverflow.com/a/37382208
func GetOutboundIP() (net.IP, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP, nil
}

//IsPostgresCompatible checks the if the version of postgres is greater than minPostgresVersion
func IsPostgresCompatible(connInfo string) bool {
	dbHandle, err := sql.Open("postgres", connInfo)
	if err != nil {
		panic(err)
	}
	defer dbHandle.Close()

	err = dbHandle.Ping()
	if err != nil {
		panic(err)
	}

	var versionNum int
	err = dbHandle.QueryRow("SHOW server_version_num;").Scan(&versionNum)
	if err != nil {
		return false
	}

	return versionNum >= minPostgresVersion
}
