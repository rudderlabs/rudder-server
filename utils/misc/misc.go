package misc

import (
	"archive/zip"
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/araddon/dateparse"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/mkmik/multierror"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	uuid "github.com/satori/go.uuid"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/thoas/go-funk"
)

var AppStartTime int64

const (
	// RFC3339Milli with milli sec precision
	RFC3339Milli = "2006-01-02T15:04:05.000Z07:00"
)

// ErrorStoreT : DS to store the app errors
type ErrorStoreT struct {
	Errors []RudderError
}

//RudderError : to store rudder error
type RudderError struct {
	StartTime         int64
	CrashTime         int64
	ReadableStartTime string
	ReadableCrashTime string
	Message           string
	StackTrace        string
	Code              int
}

var pkgLogger logger.LoggerI

func init() {
	pkgLogger = logger.NewLogger().Child("utils").Child("misc")
}

func getErrorStore() (ErrorStoreT, error) {
	var errorStore ErrorStoreT
	errorStorePath := config.GetString("recovery.errorStorePath", "/tmp/error_store.json")
	data, err := ioutil.ReadFile(errorStorePath)
	if os.IsNotExist(err) {
		defaultErrorStoreJSON := "{\"Errors\":[]}"
		data = []byte(defaultErrorStoreJSON)
	} else if err != nil {
		pkgLogger.Fatal("Failed to get ErrorStore", err)
		return errorStore, err
	}

	err = json.Unmarshal(data, &errorStore)
	if err != nil {
		pkgLogger.Errorf("Failed to Unmarshall %s. Error:  %v", errorStorePath, err)
		if renameErr := os.Rename(errorStorePath, fmt.Sprintf("%s.bkp", errorStorePath)); renameErr != nil {
			pkgLogger.Errorf("Failed to back up: %s. Error: %v", errorStorePath, err)
		}
		errorStore = ErrorStoreT{Errors: []RudderError{}}
	}
	return errorStore, nil
}

func saveErrorStore(errorStore ErrorStoreT) {
	errorStoreJSON, err := json.MarshalIndent(&errorStore, "", " ")
	if err != nil {
		pkgLogger.Fatal("failed to marshal errorStore", errorStore)
		return
	}
	errorStorePath := config.GetString("recovery.errorStorePath", "/tmp/error_store.json")
	err = ioutil.WriteFile(errorStorePath, errorStoreJSON, 0644)
	if err != nil {
		pkgLogger.Fatal("failed to write to errorStore")
	}
}

//AppendError creates or appends second error to first error
func AppendError(callingMethodName string, firstError *error, secondError *error) {
	if *firstError != nil {
		*firstError = fmt.Errorf("%v ; %v : %w", (*firstError).Error(), callingMethodName, *secondError)
	} else {
		*firstError = fmt.Errorf("%v : %w", callingMethodName, *secondError)
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

	crashTime := time.Now().Unix()

	//TODO Code is hardcoded now. When we introduce rudder error codes, we can use them.
	errorStore.Errors = append(errorStore.Errors,
		RudderError{
			StartTime:         AppStartTime,
			CrashTime:         crashTime,
			ReadableStartTime: fmt.Sprint(time.Unix(AppStartTime, 0)),
			ReadableCrashTime: fmt.Sprint(time.Unix(crashTime, 0)),
			Message:           err.Error(),
			StackTrace:        stackTrace,
			Code:              101,
		})
	saveErrorStore(errorStore)
}

func AssertErrorIfDev(err error) {

	goEnv := os.Getenv("GO_ENV")
	if goEnv == "production" {
		pkgLogger.Error(err.Error())
		return
	}

	if err != nil {
		panic(err)
	}
}

func GetHash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

//GetMD5Hash returns EncodeToString(md5 hash of the input string)
func GetMD5Hash(input string) string {
	hash := md5.Sum([]byte(input))
	return hex.EncodeToString(hash[:])
}

//GetRudderEventVal returns the value corresponding to the key in the message structure
func GetRudderEventVal(key string, rudderEvent types.SingularEventT) (interface{}, bool) {

	rudderVal, ok := rudderEvent[key]
	if !ok {
		return nil, false
	}
	return rudderVal, true
}

//ParseRudderEventBatch looks for the batch structure inside event
func ParseRudderEventBatch(eventPayload json.RawMessage) ([]types.SingularEventT, bool) {
	var gatewayBatchEvent types.GatewayBatchRequestT
	err := json.Unmarshal(eventPayload, &gatewayBatchEvent)
	if err != nil {
		pkgLogger.Debug("json parsing of event payload failed ", string(eventPayload))
		return nil, false
	}

	return gatewayBatchEvent.Batch, true
}

//GetRudderID return the UserID from the object
func GetRudderID(event types.SingularEventT) (string, bool) {
	userID, ok := GetRudderEventVal("rudderId", event)
	if !ok {
		//TODO: Remove this in next build.
		//This is for backwards compatibilty, esp for those with sessions.
		userID, ok = GetRudderEventVal("anonymousId", event)
		if !ok {
			return "", false
		}
	}
	userIDStr, ok := userID.(string)
	return userIDStr, ok
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
	os.MkdirAll(filepath.Dir(outputfile), os.ModePerm)
	outFile, err := os.OpenFile(outputfile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, inputfile.Mode())
	if err != nil {
		panic(err)
	}
	rc, _ := inputfile.Open()
	io.Copy(outFile, rc)
	outFile.Close()
	rc.Close()
}

func RemoveFilePaths(filepaths ...string) {
	for _, filepath := range filepaths {
		err := os.Remove(filepath)
		if err != nil {
			pkgLogger.Error(err)
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
			pkgLogger.Infof("RUDDER_TMPDIR not found, falling back to %v\n", fallbackPath)
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
		pkgLogger.Infof("%s: Total: %d Overall:%f, Instant(print):%f, Instant(call):%f",
			stats.compStr, stats.eventCount, overallRate, instantRate, stats.instantRateCall)
		stats.lastPrintEventCount = stats.eventCount
		stats.lastPrintElapsedTime = stats.elapsedTime
		stats.lastPrintTime = time.Now()
	}
}

func (stats *PerfStats) Status() map[string]interface{} {
	overallRate := float64(0)
	if float64(stats.elapsedTime) > 0 {
		overallRate = float64(stats.eventCount) * float64(time.Second) / float64(stats.elapsedTime)
	}
	return map[string]interface{}{
		"total-events": stats.eventCount,
		"overall-rate": overallRate,
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
		splits := strings.Split(req.RemoteAddr, ":")
		pkgLogger.Debugf("%#v", req)
		return strings.Join(splits[:len(splits)-1], ":") // When there is no load-balancer
	}

	return strings.Replace(addresses[0], " ", "", -1)
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

// TailTruncateStr returns the last `count` digits of a string
func TailTruncateStr(str string, count int) string {
	if len(str) > count {
		str = str[len(str)-count:]
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

func IntArrayToString(a []int64, delim string) string {
	return strings.Trim(strings.Replace(fmt.Sprint(a), " ", delim, -1), "[]")
}

func MakeJSONArray(bytesArray [][]byte) []byte {
	joinedArray := bytes.Join(bytesArray, []byte(","))

	// insert '[' to the front
	joinedArray = append(joinedArray, 0)
	copy(joinedArray[1:], joinedArray[0:])
	joinedArray[0] = byte('[')

	// append ']'
	joinedArray = append(joinedArray, ']')
	return joinedArray
}

func SingleQuoteLiteralJoin(slice []string) string {
	var str string
	//TODO: use strings.Join() instead
	for index, key := range slice {
		if index > 0 {
			str += `, `
		}
		str += QuoteLiteral(key)
	}
	return str
}

// PrintMemUsage outputs the current, total and OS memory being used. As well as the number
// of garage collection cycles completed.
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	pkgLogger.Debug("#########")
	pkgLogger.Debugf("Alloc = %v MiB\n", bToMb(m.Alloc))
	pkgLogger.Debugf("\tTotalAlloc = %v MiB\n", bToMb(m.TotalAlloc))
	pkgLogger.Debugf("\tSys = %v MiB\n", bToMb(m.Sys))
	pkgLogger.Debugf("\tNumGC = %v\n", m.NumGC)
	pkgLogger.Debug("#########")
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
	count, err := w.BufWriter.WriteString(s)
	if err != nil {
		pkgLogger.Errorf(`[GZWriter]: Error writing string of length %d by GZipWriter.WriteGZ. Bytes written: %d. Error: %v`, len(s), count, err)
	}
}

func (w GZipWriter) Write(b []byte) {
	count, err := w.BufWriter.Write(b)
	if err != nil {
		pkgLogger.Errorf(`[GZWriter]: Error writing bytes of length %d by GZipWriter.Write. Bytes written: %d. Error: %v`, len(b), count, err)
	}
}

func (w GZipWriter) CloseGZ() error {
	err := w.BufWriter.Flush()
	if err != nil {
		pkgLogger.Errorf(`[GZWriter]: Error flushing GZipWriter.BufWriter : %v`, err)
	}
	err = w.GzWriter.Close()
	if err != nil {
		pkgLogger.Errorf(`[GZWriter]: Error closing GZipWriter : %v`, err)
	}
	err = w.File.Close()
	if err != nil {
		if pathErr, ok := err.(*os.PathError); ok && pathErr.Err == os.ErrClosed {
			err = nil
		} else {
			pkgLogger.Errorf(`[GZWriter]: Error closing GZipWriter File %s: %v`, w.File.Name(), err)
		}
	}
	return err
}

func GetMacAddress() string {
	//----------------------
	// Get the local machine IP address
	// https://www.socketloop.com/tutorials/golang-how-do-I-get-the-local-ip-non-loopback-address
	//----------------------

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}

	var currentIP, currentNetworkHardwareName string
	for _, address := range addrs {
		// check the address type and if it is not a loopback then that's the current ip
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				currentIP = ipnet.IP.String()
			}
		}
	}

	// get all the system's or local machine's network interfaces
	interfaces, _ := net.Interfaces()
	for _, interf := range interfaces {
		if addrs, err := interf.Addrs(); err == nil {
			for _, addr := range addrs {
				// only interested in the name with current IP address
				if strings.Contains(addr.String(), currentIP) {
					currentNetworkHardwareName = interf.Name
				}
			}
		}
	}

	// extract the hardware information base on the interface name captured above
	netInterface, err := net.InterfaceByName(currentNetworkHardwareName)
	if err != nil {
		return ""
	}

	macAddress := netInterface.HardwareAddr

	return macAddress.String()
}

func KeepProcessAlive() {
	var ch chan int
	<-ch
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

/*
RunWithTimeout runs provided function f until provided timeout d.
If the timeout is reached, onTimeout callback will be called.
*/
func RunWithTimeout(f func(), onTimeout func(), d time.Duration) {
	c := make(chan struct{})
	go func() {
		defer close(c)
		f()
	}()

	select {
	case <-c:
	case <-time.After(d):
		onTimeout()
	}
}

/*
IsValidUUID will check if provided string is a valid UUID
*/
func IsValidUUID(uuid string) bool {
	r := regexp.MustCompile("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-4[a-fA-F0-9]{3}-[8|9|aA|bB][a-fA-F0-9]{3}-[a-fA-F0-9]{12}$")
	return r.MatchString(uuid)
}

func HasAWSKeysInConfig(config interface{}) bool {
	configMap := config.(map[string]interface{})
	if configMap["accessKeyID"] == nil || configMap["accessKey"] == nil {
		return false
	}
	if configMap["accessKeyID"].(string) == "" || configMap["accessKey"].(string) == "" {
		return false
	}
	return true
}

func GetObjectStorageConfig(provider string, objectStorageConfig interface{}) map[string]interface{} {
	objectStorageConfigMap := objectStorageConfig.(map[string]interface{})
	if provider == "S3" && !HasAWSKeysInConfig(objectStorageConfig) {
		clonedObjectStorageConfig := make(map[string]interface{})
		for k, v := range objectStorageConfigMap {
			clonedObjectStorageConfig[k] = v
		}
		clonedObjectStorageConfig["accessKeyID"] = config.GetEnv("RUDDER_AWS_S3_COPY_USER_ACCESS_KEY_ID", "")
		clonedObjectStorageConfig["accessKey"] = config.GetEnv("RUDDER_AWS_S3_COPY_USER_ACCESS_KEY", "")
		return clonedObjectStorageConfig
	}
	return objectStorageConfigMap

}

func GetSpacesLocation(location string) (region string) {
	r, _ := regexp.Compile("\\.*.*\\.digitaloceanspaces\\.com")
	subLocation := r.FindString(location)
	regionTokens := strings.Split(subLocation, ".")
	if len(regionTokens) == 3 {
		region = regionTokens[0]
	}
	return region
}

//GetNodeID returns the nodeId of the current node
func GetNodeID() string {
	nodeID := config.GetRequiredEnv("INSTANCE_ID")
	return nodeID
}

//MakeRetryablePostRequest is Util function to make a post request.
func MakeRetryablePostRequest(url string, endpoint string, data interface{}) (response []byte, statusCode int, err error) {
	backendURL := fmt.Sprintf("%s%s", url, endpoint)
	dataJSON, err := json.Marshal(data)

	resp, err := retryablehttp.Post(backendURL, "application/json", dataJSON)

	if err != nil {
		return nil, -1, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()

	pkgLogger.Debugf("Post request: Successful %s", string(body))
	return body, resp.StatusCode, nil
}

//GetMD5UUID hashes the given string into md5 and returns it as auuid
func GetMD5UUID(str string) (uuid.UUID, error) {
	md5Sum := md5.Sum([]byte(str))
	u, err := uuid.FromBytes(md5Sum[:])
	u.SetVersion(uuid.V4)
	u.SetVariant(uuid.VariantRFC4122)
	return u, err
}

// GetParsedTimestamp returns the parsed timestamp
func GetParsedTimestamp(input interface{}) (time.Time, bool) {
	var parsedTimestamp time.Time
	var valid bool
	if timestampStr, typecasted := input.(string); typecasted {
		var err error
		parsedTimestamp, err = dateparse.ParseAny(timestampStr)
		if err == nil {
			valid = true
		}
	}
	return parsedTimestamp, valid
}

func isValidTag(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		switch {
		case strings.ContainsRune("!#$%&()*+-./:<=>?@[]^_{|}~ ", c):
			// Backslash and quote chars are reserved, but
			// otherwise any punctuation chars are allowed
			// in a tag name.
		case !unicode.IsLetter(c) && !unicode.IsDigit(c):
			return false
		}
	}
	return true
}

func parseTag(tag string) (string, string) {
	if idx := strings.Index(tag, ","); idx != -1 {
		return tag[:idx], tag[idx+1:]
	}
	return tag, ""
}

//GetMandatoryJSONFieldNames returns all the json field names defined against the json tag for each field.
func GetMandatoryJSONFieldNames(st interface{}) []string {
	v := reflect.TypeOf(st)
	mandatoryJSONFieldNames := make([]string, 0, v.NumField())
	for i := 0; i < v.NumField(); i++ {
		jsonTag, ok := v.Field(i).Tag.Lookup("json")
		if !ok {
			continue
		}
		name, tags := parseTag(jsonTag)
		if !strings.Contains(tags, "optional") {
			if !isValidTag(name) {
				name = v.Field(i).Name
			}
			mandatoryJSONFieldNames = append(mandatoryJSONFieldNames, name)
		}
	}
	return mandatoryJSONFieldNames
}

func MinInt(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

//GetTagName gets the tag name using a uuid and name
func GetTagName(id string, names ...string) string {
	var truncatedNames string
	for _, name := range names {
		name = strings.ReplaceAll(name, ":", "-")
		truncatedNames += TruncateStr(name, 15) + "_"
	}
	return truncatedNames + TailTruncateStr(id, 6)
}

//UpdateJSONWithNewKeyVal enhances the json passed with key, val
func UpdateJSONWithNewKeyVal(params []byte, key, val string) []byte {
	updatedParams, err := sjson.SetBytes(params, key, val)
	if err != nil {
		return params
	}

	return updatedParams
}

func ConcatErrors(givenErrors []error) error {
	var errorsToJoin []error
	for _, err := range givenErrors {
		if err == nil {
			pkgLogger.Errorf("%v", string(debug.Stack()))
			continue
		}
		errorsToJoin = append(errorsToJoin, err)
	}
	return multierror.Join(errorsToJoin)
}
