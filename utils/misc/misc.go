package misc

import (
	"archive/zip"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"

	jsoniter "github.com/json-iterator/go"

	"github.com/araddon/dateparse"
	"github.com/bugsnag/bugsnag-go/v2"
	"github.com/cenkalti/backoff"
	"github.com/google/uuid"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/mkmik/multierror"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/metric"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/logger"

	"github.com/thoas/go-funk"

	"github.com/rudderlabs/rudder-server/utils/types"
)

var (
	AppStartTime        int64
	errorStorePath      string
	reservedFolderPaths []*RFP
	jsonfast            = jsoniter.ConfigCompatibleWithStandardLibrary
	notifyOnce          sync.Once
)

const (
	// RFC3339Milli with milli sec precision
	RFC3339Milli          = "2006-01-02T15:04:05.000Z07:00"
	NOTIMEZONEFORMATPARSE = "2006-01-02T15:04:05"
)

const (
	RudderAsyncDestinationLogs    = "rudder-async-destination-logs"
	RudderArchives                = "rudder-archives"
	RudderWarehouseStagingUploads = "rudder-warehouse-staging-uploads"
	RudderRawDataDestinationLogs  = "rudder-raw-data-destination-logs"
	RudderWarehouseLoadUploadsTmp = "rudder-warehouse-load-uploads-tmp"
	RudderIdentityMergeRulesTmp   = "rudder-identity-merge-rules-tmp"
	RudderIdentityMappingsTmp     = "rudder-identity-mappings-tmp"
	RudderRedshiftManifests       = "rudder-redshift-manifests"
	RudderWarehouseJsonUploadsTmp = "rudder-warehouse-json-uploads-tmp"
	RudderTestPayload             = "rudder-test-payload"
)

// ErrorStoreT : DS to store the app errors
type ErrorStoreT struct {
	Errors []RudderError
}

// RudderError : to store rudder error
type RudderError struct {
	StartTime         int64
	CrashTime         int64
	ReadableStartTime string
	ReadableCrashTime string
	Message           string
	StackTrace        string
	Code              int
}

type pair struct {
	key   string
	value float64
}

type pairList []pair

func (p pairList) Len() int           { return len(p) }
func (p pairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p pairList) Less(i, j int) bool { return p[i].value < p[j].value }

type RFP struct {
	path         string
	levelsToKeep int
}

var pkgLogger logger.Logger

func init() {
	uuid.EnableRandPool()
}

func Init() {
	pkgLogger = logger.NewLogger().Child("utils").Child("misc")
	config.RegisterStringConfigVariable("/tmp/error_store.json", &errorStorePath, false, "recovery.errorStorePath")
	reservedFolderPaths = GetReservedFolderPaths()
}

func BatchDestinations() []string {
	batchDestinations := []string{"S3", "GCS", "MINIO", "RS", "BQ", "AZURE_BLOB", "SNOWFLAKE", "POSTGRES", "CLICKHOUSE", "DIGITAL_OCEAN_SPACES", "MSSQL", "AZURE_SYNAPSE", "S3_DATALAKE", "MARKETO_BULK_UPLOAD", "GCS_DATALAKE", "AZURE_DATALAKE", "DELTALAKE"}
	return batchDestinations
}

func getErrorStore() (ErrorStoreT, error) {
	var errorStore ErrorStoreT
	data, err := os.ReadFile(errorStorePath)
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
	err = os.WriteFile(errorStorePath, errorStoreJSON, 0o644)
	if err != nil {
		pkgLogger.Fatal("failed to write to errorStore")
	}
}

// AppendError creates or appends second error to first error
func AppendError(callingMethodName string, firstError, secondError *error) {
	if *firstError != nil {
		*firstError = fmt.Errorf("%v ; %v : %w", (*firstError).Error(), callingMethodName, *secondError)
	} else {
		*firstError = fmt.Errorf("%v : %w", callingMethodName, *secondError)
	}
}

// RecordAppError appends the error occurred to error_store.json
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

	// TODO Code is hardcoded now. When we introduce rudder error codes, we can use them.
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

func GetHash(s string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return int(h.Sum32())
}

// GetMD5Hash returns EncodeToString(md5 hash of the input string)
func GetMD5Hash(input string) string {
	hash := md5.Sum([]byte(input))
	return hex.EncodeToString(hash[:])
}

// GetRudderEventVal returns the value corresponding to the key in the message structure
func GetRudderEventVal(key string, rudderEvent types.SingularEventT) (interface{}, bool) {
	rudderVal, ok := rudderEvent[key]
	if !ok {
		return nil, false
	}
	return rudderVal, true
}

// ParseRudderEventBatch looks for the batch structure inside event
func ParseRudderEventBatch(eventPayload json.RawMessage) ([]types.SingularEventT, bool) {
	var gatewayBatchEvent types.GatewayBatchRequestT
	err := jsonfast.Unmarshal(eventPayload, &gatewayBatchEvent)
	if err != nil {
		pkgLogger.Debug("json parsing of event payload failed ", string(eventPayload))
		return nil, false
	}

	return gatewayBatchEvent.Batch, true
}

// GetRudderID return the UserID from the object
func GetRudderID(event types.SingularEventT) (string, bool) {
	userID, ok := GetRudderEventVal("rudderId", event)
	if !ok {
		// TODO: Remove this in next build.
		// This is for backwards compatibility, esp for those with sessions.
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
	defer func() { _ = newZipFile.Close() }()

	zipWriter := zip.NewWriter(newZipFile)
	defer func() { _ = zipWriter.Close() }()

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
	defer func() { _ = fileToZip.Close() }()

	// Get the file information
	info, err := fileToZip.Stat()
	if err != nil {
		return err
	}

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return err
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
		return err
	}
	_, err = io.Copy(writer, fileToZip)
	return err
}

// RemoveFilePaths removes filePaths as well as cleans up the empty folder structure.
func RemoveFilePaths(filePaths ...string) {
	for _, fp := range filePaths {
		err := os.Remove(fp)
		if err != nil {
			pkgLogger.Error(err)
		}
		RemoveEmptyFolderStructureForFilePath(fp)
	}
}

// GetReservedFolderPaths returns all temporary folder paths.
func GetReservedFolderPaths() []*RFP {
	return []*RFP{
		{path: RudderAsyncDestinationLogs, levelsToKeep: 0},
		{path: RudderArchives, levelsToKeep: 0},
		{path: RudderWarehouseStagingUploads, levelsToKeep: 0},
		{path: RudderRawDataDestinationLogs, levelsToKeep: 0},
		{path: RudderWarehouseLoadUploadsTmp, levelsToKeep: 0},
		{path: RudderIdentityMergeRulesTmp, levelsToKeep: 1},
		{path: RudderIdentityMappingsTmp, levelsToKeep: 1},
		{path: RudderRedshiftManifests, levelsToKeep: 0},
		{path: RudderWarehouseJsonUploadsTmp, levelsToKeep: 2},
		{path: config.GetString("RUDDER_CONNECTION_TESTING_BUCKET_FOLDER_NAME", RudderTestPayload), levelsToKeep: 0},
	}
}

func checkMatch(currDir string) bool {
	for _, rfp := range reservedFolderPaths {
		if ok, err := rfp.matches(currDir); err == nil && ok {
			return true
		}
	}
	return false
}

func (r *RFP) matches(currDir string) (match bool, err error) {
	var tmpDirPath string
	tmpDirPath, err = CreateTMPDIR()
	if err != nil {
		return
	}

	splits := strings.Split(currDir, "/")
	if len(splits) < r.levelsToKeep {
		return
	}
	join := strings.Join(splits[0:len(splits)-r.levelsToKeep], "/")
	match = fmt.Sprintf("%s/%s", tmpDirPath, r.path) == join
	return
}

// RemoveEmptyFolderStructureForFilePath recursively cleans up everything till it reaches the stage where the folders are not empty or parent.
func RemoveEmptyFolderStructureForFilePath(fp string) {
	if fp == "" {
		return
	}
	for currDir := filepath.Dir(fp); currDir != "/" && currDir != "."; {
		// Checking if the currDir is present in the temporary folders or not
		// If present we should stop at that point.
		if checkMatch(currDir) {
			break
		}
		parentDir := filepath.Dir(currDir)
		err := syscall.Rmdir(currDir)
		if err != nil {
			break
		}
		currDir = parentDir
	}
}

// ReadLines reads a whole file into memory
// and returns a slice of its lines.
func ReadLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

var logOnce sync.Once

// CreateTMPDIR creates tmp dir at path configured via RUDDER_TMPDIR env var
func CreateTMPDIR() (string, error) {
	tmpdirPath := strings.TrimSuffix(config.GetString("RUDDER_TMPDIR", ""), "/")
	// second chance: fallback to /tmp if this folder exists
	if tmpdirPath == "" {
		fallbackPath := "/tmp"
		_, err := os.Stat(fallbackPath)
		if err == nil {
			tmpdirPath = fallbackPath
			logOnce.Do(func() {
				pkgLogger.Infof("RUDDER_TMPDIR not found, falling back to %v\n", fallbackPath)
			})
		}
	}
	if tmpdirPath == "" {
		return os.UserHomeDir()
	}
	return tmpdirPath, nil
}

// Copy copies the exported fields from src to dest
// Used for copying the default transport
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

	return strings.ReplaceAll(addresses[0], " ", "")
}

func Contains[K comparable](slice []K, item K) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
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

//  Returns chronological timestamp of the event using the formula
//  timestamp = receivedAt - (sentAt - originalTimestamp)
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

func ConvertStringInterfaceToIntArray(interfaceT interface{}) ([]int64, error) {
	var intArr []int64
	if interfaceT == nil || (reflect.ValueOf(interfaceT).Kind() == reflect.Ptr && reflect.ValueOf(interfaceT).IsNil()) {
		return intArr, nil
	}
	typeInterface := reflect.TypeOf(interfaceT).Kind()
	if !(typeInterface != reflect.Slice) && !(typeInterface != reflect.Array) {
		return intArr, errors.New("didn't receive array from transformer")
	}

	interfaceArray := interfaceT.([]interface{})
	for _, val := range interfaceArray {
		strVal, _ := val.(string)
		intVal, err := strconv.ParseInt(strVal, 10, 64)
		if err != nil {
			return intArr, err
		}
		intArr = append(intArr, intVal)
	}
	return intArr, nil
}

func MakeHTTPRequestWithTimeout(url string, payload io.Reader, timeout time.Duration) ([]byte, int, error) {
	req, err := http.NewRequest("POST", url, payload)
	if err != nil {
		return []byte{}, 400, err
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Timeout: timeout,
	}
	resp, err := client.Do(req)
	if err != nil {
		return []byte{}, 400, err
	}

	var respBody []byte
	if resp != nil && resp.Body != nil {
		respBody, _ = io.ReadAll(resp.Body)
		defer func() { httputil.CloseResponse(resp) }()
	}

	return respBody, resp.StatusCode, nil
}

func HTTPCallWithRetry(url string, payload []byte) ([]byte, int) {
	return HTTPCallWithRetryWithTimeout(url, payload, time.Second*150)
}

func ConvertInterfaceToStringArray(input []interface{}) []string {
	output := make([]string, len(input))
	for i, val := range input {
		valString, _ := val.(string)
		output[i] = valString
	}
	return output
}

func HTTPCallWithRetryWithTimeout(url string, payload []byte, timeout time.Duration) ([]byte, int) {
	var respBody []byte
	var statusCode int
	operation := func() error {
		var fetchError error
		respBody, statusCode, fetchError = MakeHTTPRequestWithTimeout(url, bytes.NewBuffer(payload), timeout)
		return fetchError
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
	err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		pkgLogger.Errorf("Failed to make call. Error: %v, retrying after %v", err, t)
	})
	if err != nil {
		pkgLogger.Error("Error sending request to the server", err)
		return respBody, statusCode
	}
	return respBody, statusCode
}

func IntArrayToString(a []int64, delim string) string {
	return strings.Trim(strings.ReplaceAll(fmt.Sprint(a), " ", delim), "[]")
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
	// TODO: use strings.Join() instead
	for index, key := range slice {
		if index > 0 {
			str += `, `
		}
		str += QuoteLiteral(key)
	}
	return str
}

type BufferedWriter struct {
	File   *os.File
	Writer *bufio.Writer
}

func CreateBufferedWriter(s string) (w BufferedWriter, err error) {
	file, err := os.OpenFile(s, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o660)
	if err != nil {
		return
	}
	bufWriter := bufio.NewWriter(file)
	w = BufferedWriter{
		File:   file,
		Writer: bufWriter,
	}
	return
}

func (b BufferedWriter) Write(p []byte) (int, error) {
	return b.Writer.Write(p)
}

func (b BufferedWriter) GetFile() *os.File {
	return b.File
}

func (b BufferedWriter) Close() error {
	err := b.Writer.Flush()
	if err != nil {
		return err
	}
	return b.File.Close()
}

type GZipWriter struct {
	File      *os.File
	GzWriter  *gzip.Writer
	BufWriter *bufio.Writer
}

func CreateGZ(s string) (w GZipWriter, err error) {
	file, err := os.OpenFile(s, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o660)
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

func (w GZipWriter) WriteGZ(s string) error {
	count, err := w.BufWriter.WriteString(s)
	if err != nil {
		pkgLogger.Errorf(`[GZWriter]: Error writing string of length %d by GZipWriter.WriteGZ. Bytes written: %d. Error: %v`, len(s), count, err)
	}
	return err
}

func (w GZipWriter) Write(b []byte) (count int, err error) {
	count, err = w.BufWriter.Write(b)
	if err != nil {
		pkgLogger.Errorf(`[GZWriter]: Error writing bytes of length %d by GZipWriter.Write. Bytes written: %d. Error: %v`, len(b), count, err)
	}
	return
}

func (GZipWriter) WriteRow(_ []interface{}) error {
	return errors.New("not implemented")
}

func (w GZipWriter) Close() error {
	return w.CloseGZ()
}

func (w GZipWriter) GetLoadFile() *os.File {
	return w.File
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
	defer func() { _ = conn.Close() }()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP, nil
}

/*
RunWithTimeout runs provided function f until provided timeout d.
If the timeout is reached, onTimeout callback will be called.
*/
func RunWithTimeout(f, onTimeout func(), d time.Duration) {
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
	r := regexp.MustCompile("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-4[a-fA-F0-9]{3}-[89aAbB][a-fA-F0-9]{3}-[a-fA-F0-9]{12}$")
	return r.MatchString(uuid)
}

func FastUUID() uuid.UUID {
	return uuid.New()
}

func HasAWSRoleARNInConfig(configMap map[string]interface{}) bool {
	if configMap["iamRoleARN"] == nil {
		return false
	}
	iamRoleARN, ok := configMap["iamRoleARN"].(string)
	if !ok {
		return false
	}
	if iamRoleARN == "" {
		return false
	}
	return true
}

func HasAWSKeysInConfig(config interface{}) bool {
	configMap := config.(map[string]interface{})
	if configMap["useSTSTokens"] == false {
		return false
	}
	if configMap["accessKeyID"] == nil || configMap["accessKey"] == nil {
		return false
	}
	if configMap["accessKeyID"].(string) == "" || configMap["accessKey"].(string) == "" {
		return false
	}
	return true
}

func HasAWSRegionInConfig(config interface{}) bool {
	configMap := config.(map[string]interface{})
	if configMap["region"] == nil || configMap["region"].(string) == "" {
		return false
	}
	return true
}

func GetRudderObjectStorageAccessKeys() (accessKeyID, accessKey string) {
	return config.GetString("RUDDER_AWS_S3_COPY_USER_ACCESS_KEY_ID", ""), config.GetString("RUDDER_AWS_S3_COPY_USER_ACCESS_KEY", "")
}

func GetRudderObjectStoragePrefix() (prefix string) {
	return config.GetString("RUDDER_WAREHOUSE_BUCKET_PREFIX", config.GetNamespaceIdentifier())
}

func GetRudderObjectStorageConfig(prefixOverride string) (storageConfig map[string]interface{}) {
	// TODO: add error log if s3 keys are not available
	storageConfig = make(map[string]interface{})
	storageConfig["bucketName"] = config.GetString("RUDDER_WAREHOUSE_BUCKET", "rudder-warehouse-storage")
	storageConfig["accessKeyID"] = config.GetString("RUDDER_AWS_S3_COPY_USER_ACCESS_KEY_ID", "")
	storageConfig["accessKey"] = config.GetString("RUDDER_AWS_S3_COPY_USER_ACCESS_KEY", "")
	storageConfig["enableSSE"] = config.GetBool("RUDDER_WAREHOUSE_BUCKET_SSE", true)
	// set prefix from override for shared slave type nodes
	if prefixOverride != "" {
		storageConfig["prefix"] = prefixOverride
	} else {
		storageConfig["prefix"] = config.GetString("RUDDER_WAREHOUSE_BUCKET_PREFIX", config.GetNamespaceIdentifier())
	}
	return
}

func IsConfiguredToUseRudderObjectStorage(storageConfig map[string]interface{}) bool {
	if boolInterface, ok := storageConfig["useRudderStorage"]; ok {
		if b, ok := boolInterface.(bool); ok {
			return b
		}
	}
	return false
}

type ObjectStorageOptsT struct {
	Provider                    string
	Config                      interface{}
	UseRudderStorage            bool
	RudderStoragePrefixOverride string
	WorkspaceID                 string
}

func GetObjectStorageConfig(opts ObjectStorageOptsT) map[string]interface{} {
	objectStorageConfigMap := opts.Config.(map[string]interface{})
	if opts.UseRudderStorage {
		return GetRudderObjectStorageConfig(opts.RudderStoragePrefixOverride)
	}
	if opts.Provider == "S3" {
		clonedObjectStorageConfig := make(map[string]interface{})
		for k, v := range objectStorageConfigMap {
			clonedObjectStorageConfig[k] = v
		}
		clonedObjectStorageConfig["externalID"] = opts.WorkspaceID
		if !HasAWSRoleARNInConfig(objectStorageConfigMap) &&
			!HasAWSKeysInConfig(objectStorageConfigMap) {
			clonedObjectStorageConfig["accessKeyID"] = config.GetString("RUDDER_AWS_S3_COPY_USER_ACCESS_KEY_ID", "")
			clonedObjectStorageConfig["accessKey"] = config.GetString("RUDDER_AWS_S3_COPY_USER_ACCESS_KEY", "")
		}
		objectStorageConfigMap = clonedObjectStorageConfig
	}
	return objectStorageConfigMap
}

func GetSpacesLocation(location string) (region string) {
	r, _ := regexp.Compile(`\.*.*\.digitaloceanspaces\.com`)
	subLocation := r.FindString(location)
	regionTokens := strings.Split(subLocation, ".")
	if len(regionTokens) == 3 {
		region = regionTokens[0]
	}
	return region
}

// GetNodeID returns the nodeId of the current node
func GetNodeID() string {
	nodeID := config.MustGetString("INSTANCE_ID")
	return nodeID
}

// MakeRetryablePostRequest is Util function to make a post request.
func MakeRetryablePostRequest(url, endpoint string, data interface{}) (response []byte, statusCode int, err error) {
	backendURL := fmt.Sprintf("%s%s", url, endpoint)
	dataJSON, err := json.Marshal(data)
	if err != nil {
		return nil, -1, err
	}

	resp, err := retryablehttp.Post(backendURL, "application/json", dataJSON)
	if err != nil {
		return nil, -1, err
	}

	body, err := io.ReadAll(resp.Body)
	defer func() { httputil.CloseResponse(resp) }()

	pkgLogger.Debugf("Post request: Successful %s", string(body))
	return body, resp.StatusCode, nil
}

// GetMD5UUID hashes the given string into md5 and returns it as auuid
func GetMD5UUID(str string) (uuid.UUID, error) {
	// To maintain backward compatibility, we are using md5 hash of the string
	// We are mimicking github.com/gofrs/uuid behavior:
	//
	// md5Sum := md5.Sum([]byte(str))
	// u, err := uuid.FromBytes(md5Sum[:])

	// u.SetVersion(uuid.V4)
	// u.SetVariant(uuid.VariantRFC4122)

	// google/uuid doesn't allow us to modify the version and variant
	// so we are doing it manually, using gofrs/uuid library implementation.
	md5Sum := md5.Sum([]byte(str))
	// SetVariant: VariantRFC4122
	md5Sum[8] = md5Sum[8]&(0xff>>2) | (0x02 << 6)
	// SetVersion: Version 4
	version := byte(4)
	md5Sum[6] = (md5Sum[6] & 0x0f) | (version << 4)

	return uuid.FromBytes(md5Sum[:])
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

// GetMandatoryJSONFieldNames returns all the json field names defined against the json tag for each field.
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

func MaxInt(a, b int) int {
	if a >= b {
		return a
	}
	return b
}

// GetTagName gets the tag name using a uuid and name
func GetTagName(id string, names ...string) string {
	var truncatedNames string
	for _, name := range names {
		name = strings.ReplaceAll(name, ":", "-")
		truncatedNames += TruncateStr(name, 15) + "_"
	}
	return truncatedNames + TailTruncateStr(id, 6)
}

// UpdateJSONWithNewKeyVal enhances the json passed with key, val
func UpdateJSONWithNewKeyVal(params []byte, key string, val interface{}) []byte {
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

func isWarehouseMasterEnabled() bool {
	warehouseMode := config.GetString("Warehouse.mode", "embedded")
	return warehouseMode == config.EmbeddedMode ||
		warehouseMode == config.EmbeddedMasterMode
}

func GetWarehouseURL() (url string) {
	if isWarehouseMasterEnabled() {
		url = fmt.Sprintf(`http://localhost:%d`, config.GetInt("Warehouse.webPort", 8082))
	} else {
		url = config.GetString("WAREHOUSE_URL", "http://localhost:8082")
	}
	return
}

func GetDatabricksVersion() (version string) {
	url := fmt.Sprintf(`%s/databricksVersion`, GetWarehouseURL())
	req, err := http.NewRequest(http.MethodGet, url, http.NoBody)
	if err != nil {
		return
	}
	client := &http.Client{
		Timeout: config.GetDuration("HttpClient.timeout", 30, time.Second),
	}
	resp, err := client.Do(req)
	if err != nil {
		pkgLogger.Errorf("Unable to make a warehouse databricks build version call with error : %s", err.Error())
		return
	}
	if resp == nil {
		version = "No response from warehouse."
		return
	}
	defer func() { httputil.CloseResponse(resp) }()
	if resp.StatusCode == http.StatusOK {
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			pkgLogger.Errorf("Unable to read response into bytes with error : %s", err.Error())
			version = "Unable to read response from warehouse."
			return
		}
		version = string(bodyBytes)
	}
	return
}

func WithBugsnagForWarehouse(fn func() error) func() error {
	return func() error {
		ctx := bugsnag.StartSession(context.Background())
		defer BugsnagNotify(ctx, "Warehouse")()
		return fn()
	}
}

func BugsnagNotify(ctx context.Context, team string) func() {
	return func() {
		if r := recover(); r != nil {
			notifyOnce.Do(func() {
				defer bugsnag.AutoNotify(ctx, bugsnag.SeverityError, bugsnag.MetaData{
					"GoRoutines": {
						"Number": runtime.NumGoroutine(),
					},
					"Team": {
						"Name": team,
					},
				})
				RecordAppError(fmt.Errorf("%v", r))
				pkgLogger.Fatal(r)
				panic(r)
			})
		}
	}
}

func WithBugsnag(fn func() error) func() error {
	return func() error {
		ctx := bugsnag.StartSession(context.Background())
		defer BugsnagNotify(ctx, "Core")()
		return fn()
	}
}

func GetStringifiedData(data interface{}) string {
	if data == nil {
		return ""
	}
	switch d := data.(type) {
	case string:
		return d
	default:
		dataBytes, err := json.Marshal(d)
		if err != nil {
			return fmt.Sprint(d)
		}
		return string(dataBytes)
	}
}

// MergeMaps merging with one level of nesting.
func MergeMaps(maps ...map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for _, m := range maps {
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}

// NestedMapLookup
// m:  a map from strings to other maps or values, of arbitrary depth
// ks: successive keys to reach an internal or leaf node (variadic)
// If an internal node is reached, will return the internal map
//
// Returns: (Exactly one of these will be nil)
// rval: the target node (if found)
// err:  an error created by fmt.Errorf
func NestedMapLookup(m map[string]interface{}, ks ...string) (rval interface{}, err error) {
	var ok bool

	if len(ks) == 0 { // degenerate input
		return nil, fmt.Errorf("NestedMapLookup needs at least one key")
	}
	if rval, ok = m[ks[0]]; !ok {
		return nil, fmt.Errorf("key not found; remaining keys: %v", ks)
	} else if len(ks) == 1 { // we've reached the final key
		return rval, nil
	} else if m, ok = rval.(map[string]interface{}); !ok {
		return nil, fmt.Errorf("malformed structure at %#v", rval)
	} else { // 1+ more keys
		return NestedMapLookup(m, ks[1:]...)
	}
}

// GetJsonSchemaDTFromGoDT returns the json schema supported data types from go lang supported data types.
// References:
// 1. Go supported types: https://golangbyexample.com/all-data-types-in-golang-with-examples/
// 2. Json schema supported types: https://json-schema.org/understanding-json-schema/reference/type.html
func GetJsonSchemaDTFromGoDT(goType string) string {
	switch goType {
	case "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64":
		return "integer"
	case "float32", "float64":
		return "number"
	case "string":
		return "string"
	case "bool":
		return "boolean"
	}
	return "object"
}

func SortMap(inputMap map[string]metric.MovingAverage) []string {
	pairArr := make(pairList, len(inputMap))

	i := 0
	for k, v := range inputMap {
		pairArr[i] = pair{k, v.Value()}
		i++
	}

	sort.Sort(pairArr)
	var sortedWorkspaceList []string
	// p is sorted
	for _, k := range pairArr {
		// Workspace ID - RS Check
		sortedWorkspaceList = append(sortedWorkspaceList, k.key)
	}
	return sortedWorkspaceList
}

// SleepCtx sleeps for the given duration or until the context is canceled.
//
//	the context error is returned if context is canceled.
func SleepCtx(ctx context.Context, delay time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(delay):
		return nil
	}
}

func Unique(stringSlice []string) []string {
	keys := make(map[string]struct{})
	var list []string
	for _, entry := range stringSlice {
		if _, ok := keys[entry]; !ok {
			keys[entry] = struct{}{}
			list = append(list, entry)
		}
	}
	return list
}

func UseFairPickup() bool {
	return config.GetBool("JobsDB.fairPickup", false) || config.GetBool("EnableMultitenancy", false)
}

// MapLookup returns the value of the key in the map, or nil if the key is not present.
//
// If multiple keys are provided then it looks for nested maps recursively.
func MapLookup(mapToLookup map[string]interface{}, keys ...string) interface{} {
	if len(keys) == 0 {
		return nil
	}
	if val, ok := mapToLookup[keys[0]]; ok {
		if len(keys) == 1 {
			return val
		}
		nextMap, ok := val.(map[string]interface{})
		if !ok {
			return nil
		}
		return MapLookup(nextMap, keys[1:]...)
	}
	return nil
}

func CopyStringMap(originalMap map[string]string) map[string]string {
	newMap := make(map[string]string)
	for key, value := range originalMap {
		newMap[key] = value
	}
	return newMap
}
