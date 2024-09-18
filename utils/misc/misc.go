package misc

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/hex"
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
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"

	"github.com/araddon/dateparse"
	"github.com/cenkalti/backoff"
	"github.com/google/uuid"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/types"
)

var (
	reservedFolderPaths []*RFP

	regexGwHa               = regexp.MustCompile(`^.*-gw-ha-\d+-\w+-\w+$`)
	regexGwNonHaOrProcessor = regexp.MustCompile(`^.*-\d+$`)
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
	reservedFolderPaths = GetReservedFolderPaths()
}

func BatchDestinations() []string {
	batchDestinations := []string{"S3", "GCS", "MINIO", "RS", "BQ", "AZURE_BLOB", "SNOWFLAKE", "POSTGRES", "CLICKHOUSE", "DIGITAL_OCEAN_SPACES", "MSSQL", "AZURE_SYNAPSE", "S3_DATALAKE", "MARKETO_BULK_UPLOAD", "GCS_DATALAKE", "AZURE_DATALAKE", "DELTALAKE", "BINGADS_AUDIENCE", "ELOQUA", "YANDEX_METRICA_OFFLINE_EVENTS", "SFTP", "BINGADS_OFFLINE_CONVERSIONS", "KLAVIYO_BULK_UPLOAD", "LYTICS_BULK_UPLOAD", "SNOWPIPE_STREAMING"}
	return batchDestinations
}

func GetHash(s string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return int(h.Sum32())
}

// GetMD5Hash returns EncodeToString(md5 hash of the input string)
func GetMD5Hash(input string) string {
	hash := md5.Sum([]byte(input)) // skipcq: GO-S1023
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

// RemoveContents removes all the contents of the directory
func RemoveContents(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
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
				fmt.Printf("RUDDER_TMPDIR not found, falling back to %v\n", fallbackPath)
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

//  Returns chronological timestamp of the event using the formula
//  timestamp = receivedAt - (sentAt - originalTimestamp)
func GetChronologicalTimeStamp(receivedAt, sentAt, originalTimestamp time.Time) time.Time {
	return receivedAt.Add(-sentAt.Sub(originalTimestamp))
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
	return errors.Join(errorsToJoin...)
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

type MapLookupError struct {
	SearchKey string // indicates the searchkey which is not present in the map
	Err       error  // contains the error occurred string while looking up the key in the map
	Level     int    // indicates the nesting level at which error has occurred
}

func (e *MapLookupError) Error() string {
	return e.Err.Error()
}

// NestedMapLookup
// m:  a map from strings to other maps or values, of arbitrary depth
// ks: successive keys to reach an internal or leaf node (variadic)
// If an internal node is reached, will return the internal map
//
// Returns: (Exactly one of these will be nil)
// rval: the target node (if found)
// err:  an error created by fmt.Errorf
func NestedMapLookup(m map[string]interface{}, ks ...string) (interface{}, *MapLookupError) {
	var lookupWithLevel func(map[string]interface{}, int, ...string) (interface{}, *MapLookupError)

	lookupWithLevel = func(searchMap map[string]interface{}, level int, keys ...string) (rval interface{}, err *MapLookupError) {
		var ok bool
		if len(keys) == 0 { // degenerate input
			return nil, &MapLookupError{Err: fmt.Errorf("NestedMapLookup needs at least one key"), Level: level}
		}
		if rval, ok = searchMap[keys[0]]; !ok {
			return nil, &MapLookupError{Err: fmt.Errorf("key: %v not found", keys[0]), SearchKey: keys[0], Level: level}
		} else if len(keys) == 1 { // we've reached the final key
			return rval, nil
		} else if searchMap, ok = rval.(map[string]interface{}); !ok {
			return nil, &MapLookupError{Err: fmt.Errorf("malformed structure at %#v", rval), SearchKey: keys[0], Level: level}
		}
		// 1+ more keys
		level += 1
		return lookupWithLevel(searchMap, level, keys[1:]...)
	}
	return lookupWithLevel(m, 0, ks...)
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

func GetDiskUsageOfFile(path string) (int64, error) {
	// Notes
	// 1. stat.Blocks is the number of stat.Blksize blocks allocated to the file
	// 2. stat.Blksize is the filesystem block size for this filesystem
	// 3. We compute the actual disk usage of a (sparse) file by multiplying the number of blocks allocated to the file with the block size. This computes a different value than the one returned by stat.Size particularly for sparse files.
	var stat syscall.Stat_t
	err := syscall.Stat(path, &stat)
	if err != nil {
		return 0, fmt.Errorf("unable to get file size %w", err)
	}
	return int64(stat.Blksize) * stat.Blocks / 8, nil //nolint:unconvert // In amd64 architecture stat.Blksize is int64 whereas in arm64 it is int32
}

// DiskUsage calculates the path's disk usage recursively in bytes. If exts are provided, only files with matching extensions will be included in the result.
func DiskUsage(path string, ext ...string) (int64, error) {
	var totSize int64
	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		size, _ := GetDiskUsageOfFile(path)
		if len(ext) == 0 {
			totSize += size
		} else {
			for _, e := range ext {
				if filepath.Ext(path) == e {
					totSize += size
				}
			}
		}
		return nil
	})
	return totSize, err
}

func GetBadgerDBUsage(dir string) (int64, int64, int64, error) {
	// Notes
	// Instead of using BadgerDB's internal function to get the disk usage, we are writing our own implementation because of the following reasons:
	// 1. BadgerDB internally creates a sparse memory backed file to store the data
	// 2. The size returned by the filepath.Walk used internally gives a misleading size because the file is mostly empty and doesn't consume any disk space
	lsmSize, err := DiskUsage(dir, ".sst")
	if err != nil {
		return 0, 0, 0, err
	}
	vlogSize, err := DiskUsage(dir, ".vlog")
	if err != nil {
		return 0, 0, 0, err
	}
	totSize, err := DiskUsage(dir)
	if err != nil {
		return 0, 0, 0, err
	}
	return lsmSize, vlogSize, totSize, nil
}

func GetInstanceID() string {
	instance := config.GetString("INSTANCE_ID", "")
	instanceArr := strings.Split(instance, "-")
	length := len(instanceArr)
	// This handles 2 kinds of server instances
	// a) Processor OR Gateway running in non HA mod where the instance name ends with the index
	// b) Gateway running in HA mode, where the instance name is of the form *-gw-ha-<index>-<statefulset-id>-<pod-id>
	if (regexGwHa.MatchString(instance)) && (length > 3) {
		return instanceArr[length-3]
	} else if (regexGwNonHaOrProcessor.MatchString(instance)) && (length > 1) {
		return instanceArr[length-1]
	}
	return ""
}
