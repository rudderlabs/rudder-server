package redis

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

var abortableErrors = []string{}

type RedisManager struct {
	logger        logger.Logger
	clusterMode   bool
	config        types.ConfigT
	client        *redis.Client
	clusterClient *redis.ClusterClient
}

func init() {
	abortableErrors = []string{"connection refused", "invalid password"}
}

func NewRedisManager(config types.ConfigT) *RedisManager {
	redisMgr := &RedisManager{
		config: config,
		logger: logger.NewLogger().Child("kvstoremgr.redis"),
	}
	redisMgr.CreateClient()
	return redisMgr
}

func (m *RedisManager) GetClient() redis.Cmdable {
	if m.clusterMode {
		return m.clusterClient
	}
	return m.client
}

func (m *RedisManager) CreateClient() {
	var ok bool
	if m.clusterMode, ok = m.config["clusterMode"].(bool); !ok {
		// setting redis to cluster mode by default if setting missing in config
		m.clusterMode = true
	}
	shouldSecureConn, _ := m.config["secure"].(bool)
	addr, _ := m.config["address"].(string)
	password, _ := m.config["password"].(string)

	tlsConfig := tls.Config{}
	if shouldSecureConn {
		if skipServerCertCheck, ok := m.config["skipVerify"].(bool); ok && skipServerCertCheck {
			tlsConfig.InsecureSkipVerify = true
		}
		if serverCACert, ok := m.config["caCertificate"].(string); ok && len(strings.TrimSpace(serverCACert)) > 0 {
			caCert := []byte(serverCACert)
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tlsConfig.RootCAs = caCertPool
		}
	}

	if m.clusterMode {
		addrs := strings.Split(addr, ",")
		for i := range addrs {
			addrs[i] = strings.TrimSpace(addrs[i])
		}
		opts := redis.ClusterOptions{
			Addrs:    addrs,
			Password: password,
		}
		if shouldSecureConn {
			opts.TLSConfig = &tlsConfig
		}
		m.clusterClient = redis.NewClusterClient(&opts)
	} else {
		var db int
		if dbStr, ok := m.config["database"].(string); ok {
			db, _ = strconv.Atoi(dbStr)
		}
		opts := redis.Options{
			Addr:     strings.TrimSpace(addr),
			Password: password,
			DB:       db,
		}
		if shouldSecureConn {
			opts.TLSConfig = &tlsConfig
		}
		m.client = redis.NewClient(&opts)
	}
}

func (m *RedisManager) Close() error {
	if m.clusterMode {
		return m.clusterClient.Close()
	}
	return m.client.Close()
}

func (m *RedisManager) HMSet(key string, fields map[string]interface{}) (err error) {
	ctx := context.Background()
	if m.clusterMode {
		_, err = m.clusterClient.HMSet(ctx, key, fields).Result()
	} else {
		_, err = m.client.HMSet(ctx, key, fields).Result()
	}
	return err
}

func (*RedisManager) StatusCode(err error) int {
	if err == nil {
		return http.StatusOK
	}
	statusCode := http.StatusInternalServerError
	errorString := err.Error()
	for _, s := range abortableErrors {
		if strings.Contains(errorString, s) {
			statusCode = 400
			break
		}
	}
	return statusCode
}

func (m *RedisManager) DeleteKey(key string) (err error) {
	ctx := context.Background()
	if m.clusterMode {
		_, err = m.clusterClient.Del(ctx, key).Result()
	} else {
		_, err = m.client.Del(ctx, key).Result()
	}
	return err
}

func (m *RedisManager) HMGet(key string, fields ...string) (result []interface{}, err error) {
	ctx := context.Background()
	if m.clusterMode {
		result, err = m.clusterClient.HMGet(ctx, key, fields...).Result()
	} else {
		result, err = m.client.HMGet(ctx, key, fields...).Result()
	}
	return result, err
}

func (m *RedisManager) HGetAll(key string) (result map[string]string, err error) {
	ctx := context.Background()
	if m.clusterMode {
		result, err = m.clusterClient.HGetAll(ctx, key).Result()
	} else {
		result, err = m.client.HGetAll(ctx, key).Result()
	}
	return result, err
}

func (m *RedisManager) HSet(hash, key string, value interface{}) (err error) {
	ctx := context.Background()
	if m.clusterMode {
		_, err = m.clusterClient.HSet(ctx, hash, key, value).Result()
	} else {
		_, err = m.client.HSet(ctx, hash, key, value).Result()
	}
	return err
}

type NonRootInsertParams struct {
	valueInRedis string
	Path         string
	JsonVal      gjson.Result
}
type NonRootInsertReturn struct {
	SetArgsPath string
	MergeTo     string
	MergeFrom   interface{}
}

func (m *RedisManager) HandleNonRootInsert(p NonRootInsertParams) (*NonRootInsertReturn, error) {
	path := p.Path
	insertValue := p.JsonVal

	// Case-1: path is not sent, userValue is empty or unavailable in Redis -- Done

	// Case-2.1: path is sent but one of parents is empty, userValue is empty or unavailable in Redis -- not possible
	// Case-2.2: path is sent but child is empty, userValue is empty or unavailable in Redis -- not possible
	// Case-2.3: path is sent but child is non-empty, userValue is empty or unavailable in Redis -- not possible
	// Case-2.4: path is sent but first parent is empty, userValue is empty or unavailable in Redis -- Done
	// Case 4: path is sent, userValue is empty -- Done

	// Case-3.1: path is sent but one of parents is empty, userValue is non-empty or available in Redis -- Done
	// Case-3.2: path is sent but child is empty, userValue is non-empty or available in Redis -- Done
	// Case-3.3: path is sent but child is non-empty, userValue is non-empty or available in Redis -- Done
	// Case-3.4: path is sent but first parent is empty, userValue is empty or unavailable in Redis -- Done

	paths := strings.Split(path, ".")
	insertMap := make(map[string]interface{})

	var lastNonEmptyVal interface{}
	if p.valueInRedis == "" {
		p.valueInRedis = "{}"
	}

	err := json.Unmarshal([]byte(p.valueInRedis), &lastNonEmptyVal)
	if err != nil {
		return nil, err
	}
	var nestedSearchKey string
	var lastNonEmptyKeyIdx int
	for i, kp := range paths {
		nestedSearchKey = strings.Join([]string{nestedSearchKey, kp}, ".")
		if i == 0 {
			nestedSearchKey = kp
		}
		res := gjson.Get(p.valueInRedis, nestedSearchKey)
		if !res.Exists() {
			break
		}
		lastNonEmptyKeyIdx += 1
		lastNonEmptyVal = res.Value()
	}
	nonEmptyPaths := paths[:lastNonEmptyKeyIdx]
	setArgsPath := "$"
	if len(nonEmptyPaths) > 0 {
		setArgsPath = strings.Join([]string{setArgsPath, strings.Join(nonEmptyPaths, ".")}, ".")
	}

	lastNonEmptyValBytes, marshalErr := json.Marshal(lastNonEmptyVal)
	if marshalErr != nil {
		m.logger.Error("marshal of non empty value: %+v", marshalErr.Error())
		return nil, marshalErr
	}

	var ok bool
	if path == "" || len(paths[lastNonEmptyKeyIdx:]) == 0 {
		// when path is not sent at all
		// when there is value in last child of "path"
		insertMap, ok = insertValue.Value().(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("cannot be type-casted to map:%s", insertValue.String())
		}
		return &NonRootInsertReturn{
			SetArgsPath: setArgsPath,
			MergeTo:     string(lastNonEmptyValBytes),
			MergeFrom:   insertMap,
		}, nil
	}

	misc.FormNestedMap(insertMap, paths[lastNonEmptyKeyIdx:], insertValue.Value())

	return &NonRootInsertReturn{
		SetArgsPath: setArgsPath,
		MergeTo:     string(lastNonEmptyValBytes),
		MergeFrom:   insertMap,
	}, nil
}

func (m *RedisManager) setArgsForMergeStrategy(inputArgs setArguments) ([]string, error) {
	key := inputArgs.key
	path := inputArgs.path
	jsonVal := inputArgs.jsonVal

	actualPath := "$" // root insert
	isRootInsert := path == ""
	if !isRootInsert {
		actualPath = fmt.Sprintf("$.%s", path)
	}
	args := []string{key, actualPath, jsonVal.String()}
	redisValueForKey, err := m.GetClient().JSONGet(context.Background(), key).Result()
	if err != nil {
		return nil, err
	}

	valueToBeInserted := "{}" // value to which the transformed value should be merged which will be inserted into Redis
	if isRootInsert && redisValueForKey != "" {
		valueToBeInserted = redisValueForKey
	}
	var mergeFrom interface{} = jsonVal.Value() // transformed value
	if !isRootInsert {
		ret, err := m.HandleNonRootInsert(NonRootInsertParams{
			valueInRedis: redisValueForKey,
			Path:         path,
			JsonVal:      jsonVal,
		})
		if err != nil {
			return nil, err
		}
		mergeFrom = ret.MergeFrom
		valueToBeInserted = ret.MergeTo
		args[1] = ret.SetArgsPath
	}

	switch jsonValue := mergeFrom.(type) {
	case map[string]interface{}:
		var setErr error
		for k, mapV := range jsonValue {
			valueToBeInserted, setErr = sjson.Set(valueToBeInserted, k, mapV)
			if setErr != nil {
				return nil, fmt.Errorf("problem while setting key(%s): %w", k, setErr)
			}
		}
	}
	args[2] = valueToBeInserted
	return args, nil
}

func (m *RedisManager) setArgsForReplaceStrategy(inputArgs setArguments) ([]string, error) {
	key := inputArgs.key
	path := inputArgs.path
	jsonVal := inputArgs.jsonVal

	actualPath := "$" // root insert
	if path != "" {
		actualPath = fmt.Sprintf("$.%s", path)
	}
	args := []string{key, actualPath, jsonVal.String()}

	// Insert a value into a path for a key
	// 1. Validate if key is present
	// 2. If key is not present, then insert value into the path
	//    Execute the following command:
	//    > JSON.SET key $ {[path]: value}
	// Limitation: It can only insert values at a single level
	// Example of limitation:
	//    - JSON.SET key $.k1.k2.k3 '{"A":1}' cannot be done and would be converted to
	//      JSON.SET key $ '{"k1.k2.k3": {"A": 1}}'
	// Example of correct usage:
	//    - JSON.SET key $.k1 '{"A":1}' can be done and would be converted to
	//      JSON.SET key $ '{"k1": {"A": 1}}'
	if actualPath != "$" {
		v, err := m.GetClient().JSONGet(context.Background(), key).Result()
		if err != nil {
			return nil, err
		}
		if v == "" {
			// key is new one but we need to insert a value other than root
			// formulate {[path]: value}
			mapStr, err := json.Marshal(map[string]interface{}{
				path: jsonVal.Value(),
			})
			if err != nil {
				return nil, err
			}
			// data is not present in key
			args = []string{key, "$", string(mapStr)} // arguments to insert data
		}
	}
	return args, nil
}

type setArguments struct {
	key     string
	path    string
	jsonVal gjson.Result
}

func (m *RedisManager) ExtractJSONSetArgs(transformedData json.RawMessage, config map[string]interface{}) ([]string, error) {
	key := gjson.GetBytes(transformedData, "message.key").String()
	path := gjson.GetBytes(transformedData, "message.path").String()
	jsonVal := gjson.GetBytes(transformedData, "message.value")

	if m.shouldMerge(config) {
		return m.setArgsForMergeStrategy(setArguments{
			key:     key,
			path:    path,
			jsonVal: jsonVal,
		})
	}
	return m.setArgsForReplaceStrategy(setArguments{
		key:     key,
		path:    path,
		jsonVal: jsonVal,
	})
}

func (m *RedisManager) SendDataAsJSON(jsonData json.RawMessage, config map[string]interface{}) (interface{}, error) {
	nmSetArgs, err := m.ExtractJSONSetArgs(jsonData, config)
	if err != nil {
		return nil, err
	}
	redisClient := m.GetClient()
	ctx := context.Background()
	val, err := redisClient.JSONSet(ctx, nmSetArgs[0], nmSetArgs[1], nmSetArgs[2]).Result()
	if err != nil {
		return nil, fmt.Errorf("setting key:(%s %s %s): %w", nmSetArgs[0], nmSetArgs[1], nmSetArgs[2], err)
	}

	return val, err
}

func (*RedisManager) shouldMerge(config map[string]interface{}) bool {
	if mergeStrategyI, ok := config["shouldMerge"]; ok {
		if mergeStrategy, ok := mergeStrategyI.(bool); ok {
			return mergeStrategy
		}
	}
	return false
}

func (*RedisManager) ShouldSendDataAsJSON(config map[string]interface{}) bool {
	var dataAsJSON bool
	if dataAsJSONI, ok := config["useJSONModule"]; ok {
		if dataAsJSON, ok = dataAsJSONI.(bool); ok {
			return dataAsJSON
		}
	}
	return dataAsJSON
}
