package client

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/credentials-go/credentials"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// ODPSClient is the MaxCompute variant of Client.
const ODPSClient = "ODPSClient"

const (
	odpsRequestTimeout = 30 * time.Second
)

var (
	odpsQueryTimeout = 5 * time.Minute
	odpsPollInterval = time.Second
)

var (
	odpsProjectName = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]{0,127}$`)
	odpsRegionName  = regexp.MustCompile(`^[a-z]{2}-[a-z0-9]+(?:-[a-z0-9]+)*$`)
)

// NewODPSClient validates the project, region and optional API endpoint before
// the SDK can make requests. Only the same region's public MaxCompute endpoint
// over HTTPS is accepted. Use NewODPSClientWithEndpoint for a configured endpoint.
func NewODPSClient(project, region string, credential credentials.Credential) (*odps.Odps, error) {
	return NewODPSClientWithEndpoint(project, region, "", credential)
}

// NewODPSClientWithEndpoint accepts an empty endpoint (the regional default) or
// the exact public API URL for region. It guards credentials regardless of
// whether the provider returns STS tokens or long-lived access keys.
func NewODPSClientWithEndpoint(project, region, endpoint string, credential credentials.Credential) (*odps.Odps, error) {
	if !odpsProjectName.MatchString(project) || !odpsRegionName.MatchString(region) {
		return nil, errors.New("invalid ODPS project or region")
	}
	if !strings.HasPrefix(region, "cn-") && !strings.HasPrefix(region, "ap-") &&
		!strings.HasPrefix(region, "eu-") && !strings.HasPrefix(region, "us-") &&
		!strings.HasPrefix(region, "me-") {
		return nil, errors.New("unsupported ODPS region")
	}
	allowedEndpoint := "https://service." + region + ".maxcompute.aliyun.com/api"
	if endpoint == "" {
		endpoint = allowedEndpoint
	}
	if endpoint != allowedEndpoint {
		return nil, errors.New("invalid ODPS endpoint")
	}
	if credential == nil {
		return nil, errors.New("ODPS credentials unavailable")
	}
	instance := odps.NewOdps(account.NewStsAccountWithCredential(GuardODPSCredentials(credential), region), endpoint)
	instance.SetDefaultProjectName(project)
	instance.SetHttpTimeout(odpsRequestTimeout)
	instance.SetTcpConnectTimeout(odpsRequestTimeout)
	instance.Options.RegionId = region
	return instance, nil
}

func NewODPSWarehouseClient(project, region string, credential credentials.Credential) (*Client, error) {
	instance, err := NewODPSClient(project, region, credential)
	if err != nil {
		return nil, err
	}
	return &Client{Type: ODPSClient, ODPS: instance}, nil
}

// RunODPSSQL submits SQL with the required session hints and waits for all
// tasks to succeed. The SDK has no context-aware requests, so the ODPS client's
// HTTP timeout bounds individual calls; the context bounds polling between them.
// The returned instance can be used to fetch task summaries or query results.
func RunODPSSQL(ctx context.Context, service *odps.Odps, statement string, timeout, interval time.Duration) (*odps.Instance, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if service == nil {
		return nil, errors.New("ODPS client unavailable")
	}
	if strings.TrimSpace(statement) == "" {
		return nil, errors.New("ODPS statement is empty")
	}
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	if interval <= 0 {
		interval = time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Values are server-side ODPS SQL job hints, not client-side timeouts.
	instance, err := service.ExecSQlWithHints(statement, odpsQueryHints())
	if err := ctx.Err(); err != nil {
		if instance != nil && instance.IsAsync() {
			go func() { _ = instance.Terminate() }()
		}
		return nil, err
	}
	if err != nil {
		return nil, ODPSSafeError(err)
	}
	if instance == nil {
		return nil, errors.New("ODPS instance unavailable")
	}
	if instance.IsSync() {
		for _, task := range instance.TaskResults() {
			if task.Status != odps.TaskSuccess {
				return nil, errors.New("ODPS query failed")
			}
		}
		return instance, nil
	}
	// Termination has no context-aware SDK API; do not block the caller.
	defer func() {
		if ctx.Err() != nil {
			go func() { _ = instance.Terminate() }()
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		loadErr := instance.Load()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if loadErr != nil {
			return nil, ODPSSafeError(loadErr)
		}
		if instance.Status() == odps.InstanceTerminated || instance.Status() == odps.InstanceSuspended {
			return nil, errors.New("ODPS query terminated")
		}
		tasks, err := instance.GetTasks()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil {
			return nil, ODPSSafeError(err)
		}
		finished := len(tasks) > 0
		for _, task := range tasks {
			switch task.Status {
			case odps.TaskFailed, odps.TaskCancelled, odps.TaskSuspended:
				return nil, errors.New("ODPS query failed")
			case odps.TaskSuccess:
			default:
				finished = false
			}
		}
		if finished {
			return instance, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
	}
}

// odpsQuery uses the same bounded job runner as the MaxCompute manager.
func (cl *Client) odpsQuery(statement string) (warehouseutils.QueryResult, error) {
	var result warehouseutils.QueryResult
	ctx, cancel := context.WithTimeout(context.Background(), odpsQueryTimeout)
	defer cancel()
	instance, err := RunODPSSQL(ctx, cl.ODPS, statement, odpsQueryTimeout, odpsPollInterval)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return result, errors.New("ODPS query timed out")
		}
		return result, err
	}
	if ctx.Err() != nil {
		return result, errors.New("ODPS query timed out")
	}
	results, err := instance.GetResult()
	if ctx.Err() != nil {
		return result, errors.New("ODPS query timed out")
	}
	if err != nil {
		return result, ODPSSafeError(err)
	}
	if len(results) == 0 {
		return result, nil // DDL/DML may not produce a result set.
	}
	reader := csv.NewReader(strings.NewReader(results[0].Content()))
	reader.FieldsPerRecord = -1
	result.Columns, err = reader.Read()
	if errors.Is(err, io.EOF) {
		return warehouseutils.QueryResult{}, nil
	}
	if err != nil {
		return warehouseutils.QueryResult{}, errors.New("invalid ODPS result")
	}
	for {
		row, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			return result, nil
		}
		if readErr != nil {
			return warehouseutils.QueryResult{}, errors.New("invalid ODPS result")
		}
		result.Values = append(result.Values, row)
	}
}

func odpsQueryHints() map[string]string {
	return map[string]string{
		"odps.sql.type.system.odps2": "true",
		"odps.namespace.schema":      "true",
		"odps.sql.timezone":          "UTC",
	}
}

// ODPSSafeError deliberately does not wrap arbitrary SDK errors: HTTP bodies,
// task text and request URLs can contain credentials and SQL literals. It can
// also be used by callers making ODPS SDK requests outside Client.Query.
func ODPSSafeError(err error) error {
	if err == nil {
		return nil
	}
	// HttpError is a value in the SDK, but wrapped pointers are possible too.
	// Never inspect its Error(), Status, Body or server-provided Message.
	if httpErr, ok := errors.AsType[restclient.HttpError](err); ok {
		return odpsHTTPStatusError(httpErr.StatusCode, httpErr.ErrorMessage)
	}
	if httpErr, ok := errors.AsType[*restclient.HttpError](err); ok && httpErr != nil {
		return odpsHTTPStatusError(httpErr.StatusCode, httpErr.ErrorMessage)
	}
	if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() {
		return errors.New("ODPS request timed out")
	}
	// The SDK sometimes returns raw task text on synchronous job failure.
	// Inspect it only for a fixed vocabulary; never return the message itself.
	return odpsErrorCode(err.Error())
}

func odpsHTTPStatusError(status int, message *restclient.ErrorMessage) error {
	switch status {
	case 401, 403:
		return errors.New("ODPS authentication or permission denied")
	case 404:
		return errors.New("ODPS resource not found")
	case 429, 503:
		return errors.New("ODPS service temporarily unavailable")
	}
	if message != nil {
		// Only classify known codes; do not return server-provided text.
		if classified := odpsKnownCode(message.ErrorCode); classified != nil {
			return classified
		}
	}
	if status < 100 || status > 599 {
		return errors.New("ODPS request failed")
	}
	return fmt.Errorf("ODPS request failed (status %d)", status)
}

func odpsErrorCode(text string) error {
	if classified := odpsKnownCode(text); classified != nil {
		return classified
	}
	return errors.New("ODPS request failed")
}

func odpsKnownCode(text string) error {
	text = strings.ToLower(text)
	switch {
	case strings.Contains(text, "accessdenied"), strings.Contains(text, "permission denied"),
		strings.Contains(text, "not authorized"), strings.Contains(text, "invalid accesskey"),
		strings.Contains(text, "signaturedoesnotmatch"):
		return errors.New("ODPS authentication or permission denied")
	case strings.Contains(text, "projectnotfound"), strings.Contains(text, "tablenotfound"),
		strings.Contains(text, "schemanotfound"):
		return errors.New("ODPS resource not found")
	}
	return nil
}
