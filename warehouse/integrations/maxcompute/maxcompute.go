package maxcompute

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/credentials-go/credentials"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/types"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const provider = warehouseutils.MAXCOMPUTE

var (
	errNotImplemented = errors.New(warehouseutils.NotImplementedErrorCode)
	identifierPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	errorsMappings    = []model.JobError{
		{Type: model.PermissionError, Format: regexp.MustCompile(`(?i)(accessdenied|permission denied|not authorized|invalid accesskey|signaturedoesnotmatch)`)},
		{Type: model.ResourceNotFoundError, Format: regexp.MustCompile(`(?i)(projectnotfound|tablenotfound|schemanotfound)`)},
	}
)

// MaxCompute stores RudderStack tables in the configured ODPS project. A
// separate schema is optional: in one-schema projects the project itself is
// the warehouse namespace. All SDK network operations have bounded request
// timeouts; SQL tasks are polled with context cancellation.
type MaxCompute struct {
	warehouse      model.Warehouse
	uploader       warehouseutils.Uploader
	conf           *config.Config
	odps           *odps.Odps
	logger         logger.Logger
	connectTimeout time.Duration
	queryTimeout   time.Duration
	pollInterval   time.Duration

	// exec and catalog are replaceable in tests. The production implementations
	// use the SDK rather than parsing SHOW output for schema discovery.
	exec       func(context.Context, string) error
	query      func(context.Context, string) (warehouseutils.QueryResult, error)
	catalog    func(context.Context) (model.Schema, error)
	partitions func(context.Context, string) ([]string, error) // replaceable in tests; SDK lists staging partitions in production.
	principal  func(context.Context) (string, error)           // replaceable in tests; SDK security WHOAMI in production.
}

func New(conf *config.Config, log logger.Logger) *MaxCompute {
	m := &MaxCompute{logger: log.Child("integrations").Child("maxcompute"), conf: conf}
	m.connectTimeout = conf.GetDurationVar(30, time.Second, "Warehouse.maxcompute.connectionTimeout")
	m.queryTimeout = conf.GetDurationVar(5, time.Minute, "Warehouse.maxcompute.queryTimeout")
	m.pollInterval = conf.GetDurationVar(1, time.Second, "Warehouse.maxcompute.pollInterval")
	return m
}

func (m *MaxCompute) init(warehouse model.Warehouse) error {
	m.warehouse = warehouse
	settings := warehouse.Destination.Config
	project, _ := settings["project"].(string)
	region, _ := settings["region"].(string)
	if project == "" || warehouse.Namespace != "" && warehouse.Namespace != project && !identifierPattern.MatchString(warehouse.Namespace) {
		return errors.New("invalid MaxCompute project or schema")
	}
	if err := warehouseutils.ValidateMaxComputeSettings(settings, warehouse.Destination.WorkspaceID); err != nil {
		return fmt.Errorf("validating MaxCompute OSS configuration: %w", err)
	}
	role, _ := warehouseutils.MaxComputeRoleMode(settings)
	endpoint, ok := settings["endpoint"].(string)
	if !ok || endpoint == "" {
		return errors.New("MaxCompute endpoint is required")
	}
	if err := client.ValidateODPSEndpoint(project, region, endpoint); err != nil {
		return err
	}
	var instance *odps.Odps
	if role {
		// A RAM role uses server-configured caller credentials and Alibaba's
		// refreshable credential chain, never destination-supplied keys.
		arn, _ := settings["iamRoleARN"].(string)
		if m.conf == nil {
			return errors.New("MaxCompute role caller credentials unavailable")
		}
		id := m.conf.GetStringVar("", "FileManager.ALIBABA_OSS.callerAccessKeyID")
		secret := m.conf.GetStringVar("", "FileManager.ALIBABA_OSS.callerAccessKeySecret")
		if id == "" || secret == "" {
			return errors.New("MaxCompute role caller credentials unavailable")
		}
		// ODPS and OSS both assume the configured RAM role with server-side
		// caller credentials, never with keys supplied by the destination.
		kind := "ram_role_arn"
		config := (&credentials.Config{Type: &kind}).SetRoleArn(arn).SetRoleSessionName("rudder-warehouse").SetAccessKeyId(id).SetAccessKeySecret(secret)
		externalID := warehouse.Destination.WorkspaceID
		config.ExternalId = &externalID
		cred, err := credentials.NewCredential(config)
		if err != nil {
			return errors.New("MaxCompute role credentials unavailable")
		}
		instance, err = client.NewODPSClientWithEndpoint(project, region, endpoint, cred)
		if err != nil {
			return err
		}
	} else {
		id, _ := settings["accessKeyId"].(string)
		secret, _ := settings["accessKeySecret"].(string)
		var err error
		instance, err = client.NewODPSClientWithAccessKey(project, region, endpoint, id, secret)
		if err != nil {
			return err
		}
	}
	m.applyConnectionTimeout(instance)
	if warehouse.Namespace != "" && warehouse.Namespace != project {
		instance.SetCurrentSchemaName(warehouse.Namespace)
	}
	m.odps = instance
	return nil
}

func (m *MaxCompute) Setup(_ context.Context, warehouse model.Warehouse, uploader warehouseutils.Uploader) error {
	m.uploader = uploader
	return m.init(warehouse)
}

func (m *MaxCompute) Connect(_ context.Context, warehouse model.Warehouse) (client.Client, error) {
	if err := m.init(warehouse); err != nil {
		return client.Client{}, err
	}
	return client.Client{Type: client.ODPSClient, ODPS: m.odps}, nil
}

func (m *MaxCompute) TestConnection(ctx context.Context, warehouse model.Warehouse) error {
	if m.odps == nil || m.warehouse.Destination.ID != warehouse.Destination.ID || m.warehouse.Namespace != warehouse.Namespace {
		if err := m.init(warehouse); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Project.Load checks both access and project existence without creating a
	// table, so a connection validation needs only project metadata permission.
	project := m.odps.DefaultProject()
	if err := project.Load(); err != nil {
		return fmt.Errorf("loading MaxCompute project: %w", client.ODPSSafeError(err))
	}
	if project.Owner() == "" {
		return errors.New("MaxCompute project owner unavailable")
	}
	return m.checkNamespaceOwner(ctx)
}

// checkNamespaceOwner rejects validation writes to an existing schema owned by
// someone else. The project itself is not a schema; only check schema ownership
// when an explicit namespace is configured. New schemas have no owner yet.
func (m *MaxCompute) checkNamespaceOwner(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.odps == nil {
		return errors.New("MaxCompute client is not initialized")
	}
	if m.namespace() == "" {
		return nil
	}
	schema := m.odps.Schema(m.namespace())
	if err := schema.Load(); err != nil {
		if httpErr, ok := errors.AsType[restclient.HttpError](err); ok && httpErr.StatusCode == http.StatusNotFound {
			return ctx.Err() // no existing schema to write to
		}
		return fmt.Errorf("loading MaxCompute schema: %w", client.ODPSSafeError(err))
	}
	owner := strings.TrimSpace(schema.Owner())
	if owner == "" {
		return errors.New("MaxCompute schema owner unavailable")
	}
	// ODPS security WHOAMI reports the effective principal, including assumed
	// roles. Do not infer identity from an access key or the project owner.
	principal, err := m.currentPrincipal(ctx)
	if err != nil {
		return err
	}
	if owner != principal {
		return errors.New("MaxCompute schema is not owned by the current principal")
	}
	return ctx.Err()
}

func (m *MaxCompute) currentPrincipal(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if m.principal != nil {
		return m.principal(ctx)
	}
	// The SDK's authorization query hides whether the response is async, and
	// WaitForSuccess polls async responses forever. Use its signed REST client
	// and accept only a synchronous WHOAMI response; fail closed otherwise.
	rest := m.odps.RestClient()
	resource := "/projects/" + m.odps.DefaultProjectName() + "/authorization"
	body := strings.NewReader("<Authorization><Query>whoami;</Query><ResponseInJsonFormat>false</ResponseInJsonFormat></Authorization>")
	req, err := rest.NewRequest(http.MethodPost, resource, body)
	if err != nil {
		return "", errors.New("creating MaxCompute principal request failed")
	}
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", "application/xml")
	var principal string
	err = rest.DoWithParseRes(req, func(response *http.Response) error {
		if response.StatusCode != http.StatusOK {
			return errors.New("MaxCompute principal lookup did not complete synchronously")
		}
		var result struct {
			Result string `xml:"Result"`
		}
		if err := xml.NewDecoder(response.Body).Decode(&result); err != nil {
			return errors.New("invalid MaxCompute principal response")
		}
		principal = strings.TrimSpace(result.Result)
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("getting MaxCompute principal: %w", client.ODPSSafeError(err))
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if principal == "" {
		return "", errors.New("MaxCompute principal unavailable")
	}
	return principal, nil
}

func (m *MaxCompute) namespace() string {
	project := m.odps.DefaultProjectName()
	if m.warehouse.Namespace == "" || m.warehouse.Namespace == project {
		return ""
	}
	return m.warehouse.Namespace
}

func quote(name string) (string, error) {
	if !identifierPattern.MatchString(name) {
		return "", fmt.Errorf("invalid MaxCompute identifier %q", name)
	}
	return "`" + name + "`", nil
}

func (m *MaxCompute) qualified(table string) (string, error) {
	if m.odps == nil {
		return "", errors.New("MaxCompute client is not initialized")
	}
	name, err := quote(table)
	if err != nil {
		return "", err
	}
	project, err := quote(m.odps.DefaultProjectName())
	if err != nil {
		return "", err
	}
	if schema := m.namespace(); schema != "" {
		part, err := quote(schema)
		if err != nil {
			return "", err
		}
		return project + "." + part + "." + name, nil
	}
	return project + "." + name, nil
}

func (m *MaxCompute) run(ctx context.Context, statement string) error {
	_, err := m.runSQL(ctx, statement, false)
	return err
}

func loadSummaryRows(text string) (int64, error) {
	var parsed any
	if err := jsonrs.Unmarshal([]byte(text), &parsed); err != nil {
		return 0, errors.New("invalid MaxCompute load task summary")
	}
	var total int64
	var found bool
	var addCounts func(any) bool
	addCounts = func(value any) bool {
		switch value := value.(type) {
		case float64:
			if value < 0 || value >= 1<<53 || value != float64(int64(value)) {
				return false
			}
			count := int64(value)
			if count > int64(^uint64(0)>>1)-total {
				return false
			}
			total += count
			found = true
			return true
		case []any:
			for _, child := range value {
				if !addCounts(child) {
					return false
				}
			}
			return true
		case map[string]any:
			for _, child := range value {
				if !addCounts(child) {
					return false
				}
			}
			return true
		default:
			return false
		}
	}
	var walkOutputs func(any) bool
	walkOutputs = func(value any) bool {
		switch value := value.(type) {
		case map[string]any:
			if counts, ok := value["OutputRecordCounts"]; ok {
				return addCounts(counts)
			}
			for _, child := range value {
				if !walkOutputs(child) {
					return false
				}
			}
		case []any:
			for _, child := range value {
				if !walkOutputs(child) {
					return false
				}
			}
		}
		return true
	}
	var findOutputs func(any) bool
	findOutputs = func(value any) bool {
		switch value := value.(type) {
		case map[string]any:
			if outputs, ok := value["Outputs"]; ok {
				return walkOutputs(outputs)
			}
			for _, child := range value {
				if !findOutputs(child) {
					return false
				}
			}
		case []any:
			for _, child := range value {
				if !findOutputs(child) {
					return false
				}
			}
		}
		return true
	}
	if !findOutputs(parsed) || !found {
		return 0, errors.New("MaxCompute load task summary has no valid output record count")
	}
	return total, nil
}

func (m *MaxCompute) runSQL(ctx context.Context, statement string, summary bool) (int64, error) {
	if m.exec != nil {
		return 0, m.exec(ctx, statement)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if m.odps == nil {
		return 0, errors.New("MaxCompute client is not initialized")
	}
	timeout := m.queryTimeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ins, err := client.RunODPSSQL(ctx, m.odps, statement, timeout, m.pollInterval)
	if err != nil {
		return 0, fmt.Errorf("running MaxCompute SQL: %w", err)
	}
	if !summary {
		return 0, nil
	}
	rows, summaryErr := m.loadTaskSummary(ctx, ins)
	if summaryErr != nil {
		if m.logger != nil {
			m.logger.Warnn("MaxCompute LOAD succeeded but row count is unknown")
		}
		return -1, nil
	}
	return rows, nil
}

func (m *MaxCompute) loadTaskSummary(ctx context.Context, ins *odps.Instance) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	// Fetch the completed task names for the manager's load-summary parser.
	tasks, err := ins.GetTasks()
	if err != nil {
		return 0, client.ODPSSafeError(err)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if len(tasks) == 0 {
		return 0, errors.New("MaxCompute load has no tasks")
	}
	var total int64
	for _, task := range tasks {
		if task.Status != odps.TaskSuccess {
			return 0, errors.New("MaxCompute SQL task failed")
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		result, err := ins.GetTaskSummary(task.Name)
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if err != nil {
			return 0, client.ODPSSafeError(err)
		}
		if result == nil {
			return 0, errors.New("MaxCompute load task summary unavailable")
		}
		count, err := loadSummaryRows(result.JsonSummary)
		if err != nil {
			return 0, err
		}
		if count > (1<<63-1)-total {
			return 0, errors.New("MaxCompute load row count overflow")
		}
		total += count
	}
	return total, nil
}

func (m *MaxCompute) CreateSchema(ctx context.Context) error {
	if m.odps == nil {
		return errors.New("MaxCompute client is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.namespace() == "" {
		return m.TestConnection(ctx, m.warehouse)
	}
	if err := m.checkNamespaceOwner(ctx); err != nil {
		return err
	}
	part, err := quote(m.namespace())
	if err != nil {
		return err
	}
	project, err := quote(m.odps.DefaultProjectName())
	if err != nil {
		return err
	}
	return m.run(ctx, "CREATE SCHEMA IF NOT EXISTS "+project+"."+part)
}

func (m *MaxCompute) CreateTable(ctx context.Context, table string, columns model.TableSchema) error {
	name, err := m.qualified(table)
	if err != nil {
		return err
	}
	if len(columns) == 0 {
		return errors.New("MaxCompute table has no columns")
	}
	plan, err := m.configuredPartition(columns)
	if err != nil {
		return err
	}
	if plan.Source == "uuid_ts" && columns[plan.Source] == "" {
		columns = maps.Clone(columns)
		columns[plan.Source] = "datetime"
	}
	if err := validatePartitionPlan(plan, columns); err != nil {
		return err
	}
	if len(columns) > 1200 {
		return errors.New("MaxCompute table exceeds 1200 columns")
	}
	definitions := make([]string, 0, len(columns))
	for _, key := range warehouseutils.SortColumnKeysFromColumnMap(columns) {
		field, err := quote(key)
		if err != nil {
			return err
		}
		kind, err := columnType(columns[key])
		if err != nil {
			return fmt.Errorf("column %q: %w", key, err)
		}
		definitions = append(definitions, field+" "+kind)
	}
	statement := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", name, strings.Join(definitions, ", "))
	statement += " AUTO PARTITIONED BY (TRUNC_TIME(`" + plan.Source + "`, '" + plan.Granularity + "') AS `pt`)"
	statement += " TBLPROPERTIES ('table.format.version'='2')"
	if err := m.run(ctx, statement); err != nil {
		return err
	}
	return m.createTableView(ctx, table, columns)
}

func (m *MaxCompute) DropTable(ctx context.Context, table string) error {
	name, err := m.qualified(table)
	if err != nil {
		return err
	}
	view, err := m.qualified(viewName(table))
	if err != nil {
		return err
	}
	if err := m.run(ctx, "DROP VIEW IF EXISTS "+view); err != nil {
		return err
	}
	return m.run(ctx, "DROP TABLE IF EXISTS "+name)
}

func (m *MaxCompute) AddColumns(ctx context.Context, table string, columns []warehouseutils.ColumnInfo) error {
	name, err := m.qualified(table)
	if err != nil {
		return err
	}
	for _, col := range columns {
		field, err := quote(col.Name)
		if err != nil {
			return err
		}
		kind, err := columnType(col.Type)
		if err != nil {
			return fmt.Errorf("column %q: %w", col.Name, err)
		}
		if err := m.run(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMNS (%s %s)", name, field, kind)); err != nil {
			return fmt.Errorf("adding column %q: %w", col.Name, err)
		}
	}
	return nil
}

func (*MaxCompute) AlterColumn(context.Context, string, string, string) (model.AlterTableResponse, error) {
	return model.AlterTableResponse{}, fmt.Errorf("MaxCompute ALTER COLUMN: %w", errNotImplemented)
}

// DeleteBy is intentionally a no-op: MaxCompute loads are append-only.
func (*MaxCompute) DeleteBy(context.Context, []string, warehouseutils.DeleteByParams) error {
	return nil
}

func (*MaxCompute) DownloadIdentityRules(context.Context, *misc.GZipWriter) error {
	return errNotImplemented
}
func (*MaxCompute) LoadIdentityMergeRulesTable(context.Context) error { return nil }
func (*MaxCompute) LoadIdentityMappingsTable(context.Context) error   { return nil }
func (*MaxCompute) Cleanup(context.Context)                           {}
func (m *MaxCompute) SetConnectionTimeout(timeout time.Duration) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if timeout > 2*time.Minute {
		timeout = 2 * time.Minute
	}
	m.connectTimeout = timeout
	m.applyConnectionTimeout(m.odps)
}

func (m *MaxCompute) applyConnectionTimeout(instance *odps.Odps) {
	if instance != nil {
		if m.connectTimeout <= 0 {
			m.connectTimeout = 30 * time.Second
		}
		instance.SetHttpTimeout(m.connectTimeout)
		instance.SetTcpConnectTimeout(m.connectTimeout)
	}
}

func (*MaxCompute) ErrorMappings() []model.JobError { return errorsMappings }
func (m *MaxCompute) TestFetchSchema(ctx context.Context) error {
	_, err := m.FetchSchema(ctx)
	return err
}

func (m *MaxCompute) IsEmpty(ctx context.Context, warehouse model.Warehouse) (bool, error) {
	if m.odps == nil || m.warehouse.Destination.ID != warehouse.Destination.ID || m.warehouse.Namespace != warehouse.Namespace {
		if err := m.init(warehouse); err != nil {
			return false, err
		}
	}
	schema, err := m.FetchSchema(ctx)
	return len(schema) == 0, err
}

func (m *MaxCompute) LoadUserTables(ctx context.Context) map[string]error {
	result := map[string]error{warehouseutils.IdentifiesTable: nil}
	_, result[warehouseutils.IdentifiesTable] = m.LoadTable(ctx, warehouseutils.IdentifiesTable)
	if result[warehouseutils.IdentifiesTable] != nil || m.uploader == nil {
		return result
	}
	if skip, _ := m.warehouse.Destination.Config["skipUsersTable"].(bool); skip {
		return result
	}
	users := m.uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)
	if len(users) == 0 {
		return result
	}
	result[warehouseutils.UsersTable] = m.loadUsersWithCarryForward(ctx)
	return result
}

func (m *MaxCompute) LoadTable(ctx context.Context, table string) (*types.LoadTableStats, error) {
	if m.uploader == nil {
		return nil, errors.New("MaxCompute uploader is not initialized")
	}
	if m.uploader.GetLoadFileType() != warehouseutils.LoadFileTypeJson {
		return nil, errors.New("MaxCompute requires JSON load files")
	}
	files, err := m.uploader.GetLoadFilesMetadata(ctx, warehouseutils.GetLoadFilesOptions{Table: table})
	if err != nil {
		return nil, errors.New("getting MaxCompute load files failed")
	}
	return m.loadTable(ctx, table, files)
}

func (m *MaxCompute) TestLoadTable(ctx context.Context, location, table string, payload map[string]any, format string) error {
	if format != warehouseutils.LoadFileTypeJson {
		return fmt.Errorf("MaxCompute validation expects JSON load files, got %q", format)
	}
	size, ok := warehouseutils.MaxComputeValidationFileSize(ctx)
	if !ok || size <= 0 {
		return errors.New("MaxCompute validation load file size is missing")
	}
	metadata := fmt.Appendf(nil, `{"content_length":%d}`, size)
	if _, err := m.loadTable(ctx, table, []warehouseutils.LoadFile{{Location: location, Metadata: metadata}}); err != nil {
		return err
	}
	stamp, ok := payload["uuid_ts"].(string)
	if !ok {
		return errors.New("MaxCompute validation timestamp is missing")
	}
	parsed, err := time.ParseInLocation(encoding.MaxComputeUUIDTsFormat, stamp, time.UTC)
	if err != nil {
		return errors.New("MaxCompute validation timestamp is invalid")
	}
	name, err := m.qualified(table)
	if err != nil {
		return err
	}
	day := parsed.Format("2006-01-02")
	end := parsed.AddDate(0, 0, 1).Format("2006-01-02")
	id, ok := payload["id"].(int)
	if !ok {
		return errors.New("MaxCompute validation row ID is invalid")
	}
	statement := fmt.Sprintf("SELECT `id`, `val` FROM %s WHERE `pt` >= '%s' AND `pt` < '%s' AND `id` = %d LIMIT 1", name, day, end, id)
	if err := ctx.Err(); err != nil {
		return err
	}
	var result warehouseutils.QueryResult
	if m.query != nil {
		result, err = m.query(ctx, statement)
	} else {
		result, err = client.QueryODPS(ctx, m.odps, statement, m.queryTimeout, m.pollInterval)
	}
	if err != nil {
		return err
	}
	if len(result.Values) != 1 || len(result.Values[0]) != 2 ||
		result.Values[0][0] != fmt.Sprint(payload["id"]) || result.Values[0][1] != fmt.Sprint(payload["val"]) {
		return errors.New("MaxCompute validation load row was not found")
	}
	return nil
}
