package warehouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/controlplane"
	proto "github.com/rudderlabs/rudder-server/proto/warehouse"
	"github.com/rudderlabs/rudder-server/utils/filemanagerutil"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
	cpclient "github.com/rudderlabs/rudder-server/warehouse/client/controlplane"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
	"google.golang.org/grpc"
	"os"
)

type GRPC struct {
	logger             logger.Logger
	statsFactory       stats.Stats
	db                 *sqlmw.DB
	isMultiWorkspace   bool
	cpClient           cpclient.InternalControlPlane
	connectionManager  *controlplane.ConnectionManager
	bcManager          *backendConfigManager
	tableUploadsRepo   *repo.TableUploads
	stagingRepo        *repo.StagingFiles
	uploadRepo         *repo.Uploads
	fileManagerFactory filemanager.Factory

	config struct {
		region         string
		cpRouterUseTLS bool
		instanceID     string
		controlPlane   struct {
			url      string
			userName string
			password string
		}
		enableTunnelling bool
	}
}

func NewGRPC(
	conf *config.Config,
	logger logger.Logger,
	statsFactory stats.Stats,
	db *sqlmw.DB,
	bcManager *backendConfigManager,
) (*GRPC, error) {
	g := GRPC{
		logger:             logger.Child("grpc"),
		statsFactory:       statsFactory,
		db:                 db,
		bcManager:          bcManager,
		stagingRepo:        repo.NewStagingFiles(db),
		uploadRepo:         repo.NewUploads(db),
		tableUploadsRepo:   repo.NewTableUploads(db),
		fileManagerFactory: filemanager.New,
	}

	g.config.region = conf.GetString("REGION", "")
	g.config.cpRouterUseTLS = conf.GetBool("CP_ROUTER_USE_TLS", true)
	g.config.instanceID = conf.GetString("INSTANCE_ID", "1")
	g.config.controlPlane.url = conf.GetString("CONFIG_BACKEND_URL", "api.rudderlabs.com")
	g.config.controlPlane.userName = conf.GetString("CP_INTERNAL_API_USERNAME", "")
	g.config.controlPlane.password = conf.GetString("CP_INTERNAL_API_PASSWORD", "")
	g.config.enableTunnelling = conf.GetBool("ENABLE_TUNNELLING", true)

	g.cpClient = cpclient.NewInternalClientWithCache(
		g.config.controlPlane.url,
		cpclient.BasicAuth{
			Username: g.config.controlPlane.userName,
			Password: g.config.controlPlane.password,
		},
	)

	connectionToken, tokenType, isMultiWorkspace, err := deployment.GetConnectionToken()
	if err != nil {
		return nil, fmt.Errorf("connection token: %w", err)
	}

	labels := map[string]string{}
	if g.config.region != "" {
		labels["region"] = g.config.region
	}

	g.isMultiWorkspace = isMultiWorkspace
	g.connectionManager = &controlplane.ConnectionManager{
		AuthInfo: controlplane.AuthInfo{
			Service:         "warehouse",
			ConnectionToken: connectionToken,
			InstanceID:      g.config.instanceID,
			TokenType:       tokenType,
			Labels:          labels,
		},
		RetryInterval: 0,
		UseTLS:        g.config.cpRouterUseTLS,
		Logger:        g.logger,
		RegisterService: func(srv *grpc.Server) {
			proto.RegisterWarehouseServer(srv, &GRPCServer{
				GRPC: g,
			})
		},
	}

	return &g, nil
}

func (g *GRPC) Apply(url string, active bool) {
	g.connectionManager.Apply(url, active)
}

type validateObjectStorageRequest struct {
	Type   string                 `json:"type"`
	Config map[string]interface{} `json:"config"`
}

func (g *GRPC) validateObjectStorage(ctx context.Context, request validateObjectStorageRequest) error {
	switch request.Type {
	case warehouseutils.AzureBlob:
		if !checkMapForValidKey(request.Config, "containerName") {
			return errors.New("containerName invalid or not present")
		}
	case warehouseutils.GCS, warehouseutils.MINIO, warehouseutils.S3, warehouseutils.DigitalOceanSpaces:
		if !checkMapForValidKey(request.Config, "bucketName") {
			return errors.New("bucketName invalid or not present")
		}
	default:
		return fmt.Errorf("type: %v not supported", request.Type)
	}

	settings := &filemanager.Settings{
		Provider: request.Type,
		Config:   request.Config,
	}

	if err := overrideWithEnv(ctx, settings); err != nil {
		return fmt.Errorf("overriding config with env: %w", err)
	}

	fileManager, err := g.fileManagerFactory(settings)
	if err != nil {
		return fmt.Errorf("unable to create file manager: \n%s", err.Error())
	}

	filePath, err := validations.CreateTempLoadFile(&backendconfig.DestinationT{
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name: request.Type,
		},
	})
	if err != nil {
		return fmt.Errorf("unable to create temp load file: \n%w", err)
	}
	defer func() {
		_ = os.Remove(filePath)
	}()

	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("unable to open path to temporary file: \n%w", err)
	}

	uploadOutput, err := fileManager.Upload(ctx, f)
	if err != nil {
		return fmt.Errorf("unable to upload file: \n%w", err)
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("unable to close file: \n%w", err)
	}

	tmpDirectory, err := misc.CreateTMPDIR()
	if err != nil {
		return fmt.Errorf("unable to create temp directory: \n%w", err)
	}

	f, err = os.CreateTemp(tmpDirectory, DownloadFileNamePattern)
	if err != nil {
		return fmt.Errorf("unable to create temp file: \n%w", err)
	}
	defer func() {
		os.Remove(f.Name())
	}()

	err = fileManager.Download(
		ctx,
		f,
		fileManager.GetDownloadKeyFromFileLocation(uploadOutput.Location),
	)
	if err != nil {
		return fmt.Errorf("unable to download file: \n%w", err)
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("unable to close file: \n%w", err)
	}

	return nil
}

// checkMapForValidKey checks the presence of key in map
// and if yes verifies that the key is string and non-empty.
func checkMapForValidKey(configMap map[string]interface{}, key string) bool {
	if value, ok := configMap[key]; !ok {
		return false
	} else if valStr, ok := value.(string); ok {
		return valStr != ""
	}
	return false
}

// overrideWithEnv overrides the config keys in the fileManager settings
// with fallback values pulled from env. Only supported for S3 for now.
func overrideWithEnv(ctx context.Context, settings *filemanager.Settings) error {
	envConfig := filemanager.GetProviderConfigFromEnv(
		filemanagerutil.ProviderConfigOpts(
			ctx,
			settings.Provider,
			config.Default,
		),
	)

	if settings.Provider == warehouseutils.S3 {
		ifNotExistThenSet("prefix", envConfig["prefix"], settings.Config)
		ifNotExistThenSet("accessKeyID", envConfig["accessKeyID"], settings.Config)
		ifNotExistThenSet("accessKey", envConfig["accessKey"], settings.Config)
		ifNotExistThenSet("enableSSE", envConfig["enableSSE"], settings.Config)
		ifNotExistThenSet("iamRoleARN", envConfig["iamRoleArn"], settings.Config)
		ifNotExistThenSet("externalID", envConfig["externalID"], settings.Config)
		ifNotExistThenSet("regionHint", envConfig["regionHint"], settings.Config)
	}
	return ctx.Err()
}

func ifNotExistThenSet(keyToReplace string, replaceWith interface{}, configMap map[string]interface{}) {
	if _, ok := configMap[keyToReplace]; !ok {
		// In case we don't have the key, simply replace it with replaceWith
		configMap[keyToReplace] = replaceWith
	}
}

type validationRequest struct {
	Path string
	Step string
	Body string
}

type validationResponse struct {
	Error string
	Data  string
}

func (g *GRPC) validate(ctx context.Context, req *validationRequest) (*validationResponse, error) {
	var (
		err      error
		resModel struct {
			Destination backendconfig.DestinationT `json:"destination"`
		}
	)

	err = json.Unmarshal(json.RawMessage(req.Body), &resModel)
	if err != nil {
		return nil, fmt.Errorf("unmarshal request body: %w", err)
	}

	destination := resModel.Destination
	if len(destination.Config) == 0 {
		return nil, errors.New("destination config is empty")
	}

	// adding ssh tunnelling info, given we have
	// useSSH enabled from upstream
	if g.config.enableTunnelling {
		err = g.manageTunnellingSecrets(ctx, destination.Config)
		if err != nil {
			return nil, fmt.Errorf("fetching destination ssh keys: %w", err)
		}
	}

	res, err := validations.Validate(ctx, &model.ValidationRequest{
		Path:        req.Path,
		Step:        req.Step,
		Destination: &destination,
	})
	if err != nil {
		return nil, fmt.Errorf("validating destination: %w", err)
	}

	return &validationResponse{
		Error: res.Error,
		Data:  res.Data,
	}, nil
}

func (g *GRPC) manageTunnellingSecrets(ctx context.Context, config map[string]interface{}) error {
	if !warehouseutils.ReadAsBool("useSSH", config) {
		return nil
	}

	sshKeyId, ok := config["sshKeyId"]
	if !ok {
		return fmt.Errorf("missing sshKeyId in validation payload")
	}

	keys, err := g.cpClient.GetSSHKeys(ctx, sshKeyId.(string))
	if err != nil {
		return fmt.Errorf("fetching destination ssh keys: %w", err)
	}

	config["sshPrivateKey"] = keys.PrivateKey

	return nil
}
