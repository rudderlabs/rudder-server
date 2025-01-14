package destination_transformer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/processor/internal/http_client"
	"github.com/rudderlabs/rudder-server/processor/types"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type DestTransformer struct {
	config struct {
		destTransformationURL   string
		maxRetry                config.ValueLoader[int]
		failOnError             config.ValueLoader[bool]
		maxRetryBackoffInterval config.ValueLoader[time.Duration]
	}
	conf   *config.Config
	log    logger.Logger
	stat   stats.Stats
	client http_client.HTTPDoer
}

func (d *DestTransformer) SendRequest(ctx context.Context, clientEvents []types.TransformerEvent, batchSize int) types.Response {
	_ = d.destTransformURL(clientEvents[0].Destination.DestinationDefinition.Name)
	return types.Response{}
}

func NewDestTransformer(conf *config.Config, log logger.Logger, stat stats.Stats) *DestTransformer {
	handle := &DestTransformer{}
	handle.conf = conf
	handle.log = log
	handle.stat = stat
	handle.client = http_client.NewHTTPClient(conf)
	handle.config.destTransformationURL = handle.conf.GetString("DEST_TRANSFORM_URL", "http://localhost:9090")
	handle.config.maxRetry = conf.GetReloadableIntVar(30, 1, "Processor.maxRetry")
	handle.config.failOnError = conf.GetReloadableBoolVar(false, "Processor.Transformer.failOnError")
	handle.config.maxRetryBackoffInterval = conf.GetReloadableDurationVar(30, time.Second, "Processor.maxRetryBackoffInterval")
	return handle
}

func (d *DestTransformer) destTransformURL(destType string) string {
	destinationEndPoint := fmt.Sprintf("%s/v0/destinations/%s", d.config.destTransformationURL, strings.ToLower(destType))

	if _, ok := warehouseutils.WarehouseDestinationMap[destType]; ok {
		whSchemaVersionQueryParam := fmt.Sprintf("whSchemaVersion=%s&whIDResolve=%v", d.conf.GetString("Warehouse.schemaVersion", "v1"), warehouseutils.IDResolutionEnabled())
		switch destType {
		case warehouseutils.RS:
			return destinationEndPoint + "?" + whSchemaVersionQueryParam
		case warehouseutils.CLICKHOUSE:
			enableArraySupport := fmt.Sprintf("chEnableArraySupport=%s", fmt.Sprintf("%v", d.conf.GetBool("Warehouse.clickhouse.enableArraySupport", false)))
			return destinationEndPoint + "?" + whSchemaVersionQueryParam + "&" + enableArraySupport
		default:
			return destinationEndPoint + "?" + whSchemaVersionQueryParam
		}
	}
	if destType == warehouseutils.SnowpipeStreaming {
		return fmt.Sprintf("%s?whSchemaVersion=%s&whIDResolve=%t", destinationEndPoint, d.conf.GetString("Warehouse.schemaVersion", "v1"), warehouseutils.IDResolutionEnabled())
	}
	return destinationEndPoint
}
