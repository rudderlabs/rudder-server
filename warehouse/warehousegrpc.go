package warehouse

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/golang/protobuf/jsonpb"
	proto "github.com/rudderlabs/rudder-server/proto/warehouse"
)

type warehousegrpc struct {
	proto.UnimplementedWarehouseServer
}

func (w *warehousegrpc) GetWHUploads(context context.Context, request *proto.GetWHUploadsRequest) (*proto.GetWHUploadsResponse, error) {
	uploadsReq := UploadsReqT{
		SourceID:           request.SourceId,
		DestinationID:      request.DestinationId,
		DestinationType:    request.DestinationType,
		Status:             request.Status,
		IncludeTablesInRes: request.IncludeTablesInRes,
		Limit:              request.Limit,
		Offset:             request.Offset,
		API:                UploadAPI,
	}
	protoRes := &proto.GetWHUploadsResponse{}
	res, err := uploadsReq.GetWhUploads()
	if err != nil {
		return protoRes, err
	}
	bytes, err := json.Marshal(res)
	if err != nil {
		return protoRes, err
	}
	err = jsonpb.UnmarshalString(string(bytes), protoRes)
	if err != nil {
		return protoRes, err
	}
	return protoRes, nil
}

func (w *warehousegrpc) GetWHUpload(context context.Context, request *proto.GetWHUploadRequest) (*proto.GetWHUploadResponse, error) {
	uploadReq := UploadReqT{
		UploadId: request.UploadId,
		API:      UploadAPI,
	}
	protoRes := &proto.GetWHUploadResponse{}
	res, err := uploadReq.GetWHUpload()
	if err != nil {
		return protoRes, err
	}
	bytes, err := json.Marshal(res)
	if err != nil {
		return protoRes, err
	}
	err = jsonpb.UnmarshalString(string(bytes), protoRes)
	if err != nil {
		return protoRes, err
	}
	fmt.Println(protoRes)
	return protoRes, nil
}

func (w *warehousegrpc) GetWHTables(ctx context.Context, request *proto.GetWHTablesRequest) (*proto.GetWHTablesResponse, error) {
	tableReq := TableUploadReqT{
		UploadID: request.GetUploadId(),
		API:      UploadAPI,
	}
	protoRes := &proto.GetWHTablesResponse{}
	res, err := tableReq.GetWhTableUploads()
	if err != nil {
		return protoRes, err
	}
	bytes, err := json.Marshal(res)
	if err != nil {
		return protoRes, err
	}
	err = jsonpb.UnmarshalString(string(bytes), protoRes)
	if err != nil {
		return protoRes, err
	}
	return protoRes, nil
}
