package client

import (
	"context"
	"errors"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
)

var ErrNotImplemented = errors.New("databricks: not implemented")

type TestClient struct {
	FnOpenSession           func(ctx context.Context, req *cli_service.TOpenSessionReq) (_r *cli_service.TOpenSessionResp, _err error)
	FnCloseSession          func(ctx context.Context, req *cli_service.TCloseSessionReq) (_r *cli_service.TCloseSessionResp, _err error)
	FnGetInfo               func(ctx context.Context, req *cli_service.TGetInfoReq) (_r *cli_service.TGetInfoResp, _err error)
	FnExecuteStatement      func(ctx context.Context, req *cli_service.TExecuteStatementReq) (_r *cli_service.TExecuteStatementResp, _err error)
	FnGetTypeInfo           func(ctx context.Context, req *cli_service.TGetTypeInfoReq) (_r *cli_service.TGetTypeInfoResp, _err error)
	FnGetCatalogs           func(ctx context.Context, req *cli_service.TGetCatalogsReq) (_r *cli_service.TGetCatalogsResp, _err error)
	FnGetSchemas            func(ctx context.Context, req *cli_service.TGetSchemasReq) (_r *cli_service.TGetSchemasResp, _err error)
	FnGetTables             func(ctx context.Context, req *cli_service.TGetTablesReq) (_r *cli_service.TGetTablesResp, _err error)
	FnGetTableTypes         func(ctx context.Context, req *cli_service.TGetTableTypesReq) (_r *cli_service.TGetTableTypesResp, _err error)
	FnGetColumns            func(ctx context.Context, req *cli_service.TGetColumnsReq) (_r *cli_service.TGetColumnsResp, _err error)
	FnGetFunctions          func(ctx context.Context, req *cli_service.TGetFunctionsReq) (_r *cli_service.TGetFunctionsResp, _err error)
	FnGetPrimaryKeys        func(ctx context.Context, req *cli_service.TGetPrimaryKeysReq) (_r *cli_service.TGetPrimaryKeysResp, _err error)
	FnGetCrossReference     func(ctx context.Context, req *cli_service.TGetCrossReferenceReq) (_r *cli_service.TGetCrossReferenceResp, _err error)
	FnGetOperationStatus    func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (_r *cli_service.TGetOperationStatusResp, _err error)
	FnCancelOperation       func(ctx context.Context, req *cli_service.TCancelOperationReq) (_r *cli_service.TCancelOperationResp, _err error)
	FnCloseOperation        func(ctx context.Context, req *cli_service.TCloseOperationReq) (_r *cli_service.TCloseOperationResp, _err error)
	FnGetResultSetMetadata  func(ctx context.Context, req *cli_service.TGetResultSetMetadataReq) (_r *cli_service.TGetResultSetMetadataResp, _err error)
	FnFetchResults          func(ctx context.Context, req *cli_service.TFetchResultsReq) (_r *cli_service.TFetchResultsResp, _err error)
	FnGetDelegationToken    func(ctx context.Context, req *cli_service.TGetDelegationTokenReq) (_r *cli_service.TGetDelegationTokenResp, _err error)
	FnCancelDelegationToken func(ctx context.Context, req *cli_service.TCancelDelegationTokenReq) (_r *cli_service.TCancelDelegationTokenResp, _err error)
	FnRenewDelegationToken  func(ctx context.Context, req *cli_service.TRenewDelegationTokenReq) (_r *cli_service.TRenewDelegationTokenResp, _err error)
}

var _ cli_service.TCLIService = (*TestClient)(nil)

func (c *TestClient) OpenSession(ctx context.Context, req *cli_service.TOpenSessionReq) (_r *cli_service.TOpenSessionResp, _err error) {
	if c.FnOpenSession != nil {
		return c.FnOpenSession(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *TestClient) CloseSession(ctx context.Context, req *cli_service.TCloseSessionReq) (_r *cli_service.TCloseSessionResp, _err error) {
	if c.FnCloseSession != nil {
		return c.FnCloseSession(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *TestClient) GetInfo(ctx context.Context, req *cli_service.TGetInfoReq) (_r *cli_service.TGetInfoResp, _err error) {
	if c.FnGetInfo != nil {
		return c.FnGetInfo(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *TestClient) ExecuteStatement(ctx context.Context, req *cli_service.TExecuteStatementReq) (_r *cli_service.TExecuteStatementResp, _err error) {
	if c.FnExecuteStatement != nil {
		return c.FnExecuteStatement(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *TestClient) GetTypeInfo(ctx context.Context, req *cli_service.TGetTypeInfoReq) (_r *cli_service.TGetTypeInfoResp, _err error) {
	if c.FnGetTypeInfo != nil {
		return c.FnGetTypeInfo(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *TestClient) GetCatalogs(ctx context.Context, req *cli_service.TGetCatalogsReq) (_r *cli_service.TGetCatalogsResp, _err error) {
	if c.FnGetCatalogs != nil {
		return c.FnGetCatalogs(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *TestClient) GetSchemas(ctx context.Context, req *cli_service.TGetSchemasReq) (_r *cli_service.TGetSchemasResp, _err error) {
	if c.FnGetSchemas != nil {
		return c.FnGetSchemas(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *TestClient) GetTables(ctx context.Context, req *cli_service.TGetTablesReq) (_r *cli_service.TGetTablesResp, _err error) {
	if c.FnGetTables != nil {
		return c.FnGetTables(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *TestClient) GetTableTypes(ctx context.Context, req *cli_service.TGetTableTypesReq) (_r *cli_service.TGetTableTypesResp, _err error) {
	if c.FnGetTableTypes != nil {
		return c.FnGetTableTypes(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *TestClient) GetColumns(ctx context.Context, req *cli_service.TGetColumnsReq) (_r *cli_service.TGetColumnsResp, _err error) {
	if c.FnGetColumns != nil {
		return c.FnGetColumns(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *TestClient) GetFunctions(ctx context.Context, req *cli_service.TGetFunctionsReq) (_r *cli_service.TGetFunctionsResp, _err error) {
	if c.FnGetFunctions != nil {
		return c.FnGetFunctions(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *TestClient) GetPrimaryKeys(ctx context.Context, req *cli_service.TGetPrimaryKeysReq) (_r *cli_service.TGetPrimaryKeysResp, _err error) {
	if c.FnGetPrimaryKeys != nil {
		return c.FnGetPrimaryKeys(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *TestClient) GetCrossReference(ctx context.Context, req *cli_service.TGetCrossReferenceReq) (_r *cli_service.TGetCrossReferenceResp, _err error) {
	if c.FnGetCrossReference != nil {
		return c.FnGetCrossReference(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *TestClient) GetOperationStatus(ctx context.Context, req *cli_service.TGetOperationStatusReq) (_r *cli_service.TGetOperationStatusResp, _err error) {
	if c.FnGetOperationStatus != nil {
		return c.FnGetOperationStatus(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *TestClient) CancelOperation(ctx context.Context, req *cli_service.TCancelOperationReq) (_r *cli_service.TCancelOperationResp, _err error) {
	if c.FnCancelOperation != nil {
		return c.FnCancelOperation(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *TestClient) CloseOperation(ctx context.Context, req *cli_service.TCloseOperationReq) (_r *cli_service.TCloseOperationResp, _err error) {
	if c.FnCloseOperation != nil {
		return c.FnCloseOperation(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *TestClient) GetResultSetMetadata(ctx context.Context, req *cli_service.TGetResultSetMetadataReq) (_r *cli_service.TGetResultSetMetadataResp, _err error) {
	if c.FnGetResultSetMetadata != nil {
		return c.FnGetResultSetMetadata(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *TestClient) FetchResults(ctx context.Context, req *cli_service.TFetchResultsReq) (_r *cli_service.TFetchResultsResp, _err error) {
	if c.FnFetchResults != nil {
		return c.FnFetchResults(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *TestClient) GetDelegationToken(ctx context.Context, req *cli_service.TGetDelegationTokenReq) (_r *cli_service.TGetDelegationTokenResp, _err error) {
	if c.FnGetDelegationToken != nil {
		return c.FnGetDelegationToken(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *TestClient) CancelDelegationToken(ctx context.Context, req *cli_service.TCancelDelegationTokenReq) (_r *cli_service.TCancelDelegationTokenResp, _err error) {
	if c.FnCancelDelegationToken != nil {
		return c.FnCancelDelegationToken(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *TestClient) RenewDelegationToken(ctx context.Context, req *cli_service.TRenewDelegationTokenReq) (_r *cli_service.TRenewDelegationTokenResp, _err error) {
	if c.FnRenewDelegationToken != nil {
		return c.FnRenewDelegationToken(ctx, req)
	}
	return nil, ErrNotImplemented
}
