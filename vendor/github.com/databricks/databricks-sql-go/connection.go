package dbsql

import (
	"context"
	"database/sql/driver"
	"time"

	"github.com/databricks/databricks-sql-go/driverctx"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/internal/rows"
	"github.com/databricks/databricks-sql-go/internal/sentinel"
	"github.com/databricks/databricks-sql-go/logger"
	"github.com/pkg/errors"
)

type conn struct {
	id      string
	cfg     *config.Config
	client  cli_service.TCLIService
	session *cli_service.TOpenSessionResp
}

// Prepare prepares a statement with the query bound to this connection.
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{conn: c, query: query}, nil
}

// PrepareContext prepares a statement with the query bound to this connection.
// Currently, PrepareContext does not use context and is functionally equivalent to Prepare.
func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &stmt{conn: c, query: query}, nil
}

// Close closes the session.
// sql package maintains a free pool of connections and only calls Close when there's a surplus of idle connections.
func (c *conn) Close() error {
	log := logger.WithContext(c.id, "", "")
	ctx := driverctx.NewContextWithConnId(context.Background(), c.id)

	_, err := c.client.CloseSession(ctx, &cli_service.TCloseSessionReq{
		SessionHandle: c.session.SessionHandle,
	})

	if err != nil {
		log.Err(err).Msg("databricks: failed to close connection")
		return dbsqlerrint.NewRequestError(ctx, dbsqlerr.ErrCloseConnection, err)
	}
	return nil
}

// Not supported in Databricks.
func (c *conn) Begin() (driver.Tx, error) {
	return nil, dbsqlerrint.NewDriverError(context.TODO(), dbsqlerr.ErrNotImplemented, nil)
}

// Not supported in Databricks.
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, dbsqlerrint.NewDriverError(context.TODO(), dbsqlerr.ErrNotImplemented, nil)
}

// Ping attempts to verify that the server is accessible.
// Returns ErrBadConn if ping fails and consequently DB.Ping will remove the conn from the pool.
func (c *conn) Ping(ctx context.Context) error {
	log := logger.WithContext(c.id, driverctx.CorrelationIdFromContext(ctx), "")
	ctx = driverctx.NewContextWithConnId(ctx, c.id)
	ctx1, cancel := context.WithTimeout(ctx, c.cfg.PingTimeout)
	defer cancel()
	_, err := c.QueryContext(ctx1, "select 1", nil)
	if err != nil {
		log.Err(err).Msg("databricks: failed to ping")
		return driver.ErrBadConn
	}
	return nil
}

// ResetSession is called prior to executing a query on the connection.
// The session with this driver does not have any important state to reset before re-use.
func (c *conn) ResetSession(ctx context.Context) error {
	return nil
}

// IsValid signals whether a connection is valid or if it should be discarded.
func (c *conn) IsValid() bool {
	return c.session.GetStatus().StatusCode == cli_service.TStatusCode_SUCCESS_STATUS
}

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// ExecContext honors the context timeout and return when it is canceled.
// Statement ExecContext is the same as connection ExecContext
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	corrId := driverctx.CorrelationIdFromContext(ctx)
	log := logger.WithContext(c.id, corrId, "")
	msg, start := logger.Track("ExecContext")
	defer log.Duration(msg, start)

	ctx = driverctx.NewContextWithConnId(ctx, c.id)
	if len(args) > 0 {
		return nil, dbsqlerrint.NewDriverError(ctx, dbsqlerr.ErrParametersNotSupported, nil)
	}
	exStmtResp, opStatusResp, err := c.runQuery(ctx, query, args)

	if exStmtResp != nil && exStmtResp.OperationHandle != nil {
		// we have an operation id so update the logger
		log = logger.WithContext(c.id, corrId, client.SprintGuid(exStmtResp.OperationHandle.OperationId.GUID))

		// since we have an operation handle we can close the operation if necessary
		alreadyClosed := exStmtResp.DirectResults != nil && exStmtResp.DirectResults.CloseOperation != nil
		newCtx := driverctx.NewContextWithCorrelationId(driverctx.NewContextWithConnId(context.Background(), c.id), corrId)
		if !alreadyClosed && (opStatusResp == nil || opStatusResp.GetOperationState() != cli_service.TOperationState_CLOSED_STATE) {
			_, err1 := c.client.CloseOperation(newCtx, &cli_service.TCloseOperationReq{
				OperationHandle: exStmtResp.OperationHandle,
			})
			if err1 != nil {
				log.Err(err1).Msg("databricks: failed to close operation after executing statement")
			}
		}
	}
	if err != nil {
		log.Err(err).Msgf("databricks: failed to execute query: query %s", query)
		return nil, dbsqlerrint.NewExecutionError(ctx, dbsqlerr.ErrQueryExecution, err, opStatusResp)
	}

	res := result{AffectedRows: opStatusResp.GetNumModifiedRows()}

	return &res, nil
}

// QueryContext executes a query that may return rows, such as a
// SELECT.
//
// QueryContext honors the context timeout and return when it is canceled.
// Statement QueryContext is the same as connection QueryContext
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	corrId := driverctx.CorrelationIdFromContext(ctx)
	log := logger.WithContext(c.id, corrId, "")
	msg, start := log.Track("QueryContext")

	ctx = driverctx.NewContextWithConnId(ctx, c.id)
	if len(args) > 0 {
		return nil, dbsqlerrint.NewDriverError(ctx, dbsqlerr.ErrParametersNotSupported, nil)
	}
	// first we try to get the results synchronously.
	// at any point in time that the context is done we must cancel and return
	exStmtResp, opStatusResp, err := c.runQuery(ctx, query, args)

	if exStmtResp != nil && exStmtResp.OperationHandle != nil {
		ctx = driverctx.NewContextWithQueryId(ctx, client.SprintGuid(exStmtResp.OperationHandle.OperationId.GUID))
		log = logger.WithContext(c.id, driverctx.CorrelationIdFromContext(ctx), client.SprintGuid(exStmtResp.OperationHandle.OperationId.GUID))
	}
	defer log.Duration(msg, start)

	if err != nil {
		log.Err(err).Msg("databricks: failed to run query") // To log query we need to redact credentials
		return nil, dbsqlerrint.NewExecutionError(ctx, dbsqlerr.ErrQueryExecution, err, opStatusResp)
	}
	// hold on to the operation handle
	opHandle := exStmtResp.OperationHandle

	rows, err := rows.NewRows(c.id, corrId, opHandle, c.client, c.cfg, exStmtResp.DirectResults)

	return rows, err

}

func (c *conn) runQuery(ctx context.Context, query string, args []driver.NamedValue) (*cli_service.TExecuteStatementResp, *cli_service.TGetOperationStatusResp, error) {
	log := logger.WithContext(c.id, driverctx.CorrelationIdFromContext(ctx), "")
	// first we try to get the results synchronously.
	// at any point in time that the context is done we must cancel and return
	exStmtResp, err := c.executeStatement(ctx, query, args)

	if err != nil {
		return exStmtResp, nil, err
	}
	opHandle := exStmtResp.OperationHandle
	if opHandle != nil && opHandle.OperationId != nil {
		ctx = driverctx.NewContextWithQueryId(ctx, client.SprintGuid(opHandle.OperationId.GUID))
		log = logger.WithContext(
			c.id,
			driverctx.CorrelationIdFromContext(ctx), driverctx.QueryIdFromContext(ctx),
		)
	}

	if exStmtResp.DirectResults != nil {
		opStatus := exStmtResp.DirectResults.GetOperationStatus()

		switch opStatus.GetOperationState() {
		// terminal states
		// good
		case cli_service.TOperationState_FINISHED_STATE:
			return exStmtResp, opStatus, nil
		// bad
		case cli_service.TOperationState_CANCELED_STATE,
			cli_service.TOperationState_CLOSED_STATE,
			cli_service.TOperationState_ERROR_STATE,
			cli_service.TOperationState_TIMEDOUT_STATE:
			logBadQueryState(log, opStatus)
			return exStmtResp, opStatus, unexpectedOperationState(opStatus)
		// live states
		case cli_service.TOperationState_INITIALIZED_STATE,
			cli_service.TOperationState_PENDING_STATE,
			cli_service.TOperationState_RUNNING_STATE:
			statusResp, err := c.pollOperation(ctx, opHandle)
			if err != nil {
				return exStmtResp, statusResp, err
			}
			switch statusResp.GetOperationState() {
			// terminal states
			// good
			case cli_service.TOperationState_FINISHED_STATE:
				return exStmtResp, statusResp, nil
			// bad
			case cli_service.TOperationState_CANCELED_STATE,
				cli_service.TOperationState_CLOSED_STATE,
				cli_service.TOperationState_ERROR_STATE,
				cli_service.TOperationState_TIMEDOUT_STATE:
				logBadQueryState(log, statusResp)
				return exStmtResp, statusResp, unexpectedOperationState(statusResp)
				// live states
			default:
				logBadQueryState(log, statusResp)
				return exStmtResp, statusResp, invalidOperationState(ctx, statusResp)
			}
		// weird states
		default:
			logBadQueryState(log, opStatus)
			return exStmtResp, opStatus, invalidOperationState(ctx, opStatus)
		}

	} else {
		statusResp, err := c.pollOperation(ctx, opHandle)
		if err != nil {
			return exStmtResp, statusResp, err
		}
		switch statusResp.GetOperationState() {
		// terminal states
		// good
		case cli_service.TOperationState_FINISHED_STATE:
			return exStmtResp, statusResp, nil
		// bad
		case cli_service.TOperationState_CANCELED_STATE,
			cli_service.TOperationState_CLOSED_STATE,
			cli_service.TOperationState_ERROR_STATE,
			cli_service.TOperationState_TIMEDOUT_STATE:
			logBadQueryState(log, statusResp)
			return exStmtResp, statusResp, unexpectedOperationState(statusResp)
			// live states
		default:
			logBadQueryState(log, statusResp)
			return exStmtResp, statusResp, invalidOperationState(ctx, statusResp)
		}
	}
}

func logBadQueryState(log *logger.DBSQLLogger, opStatus *cli_service.TGetOperationStatusResp) {
	log.Error().Msgf("databricks: query state: %s", opStatus.GetOperationState())
	log.Error().Msg(opStatus.GetDisplayMessage())
	log.Debug().Msg(opStatus.GetDiagnosticInfo())
}

func unexpectedOperationState(opStatus *cli_service.TGetOperationStatusResp) error {
	return errors.WithMessage(errors.New(opStatus.GetDisplayMessage()), dbsqlerr.ErrUnexpectedOperationState(opStatus.GetOperationState().String()))
}

func invalidOperationState(ctx context.Context, opStatus *cli_service.TGetOperationStatusResp) error {
	return dbsqlerrint.NewDriverError(ctx, dbsqlerr.ErrInvalidOperationState(opStatus.GetOperationState().String()), nil)
}

func (c *conn) executeStatement(ctx context.Context, query string, args []driver.NamedValue) (*cli_service.TExecuteStatementResp, error) {
	corrId := driverctx.CorrelationIdFromContext(ctx)
	log := logger.WithContext(c.id, corrId, "")

	req := cli_service.TExecuteStatementReq{
		SessionHandle: c.session.SessionHandle,
		Statement:     query,
		RunAsync:      c.cfg.RunAsync,
		QueryTimeout:  int64(c.cfg.QueryTimeout / time.Second),
		GetDirectResults: &cli_service.TSparkGetDirectResults{
			MaxRows: int64(c.cfg.MaxRows),
		},
		CanDecompressLZ4Result_: &c.cfg.UseLz4Compression,
	}

	if c.cfg.UseArrowBatches {
		req.CanReadArrowResult_ = &c.cfg.UseArrowBatches
		req.UseArrowNativeTypes = &cli_service.TSparkArrowTypes{
			DecimalAsArrow:       &c.cfg.UseArrowNativeDecimal,
			TimestampAsArrow:     &c.cfg.UseArrowNativeTimestamp,
			ComplexTypesAsArrow:  &c.cfg.UseArrowNativeComplexTypes,
			IntervalTypesAsArrow: &c.cfg.UseArrowNativeIntervalTypes,
		}
	}

	if c.cfg.UseCloudFetch {
		req.CanDownloadResult_ = &c.cfg.UseCloudFetch
	}

	ctx = driverctx.NewContextWithConnId(ctx, c.id)
	resp, err := c.client.ExecuteStatement(ctx, &req)

	var shouldCancel = func(resp *cli_service.TExecuteStatementResp) bool {
		if resp == nil {
			return false
		}
		hasHandle := resp.OperationHandle != nil
		isOpen := resp.DirectResults != nil && resp.DirectResults.CloseOperation == nil
		return hasHandle && isOpen
	}

	select {
	default:
	case <-ctx.Done():
		newCtx := driverctx.NewContextWithCorrelationId(driverctx.NewContextWithConnId(context.Background(), c.id), corrId)
		// in case context is done, we need to cancel the operation if necessary
		if err == nil && shouldCancel(resp) {
			log.Debug().Msg("databricks: canceling query")
			_, err1 := c.client.CancelOperation(newCtx, &cli_service.TCancelOperationReq{
				OperationHandle: resp.GetOperationHandle(),
			})

			if err1 != nil {
				log.Err(err).Msgf("databricks: cancel failed")
			} else {
				log.Debug().Msgf("databricks: cancel success")
			}
		} else {
			log.Debug().Msg("databricks: query did not need cancellation")
		}
		return nil, ctx.Err()
	}

	return resp, err
}

func (c *conn) pollOperation(ctx context.Context, opHandle *cli_service.TOperationHandle) (*cli_service.TGetOperationStatusResp, error) {
	corrId := driverctx.CorrelationIdFromContext(ctx)
	log := logger.WithContext(c.id, corrId, client.SprintGuid(opHandle.OperationId.GUID))
	var statusResp *cli_service.TGetOperationStatusResp
	ctx = driverctx.NewContextWithConnId(ctx, c.id)
	newCtx := driverctx.NewContextWithCorrelationId(driverctx.NewContextWithConnId(context.Background(), c.id), corrId)
	pollSentinel := sentinel.Sentinel{
		OnDoneFn: func(statusResp any) (any, error) {
			return statusResp, nil
		},
		StatusFn: func() (sentinel.Done, any, error) {
			var err error
			log.Debug().Msg("databricks: polling status")
			statusResp, err = c.client.GetOperationStatus(newCtx, &cli_service.TGetOperationStatusReq{
				OperationHandle: opHandle,
			})

			if statusResp != nil && statusResp.OperationState != nil {
				log.Debug().Msgf("databricks: status %s", statusResp.GetOperationState().String())
			}
			return func() bool {
				if err != nil {
					return true
				}
				switch statusResp.GetOperationState() {
				case cli_service.TOperationState_INITIALIZED_STATE,
					cli_service.TOperationState_PENDING_STATE,
					cli_service.TOperationState_RUNNING_STATE:
					return false
				default:
					log.Debug().Msg("databricks: polling done")
					return true
				}
			}, statusResp, err
		},
		OnCancelFn: func() (any, error) {
			log.Debug().Msg("databricks: canceling query")
			ret, err := c.client.CancelOperation(newCtx, &cli_service.TCancelOperationReq{
				OperationHandle: opHandle,
			})
			return ret, err
		},
	}
	status, resp, err := pollSentinel.Watch(ctx, c.cfg.PollInterval, 0)
	if err != nil {
		if status == sentinel.WatchTimeout {
			err = dbsqlerrint.NewRequestError(ctx, dbsqlerr.ErrSentinelTimeout, err)
		}
		return nil, err
	}

	statusResp, ok := resp.(*cli_service.TGetOperationStatusResp)
	if !ok {
		return nil, dbsqlerrint.NewDriverError(ctx, dbsqlerr.ErrReadQueryStatus, nil)
	}
	return statusResp, nil
}

var _ driver.Conn = (*conn)(nil)
var _ driver.Pinger = (*conn)(nil)
var _ driver.SessionResetter = (*conn)(nil)
var _ driver.Validator = (*conn)(nil)
var _ driver.ExecerContext = (*conn)(nil)
var _ driver.QueryerContext = (*conn)(nil)
var _ driver.ConnPrepareContext = (*conn)(nil)
var _ driver.ConnBeginTx = (*conn)(nil)
