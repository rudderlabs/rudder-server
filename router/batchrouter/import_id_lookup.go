package batchrouter

import (
	"context"
	"database/sql"
	stdjson "encoding/json"
	"errors"
	"fmt"

	"github.com/segmentio/ksuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-server/utils/tx"
)

// Importing job statuses carry either the import ID itself (importId) or a reference to a row of the
// async_import_ids table that holds it (importIdRef), never both.
const (
	importIDKey    = "importId"
	importIDRefKey = "importIdRef"
)

var errImportIDNotFound = errors.New("import ID not found")

// asyncImportID is a row of the async_import_ids table.
type asyncImportID struct {
	destinationID string
	ref           string
	importID      string // raw JSON of the original import ID
}

// newImportIDRef returns the row that stores the import ID of importingParameters under a new reference,
// and a copy of importingParameters that carries the reference in place of the import ID.
// The row is nil if importingParameters have no import ID.
func newImportIDRef(destinationID string, importingParameters []byte) (*asyncImportID, []byte, error) {
	importID := gjson.GetBytes(importingParameters, importIDKey)
	if !importID.Exists() {
		return nil, importingParameters, nil
	}
	row := &asyncImportID{destinationID: destinationID, ref: ksuid.New().String(), importID: importID.Raw}
	refParameters, err := sjson.DeleteBytes(importingParameters, importIDKey)
	if err != nil {
		return nil, nil, fmt.Errorf("removing %s: %w", importIDKey, err)
	}
	refParameters, err = sjson.SetBytes(refParameters, importIDRefKey, row.ref)
	if err != nil {
		return nil, nil, fmt.Errorf("setting %s: %w", importIDRefKey, err)
	}
	return row, refParameters, nil
}

// store inserts the row and deletes the other rows of its destination. A destination has at most one
// import in flight, so no status references the deleted rows.
//
// Storing the same row again is a no-op, so that a retried transaction whose commit outcome was unknown
// succeeds.
func (row asyncImportID) store(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO async_import_ids (destination_id, id, import_id) VALUES ($1, $2, $3)
		ON CONFLICT (destination_id, id) DO NOTHING`,
		row.destinationID, row.ref, row.importID); err != nil {
		return fmt.Errorf("inserting import ID %s: %w", row.ref, err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM async_import_ids WHERE destination_id = $1 AND id <> $2`,
		row.destinationID, row.ref); err != nil {
		return fmt.Errorf("deleting older import IDs: %w", err)
	}
	return nil
}

// resolveImportParameters returns importing status parameters that carry the import ID itself. Parameters
// with an import ID reference are returned as a copy with the referenced import ID in its place; it returns
// an error wrapping errImportIDNotFound if the referenced row does not exist.
func (brt *Handle) resolveImportParameters(ctx context.Context, destinationID string, parameters stdjson.RawMessage) (stdjson.RawMessage, error) {
	ref := gjson.GetBytes(parameters, importIDRefKey)
	if !ref.Exists() {
		return parameters, nil
	}
	ctx, cancel := context.WithTimeout(ctx, brt.jobdDBQueryRequestTimeout.Load())
	defer cancel()
	var importID string
	err := brt.jobsDB.WithTx(ctx, func(tx *tx.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT import_id FROM async_import_ids WHERE destination_id = $1 AND id = $2`,
			destinationID, ref.String()).Scan(&importID)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", errImportIDNotFound, ref.String())
	}
	if err != nil {
		return nil, fmt.Errorf("loading import ID %s: %w", ref.String(), err)
	}
	resolved, err := sjson.DeleteBytes(parameters, importIDRefKey)
	if err != nil {
		return nil, fmt.Errorf("removing %s: %w", importIDRefKey, err)
	}
	resolved, err = sjson.SetRawBytes(resolved, importIDKey, []byte(importID))
	if err != nil {
		return nil, fmt.Errorf("setting %s: %w", importIDKey, err)
	}
	return resolved, nil
}
