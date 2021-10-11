package clickhouse

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/lib/data"
	"github.com/ClickHouse/clickhouse-go/lib/protocol"
)

func (ch *clickhouse) sendQuery(ctx context.Context, query string, externalTables []ExternalTable) error {
	ch.logf("[send query] %s", query)
	if err := ch.encoder.Uvarint(protocol.ClientQuery); err != nil {
		return err
	}
	var queryID string
	queryIDValue := ctx.Value(queryIDKey)
	if queryIDValue != nil {
		if queryIdStr, ok := queryIDValue.(string); ok {
			queryID = queryIdStr
		}
	}
	if err := ch.encoder.String(queryID); err != nil {
		return err
	}
	{ // client info
		ch.encoder.Uvarint(1)
		ch.encoder.String("")
		ch.encoder.String("")
		ch.encoder.String("[::ffff:127.0.0.1]:0")
		ch.encoder.Uvarint(1) // iface type TCP
		ch.encoder.String(hostname)
		ch.encoder.String(hostname)
	}
	if err := ch.ClientInfo.Write(ch.encoder); err != nil {
		return err
	}
	if ch.ServerInfo.Revision >= protocol.DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
		ch.encoder.String("")
	}

	// the settings are written as list of contiguous name-value pairs, finished with empty name
	if !ch.settings.IsEmpty() {
		ch.logf("[query settings] %s", ch.settings.settingsStr)
		if err := ch.settings.Serialize(ch.encoder); err != nil {
			return err
		}
	}
	// empty string is a marker of the end of the settings
	if err := ch.encoder.String(""); err != nil {
		return err
	}
	if err := ch.encoder.Uvarint(protocol.StateComplete); err != nil {
		return err
	}
	compress := protocol.CompressDisable
	if ch.compress {
		compress = protocol.CompressEnable
	}
	if err := ch.encoder.Uvarint(compress); err != nil {
		return err
	}
	if err := ch.encoder.String(query); err != nil {
		return err
	}
	if err := ch.sendExternalTables(externalTables); err != nil {
		return err
	}
	if err := ch.writeBlock(&data.Block{}, ""); err != nil {
		return err
	}
	return ch.encoder.Flush()
}
