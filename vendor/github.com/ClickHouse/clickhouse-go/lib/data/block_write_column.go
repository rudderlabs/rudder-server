package data

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/lib/column"
	"net"
	"reflect"
	"time"

	"github.com/ClickHouse/clickhouse-go/lib/binary"
)

func (block *Block) WriteDate(c int, v time.Time) error {
	_, offset := v.Zone()
	nday := (v.Unix() + int64(offset)) / 24 / 3600
	return block.buffers[c].Column.UInt16(uint16(nday))
}

func (block *Block) WriteDateNullable(c int, v *time.Time) error {
	if v == nil {
		return block.buffers[c].Column.UInt16Nullable(nil)
	}
	unixTime := uint16((*v).Unix() / 24 / 3600)
	return block.buffers[c].Column.UInt16Nullable(&unixTime)
}

func (block *Block) WriteDateTime(c int, v time.Time) error {
	return block.buffers[c].Column.UInt32(uint32(v.Unix()))
}

func (block *Block) WriteBool(c int, v bool) error {
	if v {
		return block.buffers[c].Column.UInt8(1)
	}
	return block.buffers[c].Column.UInt8(0)
}

func (block *Block) WriteDateTimeNullable(c int, v *time.Time) error {
	if v == nil {
		return block.buffers[c].Column.UInt32Nullable(nil)
	}
	unixTime := uint32((*v).Unix())
	return block.buffers[c].Column.UInt32Nullable(&unixTime)
}

func (block *Block) WriteInt8(c int, v int8) error {
	return block.buffers[c].Column.Int8(v)
}

func (block *Block) WriteInt8Nullable(c int, v *int8) error {
	return block.buffers[c].Column.Int8Nullable(v)
}

func (block *Block) WriteInt16(c int, v int16) error {
	return block.buffers[c].Column.Int16(v)
}

func (block *Block) WriteInt16Nullable(c int, v *int16) error {
	return block.buffers[c].Column.Int16Nullable(v)
}

func (block *Block) WriteInt32(c int, v int32) error {
	return block.buffers[c].Column.Int32(v)
}

func (block *Block) WriteInt32Nullable(c int, v *int32) error {
	return block.buffers[c].Column.Int32Nullable(v)
}

func (block *Block) WriteInt64(c int, v int64) error {
	return block.buffers[c].Column.Int64(v)
}

func (block *Block) WriteInt64Nullable(c int, v *int64) error {
	return block.buffers[c].Column.Int64Nullable(v)
}

func (block *Block) WriteUInt8(c int, v uint8) error {
	return block.buffers[c].Column.UInt8(v)
}

func (block *Block) WriteUInt8Nullable(c int, v *uint8) error {
	return block.buffers[c].Column.UInt8Nullable(v)
}

func (block *Block) WriteUInt16(c int, v uint16) error {
	return block.buffers[c].Column.UInt16(v)
}

func (block *Block) WriteUInt16Nullable(c int, v *uint16) error {
	return block.buffers[c].Column.UInt16Nullable(v)
}

func (block *Block) WriteUInt32(c int, v uint32) error {
	return block.buffers[c].Column.UInt32(v)
}

func (block *Block) WriteUInt32Nullable(c int, v *uint32) error {
	return block.buffers[c].Column.UInt32Nullable(v)
}

func (block *Block) WriteUInt64(c int, v uint64) error {
	return block.buffers[c].Column.UInt64(v)
}

func (block *Block) WriteUInt64Nullable(c int, v *uint64) error {
	return block.buffers[c].Column.UInt64Nullable(v)
}

func (block *Block) WriteFloat32(c int, v float32) error {
	return block.buffers[c].Column.Float32(v)
}

func (block *Block) WriteFloat32Nullable(c int, v *float32) error {
	return block.buffers[c].Column.Float32Nullable(v)
}

func (block *Block) WriteFloat64(c int, v float64) error {
	return block.buffers[c].Column.Float64(v)
}

func (block *Block) WriteFloat64Nullable(c int, v *float64) error {
	return block.buffers[c].Column.Float64Nullable(v)
}

func (block *Block) WriteBytes(c int, v []byte) error {
	if err := block.buffers[c].Column.Uvarint(uint64(len(v))); err != nil {
		return err
	}
	if _, err := block.buffers[c].Column.Write(v); err != nil {
		return err
	}
	return nil
}

func (block *Block) WriteBytesNullable(c int, v *[]byte) error {
	isNil := v == nil
	if err := block.buffers[c].Column.Nullable(isNil); err != nil {
		return err
	}
	if isNil {
		return block.WriteBytes(c, []byte{})
	}
	return block.WriteBytes(c, *v)
}

func (block *Block) WriteString(c int, v string) error {
	if err := block.buffers[c].Column.Uvarint(uint64(len(v))); err != nil {
		return err
	}
	if _, err := block.buffers[c].Column.Write(binary.Str2Bytes(v)); err != nil {
		return err
	}
	return nil
}

func (block *Block) WriteStringNullable(c int, v *string) error {
	isNil := v == nil
	if err := block.buffers[c].Column.Nullable(isNil); err != nil {
		return err
	}
	if isNil {
		return block.WriteString(c, "")
	}
	return block.WriteString(c, *v)
}

func (block *Block) WriteFixedString(c int, v []byte) error {
	return block.Columns[c].Write(block.buffers[c].Column, v)
}

func (block *Block) WriteIP(c int, v net.IP) error {
	return block.Columns[c].Write(block.buffers[c].Column, v)
}

func (block *Block) WriteArray(c int, v interface{}) error {
	return block.WriteArrayWithValue(c, newValue(reflect.ValueOf(v)))
}

func (block *Block) WriteArrayWithValue(c int, value Value) error {
	if value.Kind() != reflect.Slice {
		return fmt.Errorf("unsupported Array(T) type [%T]", value.Interface())
	}
	return block.writeArray(block.Columns[c], value, c, 1)
}

func (block *Block) WriteFixedStringNullable(c int, v *[]byte) error {
	writer := block.Columns[c].(*column.Nullable)
	return writer.WriteNull(block.buffers[c].Offset, block.buffers[c].Column, v)
}

func (block *Block) WriteArrayNullable(c int, v *interface{}) error {
	if err := block.buffers[c].Column.Nullable(v == nil); err != nil {
		return err
	}
	if v == nil {
		return block.Columns[c].Write(block.buffers[c].Column, []string{})
	}
	return block.WriteArray(c, *v)
}
