package column

import (
	"github.com/ClickHouse/clickhouse-go/lib/binary"
)

type UInt32 struct{ base }

func (UInt32) Read(decoder *binary.Decoder, isNull bool) (interface{}, error) {
	v, err := decoder.UInt32()
	if err != nil {
		return uint32(0), err
	}
	return v, nil
}

func (u *UInt32) Write(encoder *binary.Encoder, v interface{}) error {
	switch v := v.(type) {
	case uint32:
		return encoder.UInt32(v)
	case uint64:
		return encoder.UInt32(uint32(v))
	case int64:
		return encoder.UInt32(uint32(v))
	case int:
		return encoder.UInt32(uint32(v))

	// this relies on Nullable never sending nil values through
	case *uint64:
		return encoder.UInt32(uint32(*v))
	case *uint32:
		return encoder.UInt32(*v)
	case *int64:
		return encoder.UInt32(uint32(*v))
	case *int:
		return encoder.UInt32(uint32(*v))
	}

	return &ErrUnexpectedType{
		T:      v,
		Column: u,
	}
}
