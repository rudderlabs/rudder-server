package geolocation

import (
	"errors"
	"fmt"
	"io/fs"
	"net"

	"github.com/oschwald/maxminddb-golang"
)

var (
	ErrInvalidDatabase = errors.New("invalid database file")
	ErrInvalidIP       = errors.New("ip for lookup cannot be empty or invalid")
)

type maxmindDBReader struct {
	*maxminddb.Reader
}

func NewMaxmindDBReader(dbLoc string) (*maxmindDBReader, error) {
	reader, err := maxminddb.Open(dbLoc)
	if err != nil {

		if _, ok := err.(*fs.PathError); ok {
			return nil, ErrInvalidDatabase
		}
		if errors.As(err, &maxminddb.InvalidDatabaseError{}) {
			return nil, ErrInvalidDatabase
		}

		return nil, fmt.Errorf("opening maxmind reader from location: %w", err)
	}

	return &maxmindDBReader{reader}, nil
}

func (f *maxmindDBReader) Locate(ip string) (GeoInfo, error) {
	parsedIP := net.ParseIP(ip)

	if parsedIP == nil {
		return GeoInfo{}, ErrInvalidIP
	}

	info := GeoInfo{}
	if err := f.Lookup(parsedIP, &info); err != nil {
		return GeoInfo{}, fmt.Errorf("reading geolocation for ip: %w", err)
	}

	return info, nil
}

func (f *maxmindDBReader) Close() error {
	if err := f.Reader.Close(); err != nil {
		return fmt.Errorf("closing the underlying maxmind db reader: %w", err)
	}

	return nil
}
