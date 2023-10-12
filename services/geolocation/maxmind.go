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

type dbReader struct {
	reader *maxminddb.Reader
}

func NewMaxmindDBReader(dbLoc string) (*dbReader, error) {
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
	return &dbReader{reader}, nil
}

func (f *dbReader) GeoIP(ip string) (*GeoCity, error) {
	parsedIP := net.ParseIP(ip)
	if parsedIP == nil {
		return nil, ErrInvalidIP
	}

	lookup := GeoCity{}
	if err := f.reader.Lookup(parsedIP, &lookup); err != nil {
		return nil, fmt.Errorf("reading geolocation for ip: %w", err)
	}

	return &lookup, nil
}
