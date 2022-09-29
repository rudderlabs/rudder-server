/*
OS Interface Use this instead of os package.

usage example

import "github.com/rudderlabs/rudder-server/utils/sysUtils"

var	Os sysUtils.OsI = &sysUtils.Os{}
			or
var	Os sysUtils.OsI = sysUtils.NewOs()

...

Os.UserHomeDir()
*/

//go:generate mockgen -destination=../../mocks/utils/sysUtils/mock_os.go -package mock_sysUtils github.com/rudderlabs/rudder-server/utils/sysUtils OsI
package sysUtils

import (
	"os"
)

type OsI interface {
	IsNotExist(err error) bool
	Getenv(key string) string
	Create(name string) (*os.File, error)
	Open(name string) (*os.File, error)
	MkdirAll(path string, perm os.FileMode) error
	OpenFile(name string, flag int, perm os.FileMode) (*os.File, error)
	Remove(name string) error
	Stat(name string) (os.FileInfo, error)
	UserHomeDir() (string, error)
	LookupEnv(key string) (string, bool)
}
type Os struct{}

// NewZip returns a Os instance
func NewOs() OsI {
	return &Os{}
}

// IsNotExist returns a boolean indicating whether the error is known to report that a file or directory does not exist.
// It is satisfied by ErrNotExist as well as some syscall errors.
func (*Os) IsNotExist(err error) bool {
	return os.IsNotExist(err)
}

// Getenv retrieves the value of the environment variable named by the key.
// It returns the value, which will be empty if the variable is not present.
// To distinguish between an empty value and an unset value, use LookupEnv.
func (*Os) Getenv(key string) string {
	return os.Getenv(key)
}

// UserHomeDir returns the current user's home directory.
//
// On Unix, including macOS, it returns the $HOME environment variable.
// On Windows, it returns %USERPROFILE%.
// On Plan 9, it returns the $home environment variable.
func (*Os) UserHomeDir() (string, error) {
	return os.UserHomeDir()
}

// Create creates or truncates the named file. If the file already exists,
// it is truncated. If the file does not exist, it is created with mode 0666
// (before umask). If successful, methods on the returned File can
// be used for I/O; the associated file descriptor has mode O_RDWR.
// If there is an error, it will be of type *PathError.
func (*Os) Create(name string) (*os.File, error) {
	return os.Create(name)
}

// Open opens the named file for reading. If successful, methods on
// the returned file can be used for reading; the associated file
// descriptor has mode O_RDONLY.
// If there is an error, it will be of type *PathError.
func (*Os) Open(name string) (*os.File, error) {
	return os.Open(name)
}

// MkdirAll creates a directory named path,
// along with any necessary parents, and returns nil,
// or else returns an error.
// The permission bits perm (before umask) are used for all
// directories that MkdirAll creates.
// If path is already a directory, MkdirAll does nothing
// and returns nil.
func (*Os) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

// OpenFile is the generalized open call; most users will use Open
// or Create instead. It opens the named file with specified flag
// (O_RDONLY etc.). If the file does not exist, and the O_CREATE flag
// is passed, it is created with mode perm (before umask). If successful,
// methods on the returned File can be used for I/O.
// If there is an error, it will be of type *PathError.
func (*Os) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}

// Stat returns a FileInfo describing the named file.
// If there is an error, it will be of type *PathError.
func (*Os) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

// Remove removes the named file or (empty) directory.
// If there is an error, it will be of type *PathError.
func (*Os) Remove(name string) error {
	return os.Remove(name)
}

// LookupEnv retrieves the value of the environment variable named
// by the key. If the variable is present in the environment the
// value (which may be empty) is returned and the boolean is true.
// Otherwise the returned value will be empty and the boolean will
// be false.
func (*Os) LookupEnv(key string) (string, bool) {
	return os.LookupEnv(key)
}
