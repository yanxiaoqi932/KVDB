package ioselector

import (
	"errors"
	"os"
)

const FilePerm = 0644

var ErrInvalidFsize = errors.New("fsize can`t be zero or negative")

type IOSelector interface {
	// Writes bytes to file according to offset, returns size of bytes and error
	Write(b []byte, offset int64) (int, error)

	// Reads bytes from file according to offset, returns size of bytes and error
	Read(b []byte, offset int64) (int, error)

	// Sync content of cache to disk
	Sync() error

	// Close the file
	Close() error

	// Delete the file
	Delete() error
}

func OpenFile(FName string, FSize int64) (*os.File, error) {

	fd, err := os.OpenFile(FName, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}

	stat, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	if stat.Size() < FSize {
		if err := fd.Truncate(FSize); err != nil {
			return nil, err
		}
	}

	return fd, err

}
