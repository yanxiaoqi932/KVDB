package ioselector

import (
	"os"
)

type FileIOSelector struct {
	fd *os.File // File descriptor
}

func NewFileIOelector(FName string, FSize int64) (IOSelector, error) {
	if FSize <= 0 {
		return nil, ErrInvalidFsize
	}
	file, err := OpenFile(FName, FSize)
	if err != nil {
		return nil, err
	}
	return &FileIOSelector{fd: file}, err
}

// 实现IOSelector接口的函数

func (fio *FileIOSelector) Write(b []byte, offset int64) (int, error) {
	return fio.fd.WriteAt(b, offset)
}

func (fio *FileIOSelector) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

func (fio *FileIOSelector) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIOSelector) Close() error {
	return fio.fd.Close()
}

func (fio *FileIOSelector) Delete() error {
	if err := fio.fd.Close(); err != nil {
		return err
	}
	return os.Remove(fio.fd.Name())
}
