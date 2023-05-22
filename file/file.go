package file

import (
	"errors"
	"fmt"
	"hash/crc32"
	"kvdb/file/ioselector"
	"kvdb/util"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

type FileType int8

const (
	StrsFile FileType = iota
	ListFile
)

const FilePrefix = "log."

var (
	ErrInvalidFileType   = errors.New("logfile: file type entered does not exist")
	ErrInvalidBufSize    = errors.New("logfile: buf size is wrong")
	ErrEndOfEntry        = errors.New("logfile: end of entry in file")
	ErrInvalidKVSize     = errors.New("logfile: entry's kv size is wrong")
	ErrInvalidCRC        = errors.New("logfile: entry crc is error")
	ErrWriteSizeNotEqual = errors.New("logfile: write size is not equal to entry size")
)

var (
	FileNamesMap = map[FileType]string{
		StrsFile: "log.strs.",
		ListFile: "log.list.",
	}

	FileTypeMap = map[string]FileType{
		"strs": StrsFile,
		"list": ListFile,
	}
)

type File struct {
	sync.RWMutex
	Fid        uint32
	WriteAt    int64
	IOSelector ioselector.IOSelector
	Ftype      FileType
}

// 根据信息打开磁盘中对应file，并构建File对象
func OpenFile(FilePath string, Fid uint32, Fsize uint64, Ftype FileType) (*File, error) {
	fileName, err := GetFileName(FilePath, Fid, Ftype)
	if err != nil {
		return nil, err
	}
	ioselector, err := ioselector.NewFileIOelector(fileName, int64(Fsize))
	if err != nil {
		return nil, err
	}
	f := &File{
		Fid:        Fid,
		IOSelector: ioselector,
		Ftype:      Ftype,
	}
	return f, nil
}

// 输入entry在file中的offset，读取并返回entry，entrysize和error
func (f *File) ReadFileEntry(offset int64) (*Entry, int64, error) {
	// 获取header
	headerBuf, err := f.Read(offset, uint32(MaxHeaderSize))
	if err != nil {
		return nil, 0, err
	}
	header, headerSize := DecodeHeader(headerBuf)
	if header.crc == 0 && header.kSize == 0 && header.vSize == 0 {
		return nil, 0, ErrEndOfEntry
	}

	// 根据header的信息获取key和value
	offsetkv := offset + headerSize
	kv, err := f.Read(offsetkv, header.kSize+header.vSize)
	if err != nil {
		return nil, 0, err
	}
	key := kv[:header.kSize]
	value := kv[header.kSize:]

	// 建立entry
	ent := &Entry{
		Key:      key,
		Value:    value,
		ExpireAt: header.expiresAt,
		Type:     header.typ,
	}
	entrySize := headerSize + int64(header.kSize) + int64(header.vSize)

	// 对key和value进行校验
	crc, err := GetEntryCrc(ent, headerBuf[crc32.Size:headerSize])
	if err != nil {
		return nil, 0, err
	}
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return ent, entrySize, nil
}

// 对file文件输入一个entry，对其进行编码成buf，如果超过阈值则生成新文件返回，另外返回entrysize和error
func WriteFlieEntry(ent *Entry, activeFile *File, opts *util.Options) (*File, int, error) {
	if ent == nil {
		return nil, 0, ErrInvalidEntrySize
	}
	buf, bufSize := EncodeEntry(ent)

	// 原文件存储空间不够，新建一个做存储，原文件的sync等后续操作，这里一概不做处理
	activeWriteAt := atomic.LoadInt64(&activeFile.WriteAt)
	if activeWriteAt+int64(bufSize) > opts.LogFileSizeThreshold {
		newFileId := activeFile.Fid + 1
		filePath := opts.DBPath
		f, err := OpenFile(filePath, newFileId, uint64(opts.LogFileSizeThreshold), activeFile.Ftype)
		if err != nil {
			return nil, 0, err
		}
		if err := f.Write(buf); err != nil {
			return nil, 0, err
		}
		return f, bufSize, nil

	} else { // 原文件存储空间足够，直接使用原文件
		if err := activeFile.Write(buf); err != nil {
			return nil, 0, err
		}
		return nil, bufSize, nil
	}

}

// 从文件的指定offset开始读取指定size的内容并返回
func (f *File) Read(offset int64, size uint32) ([]byte, error) {
	f.RWMutex.RLock()
	defer f.RWMutex.RUnlock()
	if size <= 0 {
		return []byte{}, nil
	}
	buf := make([]byte, size)
	_, err := f.IOSelector.Read(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// 在文件的指定offset写入buf，返回error
func (f *File) Write(buf []byte) error {
	f.RWMutex.Lock()
	defer f.RWMutex.Unlock()
	offset := atomic.LoadInt64(&f.WriteAt)
	bufSize, err := f.IOSelector.Write(buf, offset) // 原子获取文件当前的写入偏移量
	if err != nil {
		return err
	}
	if bufSize != len(buf) {
		return ErrWriteSizeNotEqual
	}
	atomic.AddInt64(&f.WriteAt, int64(bufSize)) // 原子更新文件当前的写入偏移量
	return nil
}

// 将文件从page cache中刷入磁盘
func (f *File) Sync() error {
	return f.IOSelector.Sync()
}

// 关闭文件，一般在数据库结束运行时调用
func (f *File) Close() error {
	return f.IOSelector.Close()
}

// 删除文件，一般在 GC&Compact 时调用
func (f *File) Delete() error {
	return f.IOSelector.Delete()
}

// 获取文件路径加文件名称
func GetFileName(FilePath string, Fid uint32, Ftype FileType) (string, error) {
	if _, ok := FileNamesMap[Ftype]; !ok {
		return "", ErrInvalidFileType
	}
	FileName := FileNamesMap[Ftype] + fmt.Sprintf("%09d", Fid)
	name := filepath.Join(FilePath, FileName)
	return name, nil
}

// 检查路径是否存在，存在则返回true
func IsPathExist(FilePath string) bool {
	if _, err := os.Stat(FilePath); os.IsNotExist(err) {
		return false
	}
	return true
}
