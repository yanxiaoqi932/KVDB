package file

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

type EntryType byte // EntryType大小只有一个byte
var (
	EntryDel  EntryType = 1
	EntryMeta EntryType = 2
)
var ErrInvalidEntrySize = errors.New("entry is nil or header is nil")

// MaxHeaderSize代表headerSize的最大值，之所以ksize和vsize大小是5，是因为它们经过了varint-encoded编码
var MaxHeaderSize = 4 + 1 + binary.MaxVarintLen32 + binary.MaxVarintLen32 + binary.MaxVarintLen64

type Entry struct {
	Key      []byte
	Value    []byte
	ExpireAt int64 //time.unix，该entry能够维持的时间（如果命令使用了SetEX的话）
	Type     EntryType
}

type EntryHeader struct {
	crc       uint32
	typ       EntryType
	kSize     uint32
	vSize     uint32
	expiresAt int64 //time.unix
}

// 将entry编码为以下格式，以便于储存在file中，返回编码后的buf及其大小
// +-------+--------+----------+------------+-----------+-------+---------+
// |  crc  |  type  | key size | value size | expiresAt |  key  |  value  |
// +-------+--------+----------+------------+-----------+-------+---------+
// |------------------------HEADER----------------------|
//
//	|--------------------------crc check---------------------------|
func EncodeEntry(e *Entry) ([]byte, int) {
	if e == nil {
		return nil, 0
	}
	header := make([]byte, MaxHeaderSize)
	header[4] = byte(e.Type) // 0~3是留给crc的
	// 依次写入内容
	kvIndex := 4 + 1
	kvIndex += binary.PutVarint(header[kvIndex:], int64(len(e.Key)))
	kvIndex += binary.PutVarint(header[kvIndex:], int64(len(e.Value)))
	kvIndex += binary.PutVarint(header[kvIndex:], e.ExpireAt)

	encodeSize := kvIndex + len(e.Key) + len(e.Value) // 编码的实际长度
	encodeEntry := make([]byte, encodeSize)
	copy(encodeEntry[:kvIndex], header[:])
	copy(encodeEntry[kvIndex:], e.Key)
	copy(encodeEntry[kvIndex+len(e.Key):], e.Value)

	crc := crc32.ChecksumIEEE(encodeEntry[4:])
	binary.LittleEndian.PutUint32(encodeEntry[:4], crc)

	return encodeEntry, encodeSize
}

// 将编码储存后的entry读取出来，并解码得到其中的EntryHeader，返回EntryHeader以及header的binary size
func DecodeHeader(buf []byte) (*EntryHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	crc := binary.LittleEndian.Uint32(buf[:4])
	typ := EntryType(buf[4])
	index := 5
	kSize, n := binary.Varint(buf[index:])
	index += n
	vSize, n := binary.Varint(buf[index:])
	index += n
	expireAt, n := binary.Varint(buf[index:])
	index += n
	header := &EntryHeader{
		crc:       crc,
		typ:       typ,
		kSize:     uint32(kSize),
		vSize:     uint32(vSize),
		expiresAt: expireAt,
	}
	return header, int64(index)

}

// 为了校验读取的entry，输入entry，以及header中|  type  | key size | value size | expiresAt |部分，
// 生成crc，比较是否与header中的crc一致
func GetEntryCrc(e *Entry, buf []byte) (uint32, error) {
	if e == nil || len(buf) <= 0 {
		return 0, ErrInvalidEntrySize
	}
	crc := crc32.ChecksumIEEE(buf[:])
	crc = crc32.Update(crc, crc32.IEEETable, e.Key)
	crc = crc32.Update(crc, crc32.IEEETable, e.Value)
	return crc, nil
}
