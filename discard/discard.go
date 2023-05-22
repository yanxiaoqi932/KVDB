package discard

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"kvdb/file"
	"kvdb/file/ioselector"
	"kvdb/index"
	"path/filepath"
	"sort"
	"sync"
)

// format of discard file` record:
// +-------+--------------+----------------+  +-------+--------------+----------------+
// |  fid  |  total size  | discarded size |  |  fid  |  total size  | discarded size |
// +-------+--------------+----------------+  +-------+--------------+----------------+
// 0-------4--------------8---------------12  12------16------------20----------------24
// total size代表该磁盘文件的总容量，discarded size代表该磁盘文件中可以丢弃部分的容量
const (
	discardEntSize  uint32 = 12
	discardFileSize int64  = 2 << 12
	DiscardName            = "discard"
)

var ErrInvalidAlloc = errors.New("discard: there is not space in discard file")

type Discard struct {
	sync.RWMutex
	once    *sync.Once
	file    ioselector.IOSelector
	valChan chan *index.IdxNode
	// 存储discard文件中空白区域的每个offset，空白区域包括两种：1.discard文件后面的大片空白区；2.discard文件中间，被删除的fid留下的空白区
	freeList []int64
	// 存储discard文件中存储数据区域的每个offset
	location map[uint32]int64
}

func NewDiscard(path, name string, chanBufferSize int) (*Discard, error) {
	fileName := filepath.Join(path, name)
	disFile, err := ioselector.NewFileIOelector(fileName, discardFileSize)
	if err != nil {
		return nil, err
	}

	// 读取discard文件中所有的内容
	var location = make(map[uint32]int64)
	var freeList []int64
	var offset int64
	for {
		buf := make([]byte, 8)
		_, err := disFile.Read(buf, offset)
		if err != nil {
			if err == io.EOF || err == file.ErrEndOfEntry {
				break
			}
			return nil, err
		}
		fid := binary.LittleEndian.Uint32(buf[:4])
		total := binary.LittleEndian.Uint32(buf[4:8])
		if fid == 0 && total == 0 { // 空白区域
			freeList = append(freeList, offset)
		} else { // 非空白区域
			location[fid] = offset
		}

		offset += int64(discardEntSize)
	}

	dc := &Discard{
		once:     new(sync.Once),
		file:     disFile,
		valChan:  make(chan *index.IdxNode, chanBufferSize),
		freeList: freeList,
		location: location,
	}

	// 运行ds的listen进程...
	go dc.ListenUpdates()

	return dc, nil
}

// CCL means compaction cnadidate list.
func (dc *Discard) GetCCL(activeFid uint32, ratio float64) ([]uint32, error) {
	dc.RLock()
	defer dc.RUnlock()

	// 获取 discardSize / total 超出阈值ratio的文件的fid列表
	var offset uint32
	var ccl []uint32
	for {

		buf := make([]byte, discardEntSize)
		_, err := dc.file.Read(buf, int64(offset))
		if err != nil {
			if err == io.EOF || err == file.ErrEndOfEntry {
				break
			}
			return nil, err
		}

		fid := binary.LittleEndian.Uint32(buf[:4])
		total := binary.LittleEndian.Uint32(buf[4:8])
		discardSize := binary.LittleEndian.Uint32(buf[8:12])

		var curRatio float64 = 0
		if discardSize != 0 && total != 0 {
			curRatio = float64(discardSize / total)
		}
		if curRatio >= ratio && fid != activeFid {
			ccl = append(ccl, fid)
		}

		offset += discardEntSize
	}

	sort.Slice(ccl, func(i, j int) bool {
		return ccl[i] < ccl[j]
	})

	return ccl, nil
}

func (dc *Discard) ListenUpdates() {
	for {
		select {
		case inode, ok := <-dc.valChan:
			if !ok {
				if err := dc.file.Close(); err != nil {
					fmt.Printf("Error discard file.Close()\n")
				}
				return
			}
			dc.incrDiscard(inode.Fid, inode.EntrySize)
		}
	}
}

// 对fid文件的discard量进行增加操作，一般在删除某个kv时使用
func (dc *Discard) incrDiscard(fid uint32, incrSize int) {
	if incrSize > 0 {
		dc.updateDiscard(fid, incrSize)
	} else {
		fmt.Printf("discard: IncrDiscard < 0\n")
	}
}

// 删除fid文件在discard文件中的信息
func (dc *Discard) Clear(fid uint32) {
	dc.updateDiscard(fid, -1)

	dc.Lock()
	defer dc.Unlock()
	if offset, ok := dc.location[fid]; ok {
		dc.freeList = append(dc.freeList, offset) // 将discard文件中删除fid后的空白区域offset存入freelist中
		delete(dc.location, fid)
	}
}

// 对discard文件中指定fid的信息进行增量或删除操作
func (dc *Discard) updateDiscard(fid uint32, incrSize int) {
	dc.Lock()
	defer dc.Unlock()
	offset, err := dc.alloc(fid) // 获取fid在discard文件中对应的offset，如果没有记录该fid，就返回一个空白位置的offset
	if err != nil {
		fmt.Printf("%v\n", err)
	}

	var buf []byte
	if incrSize > 0 { // 增量操作
		offset += 8
		buf = make([]byte, 8)
		_, err = dc.file.Read(buf, offset)
		if err != nil {
			fmt.Printf("%v\n", err)
		}

		discardSize := binary.LittleEndian.Uint32(buf)
		binary.LittleEndian.PutUint32(buf, discardSize+uint32(incrSize))
	} else { // 删除操作
		buf = make([]byte, discardEntSize)
	}
	dc.file.Write(buf, offset)

}

// 检查文件是否写入了discard文件中，如果没有则写入，一般用于新建文件
func (dc *Discard) SetTotal(fid uint32, total uint32) {
	if _, ok := dc.location[fid]; ok { // 已经存在于discard文件中
		return
	}

	offset, err := dc.alloc(fid) // 不存在与discard文件中，因此新加入
	if err != nil {
		fmt.Printf("%v\n", err)
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[:4], fid)
	binary.LittleEndian.PutUint32(buf[4:8], total)
	if _, err = dc.file.Write(buf, offset); err != nil {
		fmt.Printf("%v\n", err)
	}
}

// 给出fid在discard文件中对应的offset，如果discard中没有则返回空白区域的一个offset
func (dc *Discard) alloc(fid uint32) (int64, error) {
	if offset, ok := dc.location[fid]; ok { // 如果discard中记录了fid的信息，则直接返回offset
		return offset, nil
	}

	if len(dc.freeList) == 0 { // 如果discard文件没有空余空间，则返回error
		return 0, ErrInvalidAlloc
	}

	offset := dc.freeList[len(dc.freeList)-1]
	dc.freeList = dc.freeList[:len(dc.freeList)-1]
	dc.location[fid] = offset
	return offset, nil
}

func (dc *Discard) CloseDisChan() {
	dc.once.Do(func() { close(dc.valChan) })
}
