package kvdb

import (
	"fmt"
	"io"
	"kvdb/file"
	"kvdb/index"
	"kvdb/util"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// 作为新entry加入索引树的总函数接口
func (db *KVdb) InsDelIndex(dataType Datatype, ent *file.Entry, idxNode *index.IdxNode) error {
	switch dataType {
	case String:
		return db.StrIndexMana.InsDelStrsIndex(ent, idxNode)
	case List:
		return db.ListIndexMana.InsDelListIndex(ent, idxNode)
	default:
		return ErrDatatype
	}
}

// 作为查找索引树的总函数接口
func (db *KVdb) GetIndex(dataType Datatype, key []byte) *index.IdxNode {
	switch dataType {
	case String:
		return db.StrIndexMana.GetStrsIndex(key)
	case List:
		return db.ListIndexMana.GetListIndex(key)
	default:
		return nil
	}
}

func (db *KVdb) GetActiveFile(dataType Datatype) *file.File {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.ActiveFile[dataType]
}

// 根据datatype和fid获取file对象
func (db *KVdb) GetFile(dataType Datatype, fid uint32) *file.File {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if fid == uint32(len(db.FidMap[dataType])-1) {
		return db.ActiveFile[dataType]
	} else {
		return db.ArchiveFile[dataType][fid]
	}
}

func (db *KVdb) LoadFiles() error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	fileEnts, err := os.ReadDir(db.Opts.DBPath)
	if err != nil {
		return err
	}

	// 建立db.FidMap
	fidMap := make(map[Datatype][]uint32)
	for _, fInfo := range fileEnts {
		if strings.HasPrefix(fInfo.Name(), file.FilePrefix) {
			splitNames := strings.Split(fInfo.Name(), ".")
			fid, err := strconv.Atoi(splitNames[2])
			if err != nil {
				return err
			}
			typ := Datatype(file.FileTypeMap[splitNames[1]])
			fidMap[typ] = append(fidMap[typ], uint32(fid))
		}
	}
	// if db.FidMap == nil{
	// 	db.FidMap = make(map[Datatype][]uint32)
	// }
	db.FidMap = fidMap

	// 建立db.ActiveFile和db.ArchiveFile
	for dataType, fids := range fidMap {
		if db.ArchiveFile[dataType] == nil {
			db.ArchiveFile[dataType] = make(archivefiles)
		}
		if len(fids) == 0 {
			continue
		}

		sort.Slice(fids, func(i, j int) bool { // 对fids进行升序处理
			return fids[i] < fids[j]
		})

		for i, fid := range fids {
			fType := file.FileType(dataType)
			f, err := file.OpenFile(db.Opts.DBPath, fid, uint64(db.Opts.LogFileSizeThreshold), fType)
			if err != nil {
				return err
			}
			if i == len(fids)-1 {
				db.ActiveFile[dataType] = f
			} else {
				db.ArchiveFile[dataType][fid] = f
			}
		}
	}

	return nil
}

// 开启数据库时从文件开始读取并建立索引
func (db *KVdb) LoadIndexFromFiles() error {
	ReadFileHandle := func(dataType Datatype, wg *sync.WaitGroup) {
		defer wg.Done()

		fids := db.FidMap[dataType]
		if len(fids) == 0 {
			return
		}
		sort.Slice(fids, func(i, j int) bool { // 将fids升序排列
			return fids[i] < fids[j]
		})

		// 逐个读取该datatype中每个文件中的内容
		for i, fid := range fids {
			var f *file.File
			if i == (len(fids) - 1) {
				f = db.ActiveFile[dataType]
			} else {
				f = db.ArchiveFile[dataType][fid]
			}
			if f == nil {
				fmt.Printf("[dataType][fid]:%v %v is nil\n", dataType, fid)
			}

			var offset int64
			for {
				ent, entSize, err := f.ReadFileEntry(offset)
				// 如果文件读完，则立即停止
				if err != nil {
					if err == io.EOF || err == file.ErrEndOfEntry {
						break
					} else {
						fmt.Printf("entry read error, [dataType][fid][offset]:%v %v %v\n", dataType, fid, offset)
					}
				}
				inode := index.IdxNode{
					Fid: fid, Offset: int64(offset), EntrySize: int(entSize), ExpiredAt: ent.ExpireAt,
				}
				if db.Opts.IndexMode == util.KeyValueMemMode { // 根据模式决定是否存储value到索引树中
					inode.Value = ent.Value
				}
				if err := db.InsDelIndex(dataType, ent, &inode); err != nil { // 写入该entry对应的索引
					fmt.Printf("InsDelIndex() error, err:%v\n", err)
				}
				offset += entSize
			}
			if i == len(fids)-1 {
				atomic.StoreInt64(&f.WriteAt, int64(offset))
			}
		}
	}

	wg := new(sync.WaitGroup)
	wg.Add(NumOfDataType)
	for i := 0; i < NumOfDataType; i++ {
		go ReadFileHandle(Datatype(i), wg)
	}
	wg.Wait()
	return nil
}

// 将entry写入索引树和磁盘
func (db *KVdb) WriteEntry(dataType Datatype, ent *file.Entry, seq uint32) error {
	activeFile := db.GetActiveFile(dataType)
	// 创建indexNode
	_, entrySize := file.EncodeEntry(ent)
	offset := atomic.LoadInt64(&activeFile.WriteAt)
	fid := activeFile.Fid
	inode := &index.IdxNode{
		Fid: fid, Offset: offset, EntrySize: entrySize, ExpiredAt: ent.ExpireAt,
	}
	if db.Opts.IndexMode == util.KeyValueMemMode {
		inode.Value = ent.Value
	}

	// 修改entry
	if dataType == List {
		ent.Key = index.EncodeKey(ent.Key, seq)
	}

	// 更新索引树
	err := db.InsDelIndex(dataType, ent, inode)
	if err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	// 更新磁盘文件
	f, _, err2 := file.WriteFlieEntry(ent, activeFile, db.Opts)
	if err2 != nil {
		return err2
	}
	if f != nil { // 如果原active文件已满，则需要换成新文件，原文件移入archiveFile
		activeFile.Sync()
		activeFid := activeFile.Fid
		db.ArchiveFile[dataType][uint32(activeFid)] = activeFile
		db.ActiveFile[dataType] = f
		db.Discards[dataType].SetTotal(f.Fid, uint32(db.Opts.LogFileSizeThreshold))
		if db.Opts.Sync {
			db.ActiveFile[dataType].Sync()
		}
		return nil
	}

	// 如果原active文件未满，则无需更换
	if db.Opts.Sync {
		activeFile.Sync()
	}
	return nil
}

// 从内存或磁盘中读取key对应的entry.value
func (db *KVdb) ReadEntry(dataType Datatype, key []byte, seq uint32) ([]byte, error) {
	if dataType == List {
		key = index.EncodeKey(key, seq)
	}

	// 从索引读取inode
	inode := db.GetIndex(dataType, key)
	if inode == nil {
		return []byte(""), nil
	}

	// 如果是KeyValueMemMode，可以直接拿到value，不用访问磁盘的file
	if db.Opts.IndexMode == util.KeyValueMemMode {
		return inode.Value, nil
	}
	// 从磁盘读取entry的value
	f := db.GetFile(dataType, inode.Fid)
	ent, _, err := f.ReadFileEntry(int64(inode.Offset))
	if err != nil {
		return nil, err
	}
	return ent.Value, nil
}
