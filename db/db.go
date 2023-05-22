package kvdb

import (
	"errors"
	"kvdb/discard"
	"kvdb/file"
	"kvdb/index"
	"kvdb/util"
	"os"
	"path/filepath"
	"sync"
)

var (
	ErrDatatype = errors.New("db: Datatype is not exist")
)

type Datatype int8

const (
	String Datatype = iota
	List
)

const (
	flockPath   = "FLOCK"
	discardPath = "DISCARD"
)

const (
	NumOfDataType = 2
	InitialFid    = 0
)

type (
	KVdb struct {
		ActiveFile    map[Datatype]*file.File   // 活跃文件，是所有file中id最靠后的一个
		ArchiveFile   map[Datatype]archivefiles // 归档文件，除了ActiveFile之外，其它file都是ArchiveFile
		FidMap        map[Datatype][]uint32     // 数据类型对应文件的fids
		StrIndexMana  *index.StrIndexMana
		ListIndexMana *index.ListIndexMana
		Opts          *util.Options
		mu            sync.RWMutex
		Flock         *file.FileLockGuard
		Discards      map[Datatype]*discard.Discard
	}
	archivefiles map[uint32]*file.File
)

// 开启数据库，生成KVdb对象
func Open() (*KVdb, error) {
	opts := util.DefaultOpts()
	if !file.IsPathExist(opts.DBPath) {
		err := os.MkdirAll(opts.DBPath, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}
	filePath := filepath.Join(opts.DBPath, flockPath)
	flockLockGuard, err := file.AcquireFlock(filePath, false)
	if err != nil {
		return nil, err
	}

	db := &KVdb{
		ActiveFile:    make(map[Datatype]*file.File),
		ArchiveFile:   make(map[Datatype]archivefiles),
		FidMap:        make(map[Datatype][]uint32),
		StrIndexMana:  &index.StrIndexMana{Mu: new(sync.RWMutex), IdxTree: index.NewART()},
		ListIndexMana: &index.ListIndexMana{Mu: new(sync.RWMutex), Tree: make(map[string]*index.AdaptiveRadixTree)},
		Opts:          util.DefaultOpts(),
		mu:            sync.RWMutex{},
		Flock:         flockLockGuard,
	}

	if err := db.LoadFiles(); err != nil {
		return nil, err
	}

	if err := db.LoadIndexFromFiles(); err != nil {
		return nil, err
	}

	// 如果开启数据库时，下面没有一个文件，则先为每个类型的数据创建一个空的ActiveFile
	for i := 0; i < NumOfDataType; i++ {
		if db.ActiveFile[Datatype(i)] == nil {
			f, err := file.OpenFile(db.Opts.DBPath, InitialFid, uint64(db.Opts.LogFileSizeThreshold), file.FileType(i))
			if err != nil {
				return nil, err
			}
			db.Discards[Datatype(i)].SetTotal(f.Fid, uint32(db.Opts.LogFileSizeThreshold))
			db.ActiveFile[Datatype(i)] = f
			db.FidMap[Datatype(i)] = append(db.FidMap[Datatype(i)], uint32(0))
		}
	}

	// 初始化discard...
	if err := db.initDiscard(); err != nil {
		return nil, err
	}

	// 开始运行GC&Compaction协程...

	return db, nil
}

func (db *KVdb) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 关闭文件锁
	err := db.Flock.ReleaseFlock()
	if err != nil {
		return err
	}

	// 所有文件落盘并关闭
	for _, activeFile := range db.ActiveFile {
		activeFile.Sync()
		activeFile.Close()
	}
	for _, archiveFiles := range db.ArchiveFile {
		for _, file := range archiveFiles {
			file.Sync()
			file.Close()
		}
	}

	// 索引管理器全部删除
	db.StrIndexMana = nil
	db.ListIndexMana = nil

	// 关闭discard...
	for _, dis := range db.Discards {
		dis.CloseDisChan()
	}

	// 停止运行GC&Compaction协程...

	return nil
}

func (db *KVdb) initDiscard() error {
	dcPath := filepath.Join(db.Opts.DBPath, discardPath)
	if !file.IsPathExist(dcPath) {
		if err := os.MkdirAll(dcPath, os.ModePerm); err != nil {
			return err
		}
	}

	discards := make(map[Datatype]*discard.Discard)
	for i := 0; i <= NumOfDataType; i++ {
		name := file.FileNamesMap[file.FileType(i)] + discard.DiscardName // 例如log.strs.discard
		dis, err := discard.NewDiscard(dcPath, name, db.Opts.ChanBufferSize)
		if err != nil {
			return err
		}
		discards[Datatype(i)] = dis
	}

	db.Discards = discards
	return nil
}
