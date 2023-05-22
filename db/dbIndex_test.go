package kvdb

import (
	"fmt"
	"kvdb/file"
	"kvdb/index"
	"kvdb/util"
	"os"
	"sync"
	"testing"
)

func CreateDB() *KVdb {
	// 写入entry到strFile
	f1, _ := file.OpenFile("/tmp/kvdb", 0, 1<<20, file.StrsFile)
	opts := util.DefaultOpts()
	ents := []*file.Entry{
		{Key: []byte("key 1"), Value: []byte("value 5"), ExpireAt: 0, Type: file.EntryMeta},
		{Key: []byte("key 2"), Value: []byte("value 2"), ExpireAt: 0, Type: file.EntryMeta},
		{Key: []byte("key 3"), Value: []byte("value 3"), ExpireAt: 0, Type: file.EntryMeta},
		{Key: []byte("key 1"), Value: []byte("value 1"), ExpireAt: 0, Type: file.EntryMeta},
	}
	for _, ent := range ents {
		file.WriteFlieEntry(ent, f1, opts)
	}

	// 写入entry到listFile
	f2, _ := file.OpenFile("/tmp/kvdb", 0, 1<<20, file.ListFile)
	seqs := []uint32{1, 2, 3}
	ents2 := []*file.Entry{
		{Key: []byte("key 1"), Value: []byte("value 1"), ExpireAt: 0, Type: file.EntryMeta},
		{Key: []byte("key 2"), Value: []byte("value 2"), ExpireAt: 0, Type: file.EntryMeta},
		{Key: []byte("key 3"), Value: []byte("value 3"), ExpireAt: 0, Type: file.EntryMeta},
	}
	for i := 0; i < len(ents2); i++ {
		ents2[i].Key = index.EncodeKey(ents2[i].Key, seqs[i])
	}
	for _, ent := range ents2 {
		file.WriteFlieEntry(ent, f2, opts)
	}

	db := &KVdb{}
	db.ActiveFile = make(map[Datatype]*file.File)
	db.ActiveFile[String] = f1
	db.ActiveFile[List] = f2
	db.FidMap = map[Datatype][]uint32{}
	db.FidMap[String] = []uint32{0}
	db.FidMap[List] = []uint32{0}
	db.StrIndexMana = &index.StrIndexMana{
		Mu:      &sync.RWMutex{},
		IdxTree: index.NewART(),
	}
	db.ListIndexMana = &index.ListIndexMana{
		Mu:   &sync.RWMutex{},
		Tree: make(map[string]*index.AdaptiveRadixTree),
	}
	db.Opts = util.DefaultOpts()
	return db
}

func TestLoadIndexFromFiles(t *testing.T) {
	db := CreateDB()
	t.Run("test load", func(t *testing.T) {
		if err := db.LoadIndexFromFiles(); err != nil {
			t.Errorf("test load error, err:%v", err)
		}
	})
	for i := 0; i < NumOfDataType; i++ {
		db.ActiveFile[Datatype(i)].Delete()
	}
}

func TestReadEntry(t *testing.T) {
	db := CreateDB()
	db.LoadIndexFromFiles()
	type args struct {
		dataType Datatype
		key      []byte
		seq      uint32
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"read stringfile", args{String, []byte("key 1"), 1}, false},
		{"read listfile", args{List, []byte("key 1"), 1}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err := db.ReadEntry(tt.args.dataType, tt.args.key, tt.args.seq)
			if string(value) != "value 1" {
				t.Errorf("ReadEntry() error, read value %v, real value %v", string(value), "value 1")
			}
			if err != nil {
				t.Errorf("ReadEntry() error, err:%v, wantErr:%v", err, tt.wantErr)
			}
		})
	}
	for i := 0; i < NumOfDataType; i++ {
		db.ActiveFile[Datatype(i)].Delete()
	}
}

func TestWriteEntry(t *testing.T) {
	db := CreateDB()
	db.LoadIndexFromFiles()
	type args struct {
		dataType Datatype
		entry    *file.Entry
		seq      uint32
	}
	ents := []*file.Entry{
		{Key: []byte("key 6"), Value: []byte("value 6"), Type: file.EntryMeta},
		{Key: []byte("key 1"), Value: []byte(""), Type: file.EntryDel},
		{Key: []byte("key 6"), Value: []byte("value 6"), Type: file.EntryMeta},
		{Key: []byte("key 1"), Value: []byte(""), Type: file.EntryDel},
	}
	wantValue := []string{"value 6", "", "value 6", ""}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"insert stringfile", args{dataType: String, entry: ents[0]}, false},
		{"delete stringfile", args{dataType: String, entry: ents[1]}, false},
		{"insert listfile", args{dataType: List, entry: ents[2], seq: 6}, false},
		{"delete listfile", args{dataType: List, entry: ents[3], seq: 1}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := db.WriteEntry(tt.args.dataType, tt.args.entry, tt.args.seq); err != nil {
				fmt.Printf("test writing error:%v\n", err)
			}
		})
	}

	// 由于引用的原因，目前tests[2]和[3]的key都被seq编码了，所以只能重新赋值，当然也可以解码一下
	tests[2].args.entry = &file.Entry{Key: []byte("key 6"), Value: []byte("value 6"), Type: file.EntryMeta}
	tests[3].args.entry = &file.Entry{Key: []byte("key 1"), Value: []byte(""), Type: file.EntryDel}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, _ := db.ReadEntry(tt.args.dataType, tt.args.entry.Key, tt.args.seq)
			if string(value) != wantValue[i] {
				t.Errorf("WriteEntry() value error, datatype:%v, value:%v, wantValue:%v", tt.args.dataType, value, wantValue[i])
			}
		})

	}
	for i := 0; i < NumOfDataType; i++ {
		db.ActiveFile[Datatype(i)].Delete()
	}
}

func TestLoadFile(t *testing.T) {
	fname := []string{
		"/tmp/kvdb/log.strs.000000001",
		"/tmp/kvdb/log.strs.000000002",
		"/tmp/kvdb/log.list.000000001",
		"/tmp/kvdb/log.list.000000002",
	}
	for i := 0; i < len(fname); i++ {
		os.Create(fname[i])
	}
	t.Run("load file", func(t *testing.T) {
		db := &KVdb{}
		db.ActiveFile = make(map[Datatype]*file.File)
		db.ArchiveFile = make(map[Datatype]archivefiles)
		db.FidMap = map[Datatype][]uint32{}
		db.Opts = util.DefaultOpts()

		err := db.LoadFiles()
		if err != nil {
			t.Errorf("LoadFiles() error, err:%v", err)
		}
		fmt.Printf("db.FidMap:%v\n", db.FidMap)
		if len(db.FidMap[String]) != 2 || len(db.FidMap[List]) != 2 {
			t.Errorf("LoadFiles() error, db.FidMap:%v", db.FidMap)
		}
	})
	for i := 0; i < len(fname); i++ {
		os.Remove(fname[i])
	}
}
