package file

import (
	"kvdb/util"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenFile(t *testing.T) {
	type args struct {
		FilePath string
		Fid      uint32
		Fsize    uint64
		Ftype    FileType
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"zero-size", args{"/tmp/kvdb", 0, 0, StrsFile}, true},
		{"small-size", args{"/tmp/kvdb", 1, 100, ListFile}, false},
		{"large-size", args{"/tmp/kvdb", 2, 1024 << 20, StrsFile}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := OpenFile(tt.args.FilePath, tt.args.Fid, tt.args.Fsize, tt.args.Ftype)
			if (err != nil) != tt.wantErr {
				t.Errorf("OpenFile() is error, err:%v, wantErr:%v", err, tt.wantErr)
			}
			if tt.wantErr != true && f == nil {
				t.Errorf("OpenFile() get file is nil, want not nil")
			}

			if f != nil && f.IOSelector != nil {
				f.Delete()
			}
		})
	}
}

func TestWriteFileEntry(t *testing.T) {
	f, err := OpenFile("/tmp/kvdb", 0, 1<<20, StrsFile)
	assert.Nil(t, err)
	opts := &util.Options{LogFileSizeThreshold: 1 << 20, DBPath: "/tmp/kvdb"}

	tests := []struct {
		name    string
		args    *Entry
		wantErr bool
		opts    *util.Options
	}{
		{"EntryDel", &Entry{[]byte("key 0"), []byte("value 0"), 0, EntryDel}, false, opts},
		{"EntryMeta", &Entry{[]byte("key 1"), []byte("value 1"), 0, EntryMeta}, false, opts},
		{"no value", &Entry{[]byte("key 1"), nil, 0, EntryMeta}, false, opts},
		{"nil", &Entry{nil, nil, 0, EntryMeta}, false, opts},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := WriteFlieEntry(tt.args, f, tt.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteFlieEntry() is error, err:%v, wantErr:%v", err, tt.wantErr)
			}
		})

	}
	if f != nil && f.IOSelector != nil {
		f.Delete()
	}
}

func TestReadFile(t *testing.T) {
	f, err1 := OpenFile("/tmp/kvdb", 0, 1<<20, StrsFile)
	assert.Nil(t, err1)
	opts := &util.Options{LogFileSizeThreshold: 1 << 20, DBPath: "/tmp/kvdb"}
	ents := []*Entry{
		{[]byte("key 1"), []byte("value 1"), 1000000, EntryMeta},
		{[]byte("key 2"), []byte(""), 1000000, EntryMeta},
		{[]byte(""), []byte(""), 1000000, EntryMeta},
	}
	var offsets []int64
	for _, ent := range ents {
		off := atomic.LoadInt64(&f.WriteAt)
		offsets = append(offsets, off)
		_, _, err2 := WriteFlieEntry(ent, f, opts)
		assert.Nil(t, err2)
	}

	tests := []struct {
		name    string
		offset  int64
		wantErr bool
	}{
		{"kv", offsets[0], false},
		{"nil key, value", offsets[1], false},
		{"nil key, nil value", offsets[2], false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ent, entSize, err := f.ReadFileEntry(tt.offset)
			if ent == nil || entSize == 0 {
				t.Errorf("ReadFileEntry() is nil")
			}
			if err != nil {
				t.Errorf("ReadFileEntry() is error, err:%v, wantErr:%v", err, tt.wantErr)
			}
		})
	}

	if f != nil && f.IOSelector != nil {
		f.Delete()
	}

}
