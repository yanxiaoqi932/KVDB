package index

import (
	"kvdb/file"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsDelStrsIndex(t *testing.T) {
	inodes := []*IdxNode{
		{0, 0, 1 << 10, []byte("value 0"), 1000000},
		{1, 10, 1 << 10, []byte("value 1"), 1000000},
		{2, 20, 1 << 10, []byte("value 0"), 1000000},
	}
	ents := []*file.Entry{
		{Key: []byte("key 0"), Value: []byte("value 0"), ExpireAt: 0, Type: file.EntryMeta},
		{Key: []byte("key 1"), Value: []byte("value 1"), ExpireAt: 0, Type: file.EntryMeta},
		{Key: []byte("key 0"), Value: []byte("value 0"), ExpireAt: 0, Type: file.EntryDel},
	}
	strIndex := StrIndexMana{
		Mu:      &sync.RWMutex{},
		IdxTree: NewART(),
	}

	tests := []struct {
		name    string
		entry   *file.Entry
		inode   *IdxNode
		wantErr bool
	}{
		{"write 0", ents[0], inodes[0], false},
		{"write 1", ents[1], inodes[1], false},
		{"del 0", ents[2], inodes[2], false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := strIndex.InsDelStrsIndex(tt.entry, tt.inode); err != nil {
				t.Errorf("insert error, err:%v, wantErr:%v", err, tt.wantErr)
			}
		})
	}
}

func TestGetStrsIndex(t *testing.T) {
	inodes := []*IdxNode{
		{0, 0, 1 << 10, []byte("value 0"), 1000000},
		{1, 10, 1 << 10, []byte("value 1"), 1000000},
		{2, 20, 1 << 10, []byte("value 0"), 1000000},
	}
	ents := []*file.Entry{
		{Key: []byte("key 0"), Value: []byte("value 0"), ExpireAt: 0, Type: file.EntryMeta},
		{Key: []byte("key 1"), Value: []byte("value 1"), ExpireAt: 0, Type: file.EntryMeta},
		{Key: []byte("key 0"), Value: []byte("value 0"), ExpireAt: 0, Type: file.EntryDel},
	}
	strIndex := StrIndexMana{
		Mu:      &sync.RWMutex{},
		IdxTree: NewART(),
	}

	for i := 0; i < len(inodes); i++ {
		err := strIndex.InsDelStrsIndex(ents[i], inodes[i])
		assert.Nil(t, err)
	}

	tests := []struct {
		name    string
		key     []byte
		wantErr bool
	}{
		{"get existing key", []byte("key 1"), false},
		{"get inexistent key", []byte("key 0"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inode := strIndex.GetStrsIndex(tt.key)
			if (inode == nil) != tt.wantErr {
				t.Errorf("GetStrsIndex() error")
			}
		})
	}
}

func TestInsDelListIndex(t *testing.T) {
	inodes := []*IdxNode{
		{0, 0, 1 << 10, []byte("value 0"), 1000000},
		{1, 10, 1 << 10, []byte("value 1"), 1000000},
		{2, 20, 1 << 10, []byte("value 0"), 1000000},
	}
	ents := []*file.Entry{
		{Key: []byte("key 0"), Value: []byte("value 0"), ExpireAt: 0, Type: file.EntryMeta},
		{Key: []byte("key 1"), Value: []byte("value 1"), ExpireAt: 0, Type: file.EntryMeta},
		{Key: []byte("key 0"), Value: []byte("value 0"), ExpireAt: 0, Type: file.EntryDel},
	}
	seq := []uint32{0, 1, 0}
	listIndex := ListIndexMana{
		Mu:   &sync.RWMutex{},
		Tree: make(map[string]*AdaptiveRadixTree),
	}
	for i := 0; i < len(ents); i++ {
		ents[i].Key = EncodeKey(ents[i].Key, seq[i])
	}

	tests := []struct {
		name    string
		entry   *file.Entry
		inode   *IdxNode
		seq     uint32
		wantErr bool
	}{
		{"insert key 0", ents[0], inodes[0], seq[0], false},
		{"insert key 1", ents[1], inodes[1], seq[1], false},
		{"del key 0", ents[2], inodes[2], seq[2], false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := listIndex.InsDelListIndex(tt.entry, tt.inode)
			if err != nil {
				t.Errorf("InsDelListIndex() error, err:%v, wantErr:%v", err, tt.wantErr)
			}
		})
	}
}

func TestGetListIndex(t *testing.T) {
	inodes := []*IdxNode{
		{0, 0, 1 << 10, []byte("value 0"), 1000000},
		{1, 10, 1 << 10, []byte("value 1"), 1000000},
		{2, 20, 1 << 10, []byte("value 0"), 1000000},
	}
	ents := []*file.Entry{
		{Key: []byte("key 0"), Value: []byte("value 0"), ExpireAt: 0, Type: file.EntryMeta},
		{Key: []byte("key 1"), Value: []byte("value 1"), ExpireAt: 0, Type: file.EntryMeta},
		{Key: []byte("key 0"), Value: []byte("value 0"), ExpireAt: 0, Type: file.EntryDel},
	}
	seq := []uint32{0, 1, 0}
	listIndex := ListIndexMana{
		Mu:   &sync.RWMutex{},
		Tree: make(map[string]*AdaptiveRadixTree),
	}
	for i := 0; i < len(ents); i++ {
		ents[i].Key = EncodeKey(ents[i].Key, seq[i])
	}

	for i := 0; i < len(inodes); i++ {
		err := listIndex.InsDelListIndex(ents[i], inodes[i])
		assert.Nil(t, err)
	}

	tests := []struct {
		name    string
		key     []byte
		seq     uint32
		wantErr bool
	}{
		{"get existing key", ents[1].Key, 1, false},
		{"get inexistent key", ents[0].Key, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inode := listIndex.GetListIndex(tt.key)
			if (inode == nil) != tt.wantErr {
				t.Errorf("GetListIndex() error")
			}
		})
	}
}
