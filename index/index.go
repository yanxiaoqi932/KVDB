package index

import (
	"encoding/binary"
	"errors"
	"kvdb/file"
	"sync"
	"time"
)

var (
	ErrInvalidDel = errors.New("index: Del index error")
	ErrInvalidPut = errors.New("index: Put index is already exist")
	ErrInvalidSeq = errors.New("index: List node seq < 0")
)

type StrIndexMana struct {
	Mu      *sync.RWMutex
	IdxTree *AdaptiveRadixTree
}

type ListIndexMana struct {
	Mu   *sync.RWMutex
	Tree map[string]*AdaptiveRadixTree
}

// 索引树中存储索引对应的信息，包括索引对应文件中decodeEntry的位置，以及entry的value和expiredAt信息
type IdxNode struct {
	Fid       uint32
	Offset    int64
	EntrySize int
	Value     []byte
	ExpiredAt int64 // time.Unix
}

// 对StrIndexMana中的索引进行插入或删除
func (strM *StrIndexMana) InsDelStrsIndex(ent *file.Entry, idxNode *IdxNode) error {
	strM.Mu.Lock()
	defer strM.Mu.Unlock()
	// 如果发现写入的是删除指令，或数据超时，则删除对应索引
	ts := time.Now().Unix()
	if ent.Type == file.EntryDel || (ent.ExpireAt != 0 && ent.ExpireAt < ts) {
		_, isDel := strM.IdxTree.Del(ent.Key)
		if !isDel {
			return ErrInvalidDel
		}
		return nil
	}
	// 写入正常指令，插入索引
	_, isExist := strM.IdxTree.Put(ent.Key, idxNode)
	// 如果已经有相同的旧key存在，则删除旧key节点，将新key节点写入
	if isExist {
		_, isDel := strM.IdxTree.Del(ent.Key)
		if !isDel {
			return ErrInvalidDel
		}
		strM.IdxTree.Put(ent.Key, idxNode)
	}
	return nil
}

// 查找StrIndexMana中的索引，不存在返回nil，存在返回IdxNode
func (strM *StrIndexMana) GetStrsIndex(key []byte) *IdxNode {
	strM.Mu.RLock()
	defer strM.Mu.RUnlock()
	idn, found := strM.IdxTree.Get(key)
	if !found {
		return nil
	}
	idxNode := idn.(*IdxNode)
	return idxNode
}

// 对ListIndexMana中的索引进行插入或删除，这里的entry.key是已经编码过的
func (listM *ListIndexMana) InsDelListIndex(ent *file.Entry, idxNode *IdxNode) error {
	listM.Mu.Lock()
	defer listM.Mu.Unlock()
	decKey, _ := DecodeKey(ent.Key)
	if listM.Tree[string(decKey)] == nil {
		listM.Tree[string(decKey)] = NewART()
	}
	keyTree := listM.Tree[string(decKey)]

	// 删除
	t := time.Now().Unix()
	if ent.Type == file.EntryDel || (ent.ExpireAt != 0 && ent.ExpireAt < t) {
		_, isDel := keyTree.Del(ent.Key)
		if !isDel {
			return ErrInvalidDel
		}
		return nil
	}

	// 插入
	_, isExist := keyTree.Put(ent.Key, idxNode)
	if isExist {
		return ErrInvalidPut
	}
	return nil
}

// 查找ListIndexMana中的索引
func (listM *ListIndexMana) GetListIndex(key []byte) *IdxNode {
	listM.Mu.RLock()
	defer listM.Mu.RUnlock()
	decKey, _ := DecodeKey(key)
	if listM.Tree[string(decKey)] == nil {
		listM.Tree[string(decKey)] = NewART()
	}
	keyTree := listM.Tree[string(decKey)]

	idn, isFound := keyTree.Get(key)
	if !isFound {
		return nil
	}
	return idn.(*IdxNode)
}

// 将seq和key连接起来，即为keyTree中该seq的索引
func EncodeKey(key []byte, seq uint32) []byte {
	encKey := make([]byte, len(key)+4)
	binary.LittleEndian.PutUint32(encKey[:4], seq)
	copy(encKey[4:], key[:])
	return encKey
}

// 分离seq和key
func DecodeKey(encKey []byte) ([]byte, uint32) {
	seq := binary.LittleEndian.Uint32(encKey[:4])
	key := make([]byte, len(encKey[4:]))
	copy(key[:], encKey[4:])
	return key, seq
}
