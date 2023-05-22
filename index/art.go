package index

import (
	goart "github.com/plar/go-adaptive-radix-tree"
)

type AdaptiveRadixTree struct {
	tree goart.Tree
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, value interface{}) (oldValue interface{}, isExist bool) {
	return art.tree.Insert(key, value)
}

func (art *AdaptiveRadixTree) Get(key []byte) (value interface{}, isfound bool) {
	val, found := art.tree.Search(key)
	return val, found
}

func (art *AdaptiveRadixTree) Del(key []byte) (value interface{}, isDel bool) {
	return art.tree.Delete(key)
}
