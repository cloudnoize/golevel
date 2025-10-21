package datastructures

import (
	"bytes"
	"math"

	"github.com/cloudnoize/el_gokv/src/plasma/probability"
	"github.com/cloudnoize/el_gokv/src/plasma/types"
	"github.com/cloudnoize/el_gokv/src/plasma/utils"
)

type node struct {
	levels    []*node
	key       []byte
	verValues Stack[types.VersionedValue]
}

func newNode(height uint16) *node {
	return &node{
		levels: make([]*node, height),
	}
}

// TODO I think that we can add a hashmap of from key_version -> value
// it will be inserted before the skiplist and the skip list will contain only the key, and will go to the hashmap for the value.
// this way gets will be o1. the skiplist insert can be done in a different go routine with a channel, as its need to be ready only when flushed.
// it will reduec the contention as well as it's easier to implement a concurrent map.
type SkipList struct {
	head         *node
	maxHeight    uint16
	estimatedCap uint64
	size         uint64
	UpdatesSize  uint64
	p            float64
}

func NewSkipList(estimateCap uint64, p float64) *SkipList {
	utils.Assert(utils.IsPowerOf2(estimateCap), "Not a power of two")
	mh := uint16(math.Log2(float64(estimateCap)))
	return &SkipList{
		head:         newNode(mh),
		maxHeight:    mh,
		estimatedCap: estimateCap,
		p:            p,
	}
}

func (s SkipList) Size() uint64 {
	return s.size + s.UpdatesSize
}

func (s *SkipList) PutKV(kv *types.KV) {
	s.Put(kv.Unpack())
}

func (s *SkipList) Put(key, value []byte, version uint64) {
	ptrsToNewNode := make([]*node, s.maxHeight)
	ptrsFromNewNode := make([]*node, s.maxHeight)
	curr := s.head
	for lvl := int(s.maxHeight) - 1; lvl >= 0; lvl-- {
		next := curr.levels[lvl]
		for {
			if next == nil {
				//end of current lvl, all keys are smaller
				ptrsToNewNode[lvl] = curr
				ptrsFromNewNode[lvl] = nil
				break
			}
			res := bytes.Compare(key, next.key)
			if res == 0 {
				// 0 is equals, and equals means it's an update to the value
				utils.Assert(next.verValues.Top().Version < version, "Input version is not higher than current version")
				next.verValues.Push(types.VersionedValue{Value: value, Version: version})
				s.UpdatesSize++
				return
			}
			if res == -1 {
				//next key is bigger, this is the insertion point
				ptrsToNewNode[lvl] = curr
				ptrsFromNewNode[lvl] = next
				break
			}
			//next key is smaller, keep going
			curr = next
			next = curr.levels[lvl]
		}
	}
	defer func() { s.size++ }()
	nodeHeight := uint16(math.Min(probability.Geometric(s.p)+1, float64(s.maxHeight)))
	node := newNode(nodeHeight)
	node.key = key
	node.verValues.Push(types.VersionedValue{Value: value, Version: version})
	// insert the new node, make each layer poit to it and from it
	//TODO lock
	for i := 0; i < int(nodeHeight); i++ {
		ptrsToNewNode[i].levels[i] = node
		node.levels[i] = ptrsFromNewNode[i]
	}
}

// TODO also version
func (s SkipList) Get(key []byte) (types.VersionedValue, bool, uint64) {
	steps := uint64(0)
	curr := s.head
	for lvl := int(s.maxHeight) - 1; lvl >= 0; lvl-- {
		next := curr.levels[lvl]
		steps++
		for {
			if next == nil {
				//end of current lvl, all keys are smaller
				break
			}
			res := bytes.Compare(key, next.key)
			if res == 0 {
				return next.verValues.Top(), true, steps
			}
			if res == -1 {
				// next key is bigger,need to go down a level
				break
			}
			//next key is smaller, keep going
			curr = next
			next = curr.levels[lvl]
			steps++
		}
	}
	return types.VersionedValue{}, false, steps
}

type Iterator struct {
	curr *node
}

func (s *SkipList) Iterator() *Iterator {
	return &Iterator{curr: s.head.levels[0]}
}

func (it *Iterator) Next() {
	if it.curr != nil {
		it.curr = it.curr.levels[0]
	}
}

func (it *Iterator) Dref() *types.KV {
	if it.curr == nil {
		return nil
	}
	return &types.KV{Key: it.curr.key, Value: it.curr.verValues.Top().Value, Version: it.curr.verValues.Top().Version}
}
