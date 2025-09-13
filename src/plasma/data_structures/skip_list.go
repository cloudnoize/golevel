package datastructures

import (
	"bytes"

	"github.com/cloudnoize/el_gokv/src/plasma/utils"
)

type verValue struct {
	value   []byte
	version uint64
}
type node struct {
	Levels    []*node
	key       []byte
	verValues Stack[verValue]
}

func newNode(height uint16) *node {
	return &node{
		Levels: make([]*node, height, height),
	}
}

type SkipList struct {
	head      *node
	maxHeight uint16
	size      uint64
}

func NewSkipList(maxHeight uint16) *SkipList {
	return &SkipList{
		head:      newNode(maxHeight),
		maxHeight: maxHeight,
	}
}

// TODO should accept a version as well
func (s *SkipList) Put(key []byte, val []byte, version uint64) {
	ptrsToNewNode := make([]*node, s.maxHeight, s.maxHeight)
	ptrsFromNewNode := make([]*node, s.maxHeight, s.maxHeight)
	curr := s.head
	for lvl := int(s.maxHeight) - 1; lvl >= 0; lvl-- {
		next := curr.Levels[lvl]
		for true {
			if next == nil {
				//end of current lvl, all keys are smaller
				ptrsToNewNode[lvl] = curr
				ptrsFromNewNode[lvl] = nil
				break
			}
			res := bytes.Compare(key, next.key)
			if res == 0 {
				// 0 is equals, and equals means it's an update to the value
				utils.Assert(next.verValues.Top().version < version, "Input version is not higher than current version")
				next.verValues.Push(verValue{val, version})
				return
			}
			if res == 1 {
				//next key is bigger, this is the insertion point
				ptrsToNewNode[lvl] = curr
				ptrsFromNewNode[lvl] = next
				break
			}
			//next key is smaller, keep going
			curr = next
			next = curr.Levels[lvl]
		}
	}

	//TODO
	// 1 - get random height for new node with non equal distribution, maybe better to do it at the beginning of the func then we don't need to iterate all lvls
	// 2 - point all ptrsToNewNode to it
	// 3 - point all new node lvls to ptrsFromNewNode
}
