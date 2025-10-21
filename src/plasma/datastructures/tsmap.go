package datastructures

import (
	"bytes"
	"hash/fnv"
	"sync"
)

type Node[T any] struct {
	key   []byte
	value T
	next  *Node[T]
}

type Bucket[T any] struct {
	head Node[T]
	lock sync.RWMutex
}

// TODO bench against golang conc map
type TSMap[T any] struct {
	buckets    []Bucket[T]
	nunBuckets uint64
	size       uint64
}

func NewTSMap[T any](numBuckets uint64) *TSMap[T] {
	return &TSMap[T]{buckets: make([]Bucket[T], numBuckets, numBuckets), nunBuckets: numBuckets}
}

func hash(key []byte) uint64 {
	h := fnv.New64a()
	h.Write(key)
	return h.Sum64()
}

// Get the latest version
func (m *TSMap[T]) Get(key []byte) (T, bool) {
	var zero T
	h := hash(key)
	idx := h % m.nunBuckets
	m.buckets[idx].lock.RLock()
	defer m.buckets[idx].lock.RUnlock()
	curr := m.buckets[idx].head.next
	for curr != nil {
		if bytes.Equal(key, curr.key) {
			return curr.value, true
		}
		curr = curr.next
	}
	return zero, false
}

// Put will not try to find the key and update, but inserts a key first
// and knows that get will return on first match
func (m *TSMap[T]) Put(key []byte, value T) {
	h := hash(key)
	idx := h % m.nunBuckets
	nn := &Node[T]{key: key, value: value}
	m.buckets[idx].lock.Lock()
	defer m.buckets[idx].lock.Unlock()
	nn.next = m.buckets[idx].head.next
	m.buckets[idx].head.next = nn
	m.size++
}

func (m *TSMap[T]) Size() uint64 {
	return m.size
}
