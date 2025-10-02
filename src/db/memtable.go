package db

import (
	"github.com/cloudnoize/el_gokv/src/plasma/datastructures"
	"github.com/cloudnoize/el_gokv/src/plasma/types"
)

type MemTable struct {
	skiplist datastructures.SkipList
	byteSize uint64
}

func NewMemTable(estimateCap uint64, p float64) *MemTable {
	return &MemTable{skiplist: *datastructures.NewSkipList(estimateCap, p)}
}

func (m *MemTable) Put(kv *types.KV) {
	m.byteSize += uint64(len(kv.Key) + len(kv.Value))
	m.skiplist.Put(kv)
}

func (m *MemTable) Get(key []byte) (types.VersionedValue, bool) {
	ret, ok, _ := m.skiplist.Get(key)
	return ret, ok
}

func (m *MemTable) Size() uint64 {
	return m.skiplist.Size()
}

func (m *MemTable) ByteSize() uint64 {
	return m.byteSize
}

// func (m *MemTable) Flush(f *os.File) bool {

// }
