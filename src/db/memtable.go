package db

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/cloudnoize/el_gokv/src/plasma/datastructures"
	"github.com/cloudnoize/el_gokv/src/plasma/types"
	"github.com/cloudnoize/el_gokv/src/plasma/utils"
)

type MemTable struct {
	skiplist      datastructures.SkipList
	byteSize      uint64
	latestVersion uint64
	isFlushed     atomic.Bool
	isClosed      atomic.Bool
}

func NewMemTable(estimateCap uint64, p float64) *MemTable {
	return &MemTable{skiplist: *datastructures.NewSkipList(estimateCap, p)}
}

func (m *MemTable) Put(kv *types.KV) error {
	if m.isClosed.Load() {
		return fmt.Errorf("trying to insert to inactive memtable")
	}
	utils.Assert(kv.Version > m.latestVersion, "Input version is not higher than current version")
	m.latestVersion = kv.Version
	m.byteSize += uint64(len(kv.Key) + len(kv.Value))
	m.skiplist.Put(kv)
	return nil
}

func (m *MemTable) Get(key []byte) (types.VersionedValue, bool) {
	if m.isFlushed.Load() {
		return types.VersionedValue{}, false
	}
	ret, ok, _ := m.skiplist.Get(key)
	return ret, ok
}

func (m *MemTable) Size() uint64 {
	return m.skiplist.Size()
}

func (m *MemTable) ByteSize() uint64 {
	return m.byteSize
}

func (m *MemTable) Flush(f *os.File) error {
	if m.isFlushed.Load() {
		return fmt.Errorf("memtable is already flushed")
	}

	m.isFlushed.Store(true)
	return nil
}

type MemCache struct {
	cached []*MemTable
	cap    uint64
}

// func (m *MemTable) Flush(f *os.File) bool {

// }
