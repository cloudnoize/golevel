package db

import (
	"os"

	"github.com/cloudnoize/el_gokv/src/plasma/datastructures"
	"github.com/cloudnoize/el_gokv/src/plasma/types"
)

type MemTable struct {
	skiplist datastructures.SkipList
}

func (m *MemTable) Put(kv *types.KV) {

}

func (m *MemTable) Get(key []byte) (*types.VersionedValue, bool) {
}

func (m *MemTable) Flush(f *os.File) bool {

}
