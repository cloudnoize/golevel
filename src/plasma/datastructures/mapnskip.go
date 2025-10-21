package datastructures

import (
	"fmt"

	"github.com/cloudnoize/el_gokv/src/plasma/types"
)

type MapNSkip struct {
	tsmap  *TSMap[*types.KV]
	sl     *SkipList
	slchan chan *types.KV
	size   uint64
	bytes  uint64
}

func NewMapNSkip(cap uint64) *MapNSkip {
	ms := &MapNSkip{tsmap: NewTSMap[*types.KV](cap), sl: NewSkipList(cap, 0.5), slchan: make(chan *types.KV, cap)}
	go ms.SlPutWorker()
	return ms
}

func (m *MapNSkip) SlPutWorker() {
	for kv := range m.slchan {
		if kv == nil {
			continue
		}
		key, _, ver := kv.Unpack()
		m.sl.Put(key, nil, ver)
	}
}

func (m *MapNSkip) Put(kv *types.KV) {
	m.tsmap.Put(kv.Key, kv)
	m.slchan <- kv
	m.size++
	m.bytes += uint64(len(kv.Key) + len(kv.Value))
}

func (m *MapNSkip) Size() uint64 {
	return m.size
}

func (m *MapNSkip) SizeBytes() uint64 {
	return m.bytes
}

func (m *MapNSkip) Get(key []byte) (types.VersionedValue, bool) {
	var zero types.VersionedValue
	if v, ok := m.tsmap.Get(key); ok {
		if len(v.Value) == 0 {
			fmt.Print("0")
		}
		return types.VersionedValue{Value: v.Value, Version: v.Version}, true
	}
	return zero, false
}
