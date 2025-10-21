package types

type KV struct {
	Key     []byte
	Value   []byte
	Version uint64
}

func (kv KV) Unpack() ([]byte, []byte, uint64) {
	return kv.Key, kv.Value, kv.Version
}

type VersionedValue struct {
	Value   []byte
	Version uint64
}

type KVDB interface {
	Put(kv *KV)
	Get(key []byte) (VersionedValue, bool)
	Size() uint64
	//TODO multiput
}
