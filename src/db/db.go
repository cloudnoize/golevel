package db

// DB first writes to wal then mmt
// There can be a single avtive mt and several waiting to flush mt.
type DB struct {
	activeMMT *MemTable
	cache     MemCache
	wal       *Wal
	version   uint64
}

// func NewDB() {

// }

// func (db *DB) Get(key []byte) (types.VersionedValue, bool) {
// 	//try for memtable first, then memcache then files, can be concurrent with respecting this error for the reply
// }

// func (db *DB) Put(key, value []byte) error {
// 	//increment version and insert to wal
// 	//in case memtable is full is should create new and swap
// 	//put in memtable for now we should use a global rw lock
// }
