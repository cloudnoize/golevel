package db

// DB first writes to wal then mmt
// There can be a single avtive mt and several waiting to flush mt.
type DB struct {
	activeMMT  *MemTable
	pendingMMT []MemTable
	wal        *Wal
	version    uint64
}

func NewDB() {

}
