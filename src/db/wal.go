package db

type Wal struct {
	//last version that was flushed to file, i.e. everything above it is volatile
	waterMark uint64
}
