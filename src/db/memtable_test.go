package db

import (
	"bytes"
	"testing"

	"github.com/cloudnoize/el_gokv/src/plasma/types"
)

func TestMemTable_putAndGet(t *testing.T) {
	mmt := NewMemTable(1024)
	key := []byte("key")
	version := 1
	value := []byte("Val")
	expectedSize := uint64(len(key) + len(value))
	mmt.Put(&types.KV{Key: key, Value: value, Version: uint64(version)})
	if mmt.Size() != 1 {
		t.Fatalf("Expected size 1 , actual %d", mmt.Size())
	}
	if mmt.ByteSize() != expectedSize {
		t.Fatalf("Expected byte size %d , actual %d", expectedSize, mmt.ByteSize())
	}
	if v, ok := mmt.Get([]byte("doesn't exist")); ok {
		t.Fatalf("Expected not to get a value but received %x", v)
	}
	var out types.VersionedValue
	var ok bool
	if out, ok = mmt.Get(key); !ok {
		t.Fatalf("Expected to get a value but didn't")
	}
	if !bytes.Equal(out.Value, value) {
		t.Fatalf("expected %s but got %x", value, out)
	}
	if version != int(out.Version) {
		t.Fatalf("exepected to get %d but go %d", version, out.Version)
	}
}
