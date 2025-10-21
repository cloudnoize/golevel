package datastructures

import (
	"bytes"
	"crypto/rand"
	"math/big"
	"sort"
	"testing"

	"github.com/cloudnoize/el_gokv/src/plasma/types"
)

// assertPanics runs f and fails the test if it does NOT panic.
func assertPanicsDup(t *testing.T, name string, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("%s: expected panic, but function did not panic", name)
		}
	}()
	f()
}

// --- tests ---

func TestSkipList_capacityPowerOf2(t *testing.T) {
	assertPanicsDup(t, "Power of 2", func() { NewSkipList(1000, 0.5) })
	ns := NewSkipList(1024, 0.5)

	if ns.Size() != 0 {
		t.Fatalf("Size() = %d, want 0", ns.Size())
	}
}

func TestSkipList_PutKVAndGet(t *testing.T) {
	sl := NewSkipList(1024, 0.5)
	key := []byte("key")
	version := 0
	value := []byte("Val")
	sl.PutKV(&types.KV{Key: key, Value: value, Version: uint64(version)})
	if sl.Size() != 1 {
		t.Fatalf("Expected size 1 , actual %d", sl.Size())
	}
	if v, ok, _ := sl.Get([]byte("doesn't exist")); ok {
		t.Fatalf("Expected not to get a value but received %x", v)
	}
	var out types.VersionedValue
	var ok bool
	if out, ok, _ = sl.Get(key); !ok {
		t.Fatalf("Expected to get a value but didn't")
	}
	if !bytes.Equal(out.Value, value) {
		t.Fatalf("expected %s but got %x", value, out)
	}

}

func TestSkipList_severalPutKVsAndGets(t *testing.T) {
	sl := NewSkipList(1024, 0.5)

	elems := [][]byte{
		[]byte("eee"),
		[]byte("bbb"),
		[]byte("ccc"),
	}

	sortedElems := make([][]byte, len(elems))
	copy(sortedElems, elems)
	sort.Slice(sortedElems, func(i, j int) bool {
		return bytes.Compare(sortedElems[i], sortedElems[j]) < 0
	})

	for _, e := range elems {
		sl.PutKV(&types.KV{Key: e, Value: e, Version: 0})
	}

	if sl.Size() != 3 {
		t.Fatalf("Expected 3 got %d", sl.Size())
	}

	itr := sl.Iterator()
	idx := 0
	for itr.Dref() != nil {
		if !bytes.Equal(itr.Dref().Value, sortedElems[idx]) {
			t.Fatalf("Expected %x got %x", sortedElems[idx], itr.Dref().Value)
		}
		idx++
		itr.Next()
	}

	for _, key := range elems {
		if val, ok, _ := sl.Get(key); !ok || !bytes.Equal(key, val.Value) {
			t.Fatalf("Expected %d got %d", key, val)
		}
	}
}

func TestSkipList_randomPutKV(t *testing.T) {
	size := uint64(1024)
	sl := NewSkipList(size, 0.5)

	elems := make([][]byte, size)

	// Lambda function
	gen := func() []byte {
		// Pick a random length between 1 and 20
		nBig, _ := rand.Int(rand.Reader, big.NewInt(20))
		n := int(nBig.Int64()) + 1

		b := make([]byte, n)
		_, _ = rand.Read(b) // fill slice with random bytes
		return b
	}

	for i := range size {
		b := gen()
		if i == 0 || !bytes.Equal(b, elems[i-1]) {
			elems[i] = b
		}
		sl.PutKV(&types.KV{Key: b, Value: b, Version: i})
	}

	sortedElems := make([][]byte, len(elems))
	copy(sortedElems, elems)
	sort.Slice(sortedElems, func(i, j int) bool {
		return bytes.Compare(sortedElems[i], sortedElems[j]) < 0
	})

	if sl.Size() != size {
		t.Fatalf("Expected %d got %d", size, sl.Size())
	}

	itr := sl.Iterator()
	idx := 0
	for itr.Dref() != nil {
		if idx > 0 && bytes.Equal(sortedElems[idx-1], sortedElems[idx]) {
			idx++
			continue
		}
		if !bytes.Equal(itr.Dref().Value, sortedElems[idx]) {
			t.Fatalf("Expected %x got %x", sortedElems[idx], itr.Dref().Value)
		}
		idx++
		itr.Next()
	}

	shorter := 0
	longer := 0
	for _, key := range elems {

		val, ok, steps := sl.Get(key)
		if !ok || !bytes.Equal(key, val.Value) {
			t.Fatalf("Expected %d got %d", key, val)
		}
		// The skiplist should make fewer iterations to get to the same idx lineraly
		// therefore the linear element at location steps should be smaller or equal.
		linerElem := sortedElems[steps]
		if bytes.Compare(linerElem, key) > 0 {
			longer++
		} else {
			shorter++
		}
	}
	if longer > shorter {
		t.Fatalf("Expected shorter %d to be larget than longet %d", shorter, longer)
	}

	for i := len(sortedElems) - 1; i > len(sortedElems)-10; i-- {
		key := sortedElems[i]
		_, ok, steps := sl.Get(key)
		if !ok {
			t.Fatalf("expected %d", steps)
		}
	}
}
