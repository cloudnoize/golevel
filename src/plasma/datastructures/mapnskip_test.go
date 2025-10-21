package datastructures

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/cloudnoize/el_gokv/src/plasma/types"
)

// ---------------------
// Unit Tests
// ---------------------

func TestMApNSkipPutAndGet(t *testing.T) {
	m := NewMapNSkip(1024)
	key := []byte("hello")
	value := []byte("world")
	version := 1

	m.Put(&types.KV{key, value, uint64(version)})
	got, ok := m.Get(key)
	if !ok {
		t.Fatalf("expected key to be found")
	}
	if !bytes.Equal(got.Value, value) {
		t.Fatalf("expected value %s, got %s", value, got.Value)
	}
	if version != int(got.Version) {
		t.Fatalf("expected verison")
	}

	if m.Size() != 1 {
		t.Fatalf("expected size 1, got %d", m.Size())
	}
}

func TestMapNKip_ConcurrentAccess(t *testing.T) {
	m := NewMapNSkip(16)
	var wg sync.WaitGroup
	numOps := 19990

	for i := 0; i < numOps; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key-%d", i))
			val := []byte(fmt.Sprintf("val-%d", i))
			m.Put(&types.KV{key, val, uint64(i)})
			got, ok := m.Get(key)
			if !ok {
				t.Errorf("Not ok")
			}
			if !bytes.Equal(got.Value, val) {
				t.Errorf("expected %s got %s", val, got.Value)
			}
		}(i)
	}
	wg.Wait()

	if m.Size() == 0 {
		t.Fatalf("expected map to have elements, got %d", m.Size())
	}
}
