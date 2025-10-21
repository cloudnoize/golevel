package datastructures

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// ---------------------
// Unit Tests
// ---------------------

func TestTSMap_PutAndGet(t *testing.T) {
	m := NewTSMap[[]byte](16)
	key := []byte("hello")
	value := []byte("world")

	m.Put(key, value)
	got, ok := m.Get(key)
	if !ok {
		t.Fatalf("expected key to be found")
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("expected value %s, got %s", value, got)
	}

	if m.Size() != 1 {
		t.Fatalf("expected size 1, got %d", m.Size())
	}
}

func TestTSMap_MultipleKeys(t *testing.T) {
	m := NewTSMap[[]byte](16)
	data := map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
	}

	for k, v := range data {
		val := []byte(v)
		m.Put([]byte(k), val)
	}

	for k, v := range data {
		got, ok := m.Get([]byte(k))
		if !ok {
			t.Fatalf("key %s not found", k)
		}
		if !bytes.Equal(got, []byte(v)) {
			t.Fatalf("for key %s expected %s got %s", k, v, got)
		}
	}
}

func TestTSMap_ConcurrentAccess(t *testing.T) {
	m := NewTSMap[[]byte](16)
	var wg sync.WaitGroup
	numOps := 20000

	for i := 0; i < numOps; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key-%d", i))
			val := []byte(fmt.Sprintf("val-%d", i))
			m.Put(key, val)
			got, ok := m.Get(key)
			if !ok || !bytes.Equal(got, val) {
				t.Errorf("expected %s got %s", val, got)
			}
		}(i)
	}
	wg.Wait()

	if m.Size() == 0 {
		t.Fatalf("expected map to have elements, got %d", m.Size())
	}
}

// ---------------------
// Benchmarks
// ---------------------

func BenchmarkTSMap_Put(b *testing.B) {
	m := NewTSMap[[]byte](16)
	key := []byte("key")
	val := []byte("value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Put(key, val)
	}
}

func BenchmarkSyncMap_Put(b *testing.B) {
	var m sync.Map
	key := "key"
	val := "value"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Store(key, val)
	}
}

func BenchmarkTSMap_Get(b *testing.B) {
	m := NewTSMap[[]byte](16)
	key := []byte("key")
	val := []byte("value")
	m.Put(key, val)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Get(key)
	}
}

func BenchmarkSyncMap_Get(b *testing.B) {
	var m sync.Map
	key := "key"
	val := "value"
	m.Store(key, val)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Load(key)
	}
}

// ---------------------
// Parallel Benchmarks
// ---------------------

func BenchmarkTSMap_Parallel(b *testing.B) {
	m := NewTSMap[[]byte](16)
	b.RunParallel(func(pb *testing.PB) {
		i := int64(0)
		for pb.Next() {
			n := atomic.AddInt64(&i, 1)
			key := []byte(fmt.Sprintf("key-%d", n))
			val := []byte("val")
			m.Put(key, val)
			m.Get(key)
		}
	})
}

func BenchmarkSyncMap_Parallel(b *testing.B) {
	var m sync.Map
	b.RunParallel(func(pb *testing.PB) {
		i := int64(0)
		for pb.Next() {
			n := atomic.AddInt64(&i, 1)
			key := fmt.Sprintf("key-%d", n)
			m.Store(key, "val")
			m.Load(key)
		}
	})
}

// ---------------------
// Contention Benchmarks
// ---------------------

func BenchmarkTSMap_HighContention(b *testing.B) {
	m := NewTSMap[[]byte](16)
	key := []byte("hotkey")
	val := []byte("val")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Put(key, val)
			m.Get(key)
		}
	})
}

func BenchmarkSyncMap_HighContention(b *testing.B) {
	var m sync.Map
	key := "hotkey"

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Store(key, "val")
			m.Load(key)
		}
	})
}

func BenchmarkTSMap_LowContention(b *testing.B) {
	m := NewTSMap[[]byte](16)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int64(0)
		for pb.Next() {
			n := atomic.AddInt64(&i, 1)
			key := []byte(fmt.Sprintf("key-%d", n))
			val := []byte("val")
			m.Put(key, val)
			m.Get(key)
		}
	})
}

func BenchmarkSyncMap_LowContention(b *testing.B) {
	var m sync.Map
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int64(0)
		for pb.Next() {
			n := atomic.AddInt64(&i, 1)
			key := fmt.Sprintf("key-%d", n)
			m.Store(key, "val")
			m.Load(key)
		}
	})
}
