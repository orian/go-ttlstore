package ttlstore

import (
	"container/heap"
	"fmt"
	"testing"
	"time"
)

func TestNewDeleter(t *testing.T) {
	ex := []*actionTimestamp{
		{"1", 10},
		{"2", 20},
		{"3", 15},
		{"4", 25},
		{"5", 5},
		{"6", 17},
		{"7", 14},
		{"8", time.Now().UnixNano()},
	}
	deleted := make(map[interface{}]bool)
	cnt := 0
	var dh DeleteHook = func(key interface{}) {
		deleted[key] = true
		cnt++
	}
	d := NewDeleter("", dh, time.Minute, time.Hour, 3, 3)
	go d.Process()
	for _, v := range ex {
		d.c <- v
	}
	close(d.c)
	<-d.finished
	if l := len(d.lastActionTimestamp); l != 8 {
		t.Errorf("want: 8, got: %d", l)
	}
	d.deleteUntilBelowMaxUuid()

	if cnt != 5 {
		t.Errorf("deleted, want: 5, got: %d", cnt)
		t.Errorf("%v", deleted)
	}
	if l := len(d.lastActionTimestamp); l != 3 {
		t.Errorf("want: 3, got: %d", l)
	}

	d.DeleteTooOld()
	if cnt != 7 {
		t.Errorf("deleted, want: 7, got: %d", cnt)
		t.Errorf("%v", deleted)
	}
	if l := len(d.lastActionTimestamp); l != 1 {
		t.Errorf("want: 1, got: %d", l)
	}
}

func TestFullDeleter(t *testing.T) {
	ex := []*actionTimestamp{
		{"1", 10},
		{"2", 20},
		{"3", 15},
		{"4", 25},
		{"5", 5},
		{"6", 17},
		{"7", 14},
		{"8", time.Now().UnixNano()},
	}
	d := NewDeleter("", nil, time.Minute, time.Second, 3, 3)
	go d.Start()
	for _, v := range ex {
		d.c <- v
	}
	<-time.NewTimer(time.Second * 3).C

	defer d.Stop()
	d.m.Lock()
	defer d.m.Unlock()
	if l := len(d.lastActionTimestamp); l != 1 {
		t.Errorf("want: 1, got: %d", l)
	}
}

func benchForUsers(n, a, x, y int, b *testing.B) {
	ids := [][]byte{}
	for i := 0; i < n; i++ {
		ids = append(ids, []byte(fmt.Sprint(i)))
	}

	for i := 0; i < b.N; i++ {
		d := NewDeleter("", nil, time.Minute, 5*time.Second, x, y)
		go d.Start()
		for _, id := range ids {
			for j := 0; j < a; j++ {
				d.c <- &actionTimestamp{id, time.Now().UnixNano()}
			}
		}
		d.Stop()
	}
	b.Logf("added %d", len(ids)*b.N)
}

func TestActionTimestmapHeap(t *testing.T) {
	h := &ActionTimestmapHeap{}

	for _, v := range []*actionTimestamp{
		{"1", 10},
		{"2", 20},
		{"3", 15},
		{"4", 25},
		{"5", 5},
		{"6", 17},
		{"7", 14},
	} {
		heap.Push(h, v)
	}
	a := heap.Pop(h).(*actionTimestamp)
	if uid := a.Key.(string); uid != "4" {
		t.Errorf("want 5, got %s", uid)
	}
}

// TODO think about better performance test
func BenchmarkDel70000(b *testing.B) { benchForUsers(70000, 10, 1000, 50000, b) }

func BenchmarkDel70000x100(b *testing.B) { benchForUsers(70000, 100, 1000, 50000, b) }
