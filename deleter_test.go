package ttlstore

import (
	"container/heap"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var _ SetCallback = (&Deleter{}).Set

func TestNewDeleter(t *testing.T) {
	items := []struct {
		key       interface{}
		ts        int64
		delAsSize bool
		delAsTime bool
	}{
		{"1", 10, true, true},
		{"2", 20, false, true},
		{"3", 15, true, true},
		{"4", 25, false, true},
		{"5", 5, true, true},
		{"6", 17, true, true},
		{"7", 14, true, true},
		{"8", 10000, false, false},
	}

	deleted := make(map[interface{}]bool)
	cnt := 0
	var dh DeleteHook = func(key interface{}) {
		deleted[key] = true
		cnt++
	}
	d := NewDeleter("", dh, 10, 100000, 3, 3, nil)
	d.Now = func() time.Time {
		return time.Unix(0, 10000-1)
	}
	go d.Process()
	for _, v := range items {
		d.c <- &itemTimestamp{
			Key:           v.key,
			TimestmapNsec: v.ts,
		}
	}
	close(d.c)
	<-d.finished
	require.Len(t, d.lastItemTimeNsec, 8, "want: 8, got: %d", len(d.lastItemTimeNsec))
	for _, v := range items {
		require.Equal(t, v.ts, d.lastItemTimeNsec[v.key])
	}
	d.deleteUntilBelowKeepNum()

	if cnt != 5 {
		t.Errorf("deleted, want: 5, got: %d", cnt)
		t.Fatalf("%v", deleted)
	}
	if l := len(d.lastItemTimeNsec); l != 3 {
		t.Fatalf("want: 3, got: %d", l)
	}
	for _, v := range items {
		require.Equal(t, v.delAsSize, deleted[v.key])
	}

	d.deleteTooOld()
	if cnt != 7 {
		t.Errorf("deleted, want: 7, got: %d", cnt)
		t.Fatalf("%v", deleted)
	}
	if l := len(d.lastItemTimeNsec); l != 1 {
		t.Fatalf("want: 1, got: %d", l)
	}
	for _, v := range items {
		require.Equal(t, v.delAsTime, deleted[v.key])
	}
}

func TestFullDeleter(t *testing.T) {
	ex := []*itemTimestamp{
		{"1", 10},
		{"2", 20},
		{"3", 15},
		{"4", 25},
		{"5", 5},
		{"6", 17},
		{"7", 14},
		{"8", time.Now().UnixNano()},
	}
	d := NewDeleter("", nil, time.Minute, time.Second, 3, 3, nil)
	go d.Start()
	for _, v := range ex {
		d.c <- v
	}
	time.Sleep(3 * time.Second)

	defer d.Stop()
	d.m.Lock()
	defer d.m.Unlock()
	if l := len(d.lastItemTimeNsec); l != 1 {
		t.Errorf("want: 1, got: %d", l)
	}
}

func benchForUsers(bufSize, keepNum int, b *testing.B) {
	ids := make([]string, 0, b.N)
	for i := 0; i < b.N; i++ {
		ids = append(ids, fmt.Sprint(i))
	}

	d := NewDeleter("", nil, time.Minute, 5*time.Second, bufSize, keepNum, nil)
	go d.Start()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		d.Set(ids[i], time.Now())
	}
	b.StopTimer()
	d.Stop()
	b.Logf("added %d", len(ids)*b.N)
}

func TestActionTimestmapHeap(t *testing.T) {
	h := &itemTimestmapHeap{}

	for _, v := range []*itemTimestamp{
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
	a := heap.Pop(h).(*itemTimestamp)
	if uid := a.Key.(string); uid != "4" {
		t.Errorf("want 5, got %s", uid)
	}
}

// TODO think about better performance test
func BenchmarkDel(b *testing.B) { benchForUsers(1000, 50000, b) }
